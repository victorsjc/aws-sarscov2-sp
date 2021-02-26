import json
from datetime import datetime, timezone, timedelta
import urllib3
import urllib.request
import tempfile
import boto3
import shutil
import gzip

s3 = boto3.client('s3')

#https://docs.aws.amazon.com/eventbridge/latest/userguide/run-lambda-schedule.html

def lambda_handler(event, context):
    # TODO implement
    data_e_hora_atuais = datetime.now()
    diferenca = timedelta(hours=-3)
    fuso_horario = timezone(diferenca)
    data_e_hora_sao_paulo = data_e_hora_atuais.astimezone(fuso_horario)
    print(data_e_hora_sao_paulo)

    http = urllib3.PoolManager()

    response = http.request("GET", "https://api.github.com/repos/seade-R/dados-covid-sp/commits?path=/data/dados_covid_sp_latin1.csv",
                           headers={'User-Agent': 'request'})
    print(response.status)
    if response.status == 200:
        data = json.loads(response.data)
        print(data[0]['commit'])
        commit_datetime = datetime.strptime(data[0]['commit']['message'], '%Y%m%d')
        #if commit_datetime.date() == data_e_hora_sao_paulo.date():
        sha = data[0]['sha']
        raw_data_url = 'https://github.com/seade-R/dados-covid-sp/raw/' + sha + '/data/dados_covid_sp.csv'
        print(raw_data_url)
        temppath = tempfile.gettempdir()
        with http.request('GET', raw_data_url, preload_content=False, timeout=10.0, headers={'User-Agent': 'request'}) as r, open(temppath+'/dados_covid_sp.csv', 'wb') as out_file:
               shutil.copyfileobj(r, out_file)
        with open(temppath+'/dados_covid_sp.csv', 'r', encoding='utf-8') as f_in:
            with gzip.open(temppath+'/dados_covid_sp.gz', 'wt') as f_out:
                 shutil.copyfileobj(f_in, f_out)
        key_dst = "preprocessados/dados-covid-sp-"+ data_e_hora_sao_paulo.strftime("%Y-%m-%d") + ".gz"
        s3.put_object(Bucket='data-sarcov-2-sp', Key=key_dst,
            StorageClass='REDUCED_REDUNDANCY',
            Body=open(temppath+'/dados_covid_sp.gz', 'rb'))
