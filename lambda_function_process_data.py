import json
import urllib.parse
import boto3
import io
import csv
import gzip
import os
from datetime import datetime, timezone, timedelta

print('Loading function')

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    total_infected_persons = 0
    total_death = 0
    total_infected_not_reconigzed = 0
    total_death_not_reconized = 0
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        obj = s3.Object(bucket, key)
        print("CONTENT TYPE: " + obj.content_type)
        print("CONTENT Length: " + str(obj.content_length))
        
        content = obj.get()['Body'].read()
        csv_buffer = io.StringIO()
        fieldnames = ['Municipio', 'confirmados', 'obitos']
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        
        #data_e_hora_atuais = datetime.strptime('2020-12-09', '%Y-%m-%d')
        data_e_hora_atuais = datetime.now()
        diferenca = timedelta(hours=-3)
        fuso_horario = timezone(diferenca)
        data_e_hora_sao_paulo = data_e_hora_atuais.astimezone(fuso_horario)
        
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as fh:
            with io.TextIOWrapper(fh, encoding="utf-8") as dec:
                reader = csv.DictReader(dec,delimiter=";", quoting=csv.QUOTE_NONE)
                for row in reader:
                    if row['datahora'] == data_e_hora_sao_paulo.strftime("%Y-%m-%d"):
                        print(row)
                        total_infected_persons_by_city = str(row['casos'])
                        total_death_by_city = str(row['obitos'])
                        total_infected_persons = total_infected_persons + int(total_infected_persons_by_city)
                        total_death = total_death + int(total_death_by_city)
                        city = str(row['nome_munic'])
                        if(city != 'Ignorado'):
                            writer.writerow({'Municipio': city, 'confirmados': total_infected_persons_by_city, 'obitos': total_death_by_city})
                        else:
                            total_infected_not_reconigzed = int(total_infected_persons_by_city)
                            total_death_not_reconized = int(total_death_by_city)

        print("total_infected_persons: " + str(total_infected_persons) + "\n")
        print("total_death: " + str(total_death) + "\n")
        print("total_infected_not_reconigzed:" + str(total_infected_not_reconigzed))
        print("total_death_not_reconized:" + str(total_death_not_reconized))
        
        writer.writerow({'Municipio': 'TOTAL NO ESTADO', 'confirmados': str(total_infected_persons), 'obitos': str(total_death)})
        writer.writerow({'Municipio': 'Importados/Indefinidos', 'confirmados': str(total_infected_not_reconigzed), 'obitos': str(total_death_not_reconized)})
        
        dst_key = "processados/dados-sarcov-sp-" + data_e_hora_sao_paulo.strftime('%Y-%m-%d') + ".csv"
        print(dst_key)
        
        dst_obj = s3.Object(bucket, dst_key)
        dst_obj.put(Body=csv_buffer.getvalue())
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
