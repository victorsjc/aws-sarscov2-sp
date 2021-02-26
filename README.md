# Propósito do repositório
Disponibilizar os scripts utilizados para processar informações relacionados aos casos positivos e óbitos causados pelo vírus Sars-Cov-2 no estado de SP.

Os scripts que foram criados para obter de forma automatizada os dados do covid-19 do repositório do Github do SEADE (Fundação Sistema Estadual de Análise de Dados).
https://github.com/seade-R/dados-covid-sp/

# Pré-requisitos
Para os scripts foram utilizados a linguagem Python e as seguintes módulos

- json
- datetime
- urllib3
- urllib.request
- tempfile
- boto3
- shutil
- gzip

# Funcionamento
| Script | Capacidades | Resultado | 
|--|--| -- |
| [lambda_function_get_seade_database.py](https://github.com/victorsjc/aws-sarscov2-sp/blob/main/lambda_function_get_seade_database.py "lambda_function_get_seade_database.py") | O script possibilita obter o commit baseado no mesmo dia no repositório do github do SEADE. É necessário considerar alguns intervalos durante o dia, pois percebi que as secretarias municipais de saúde não possuem uma padrão na disponibilização dos dados.|Geração da base de dados em um arquivo compactado no formato binário (tar.gz) no repositório S3|
|[lambda_function_process_data.py](https://github.com/victorsjc/aws-sarscov2-sp/blob/main/lambda_function_process_data.py "lambda_function_process_data.py")|O script é responsável em processar o arquivo binário gerado (tar.gz), descompactando-o e gerando no formato a ser definido|Geração do arquivo CSV no repositório S3|
