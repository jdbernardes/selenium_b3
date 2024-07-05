from selenium_b3.App.selenium_export import Selenium
from selenium_b3.App.data_transform import DataTransform
from selenium_b3.App.upload_aws import UploadAws

path = r"C:\Users\I857413\Desktop\TechSkills\FIAP Aulas\TechChallenge\selenium_b3\selenium_b3\data"
data_path = './data/'
url = 'https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br'
bucket_name = "fiap-julio-general"

extract = Selenium(path, url)
extract.run_selenium()

data_transform = DataTransform()
date_file = data_transform.extract_date_file(data_path)
trading_date = data_transform.transform_trading_date(data_path)
csv_file_name = f'IBOVDia_{date_file}.csv'
input_path_csv = f'data/{csv_file_name}'
output_path_parquet = f'data/IBOVDia_{trading_date}.parquet'
parquet_file_name = f'IBOVDia_{trading_date}.parquet'
parquet_file_name = f'./data/{parquet_file_name}'
bucket_layer = f'raw/ibovespa/{trading_date}'
object_name = f'{bucket_layer}/{parquet_file_name}'

data_transform.save_parquet(input_path_csv, output_path_parquet)

aws = UploadAws()
aws.upload_file(parquet_file_name, bucket_name, object_name)