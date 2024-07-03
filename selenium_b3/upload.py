import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from datetime import datetime
import os

# Retrieve a date from file (%d-%m-%y), trading_date(formated to %Y-%m-%d) and set important arguments for convertion to parquet and upload to s3
def extract_date_file(local_path):
    local_csv_files_list = [f for f in os.listdir(local_path) if f.endswith('.csv')]
    for csv_file in local_csv_files_list:
        csv_name = csv_file.split('/')[-1]
        date_file = csv_name.split('_')[1].split('.')[0] 
    return date_file

date_file = extract_date_file('./data/')

def transform_trading_date(date_file):
    date_datetime = datetime.strptime(date_file, '%d-%m-%y')
    trading_date = date_datetime.strftime('%Y-%m-%d')  
    return trading_date

trading_date = transform_trading_date(date_file)

csv_file_name = f'IBOVDia_{date_file}.csv'

bucket_name = "fiap-julio-general"
bucket_layer = f"raw/ibovespa/{trading_date}"

input_path_csv = f'data/{csv_file_name}'
output_path_parquet = f'data/IBOVDia_{trading_date}.parquet'

#Convert local csv file to parquet
def csv_to_parquet(input_path_csv, output_path_parquet):
    df = pd.read_csv(input_path_csv, encoding='ansi', sep = ';', skiprows=1, skipfooter=2, index_col=False, engine='python', dtype=str)
    df['Qtde. Teórica'] = df['Qtde. Teórica'].str.replace(".","")
    df['Part. (%)'] = df['Part. (%)'].str.replace(',','.')
    df['Part. (%)Acum.'] = df['Part. (%)Acum.'].str.replace(',','.')
    df['trading_date'] = trading_date
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path_parquet)

csv_to_parquet(input_path_csv, output_path_parquet)

parquet_file_name = f'IBOVDia_{trading_date}.parquet'

#Upload the parquet file on S3 bucket
def upload_file(parquet_file_name, bucket, object_name):

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(parquet_file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

upload_file(f'./data/{parquet_file_name}', bucket_name, f'{bucket_layer}/{parquet_file_name}')
