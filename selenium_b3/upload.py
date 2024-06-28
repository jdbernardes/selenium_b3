#%%
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from datetime import datetime

#%%
date = datetime.now().strftime('%d-%m-%y')
csv_file_name = f'IBOVDia_{date}.csv'

bucket_name = "fiap-brisa-general"
bucket_layer = f"raw/ibovespa/{date}"

input_path_csv = f'data/{csv_file_name}'
output_path_parquet = f'data/IBOVDia_{date}.parquet'

#%%
#Convert local csv file to parquet
def csv_to_parquet(input_path_csv, output_path_parquet):
    df = pd.read_csv(input_path_csv, encoding='latin-1', sep = ';', skiprows=1, skipfooter=2, index_col=False, engine='python', dtype=str)
    df['Qtde. Teórica'] = df['Qtde. Teórica'].str.replace(".","")
    df['Part. (%)'] = df['Part. (%)'].str.replace(',','.')
    df['Part. (%)Acum.'] = df['Part. (%)Acum.'].str.replace(',','.')
    df['Qtde. Teórica'] = df['Qtde. Teórica'].astype(int)
    df['Part. (%)'] = df['Part. (%)'].astype(float)
    df['Part. (%)Acum.'] = df['Part. (%)Acum.'].astype(float)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path_parquet)

csv_to_parquet(input_path_csv, output_path_parquet)

parquet_file_name = f'IBOVDia_{date}.parquet'

#%%
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
