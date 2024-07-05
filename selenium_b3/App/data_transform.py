import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os

class DataTransform:
    
    def __init__(self) -> None:
        pass

    def extract_date_file(self, local_path) -> str:
        local_csv_files_list = [f for f in os.listdir(local_path) if f.endswith('.csv')]
        for csv_file in local_csv_files_list:
            csv_name = csv_file.split('/')[-1]
            date_file = csv_name.split('_')[1].split('.')[0] 
        return date_file
    
    def transform_trading_date(self, local_path) -> str:
        """local_path: './data/'"""
        date_file = self.extract_date_file(local_path)
        date_datetime = datetime.strptime(date_file, '%d-%m-%y')
        self.trading_date = date_datetime.strftime('%Y-%m-%d')  
        return self.trading_date
    
    
    def save_parquet(self, input_path_csv, output_path_parquet) -> None:
        #Convert local csv file to parquet
        df = pd.read_csv(input_path_csv, encoding='ansi', sep = ';', skiprows=1, skipfooter=2, index_col=False, engine='python', dtype=str)
        df['Qtde. Teórica'] = df['Qtde. Teórica'].str.replace(".","")
        df['Part. (%)'] = df['Part. (%)'].str.replace(',','.')
        df['Part. (%)Acum.'] = df['Part. (%)Acum.'].str.replace(',','.')
        df['trading_date'] = self.trading_date
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path_parquet)