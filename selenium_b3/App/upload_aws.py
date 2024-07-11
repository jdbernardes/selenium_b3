import logging
import boto3
from botocore.exceptions import ClientError

class UploadAws:

    def __init__(self) -> None:
        pass

    def upload_file(self, parquet_file_name, bucket, object_name):
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(parquet_file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        else:
            print("Upload concluido com sucesso")
            return True
    