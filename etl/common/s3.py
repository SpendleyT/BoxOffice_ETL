"""Connector and methods for s3 access"""
import os
import boto3
import logging
import pandas as pd
from io import StringIO, BytesIO
from etl.common.constants import S3BucketConfigs

logger = logging.getLogger(__name__)

class s3BucketConnector():
    """Class for interacting with s3 Buckets"""

    def __init__(self, bucket: str, endpoint_url=''):
        """
        Constructor for s3BucketConnector

        :param endpoint_url: s3 access url
        :bucket target bucket name
        """
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(
            aws_access_key_id=os.environ[S3BucketConfigs.ETL_ACCESS_KEY_NAME.value],
            aws_secret_access_key=os.environ[S3BucketConfigs.ETL_SECRET_KEY_NAME.value]
        )
        if not endpoint_url:
            self._s3 = self.session.resource(service_name='s3')
        else:
            self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)
        self._client = self.session.client('s3')


    def list_files_in_prefix(self, prefix: str):
        """ 
        Listing all files in bucket with matching prefix
        
        :param prefix: prefix on s3 bucket for match

        return: 
            files: list of all files that match the prefix
        """
        files = [
            obj.key for obj 
            in self._bucket.objects.filter(Prefix=prefix) 
        ]
        return files[1:]


    def read_csv_to_df(self, key: str, decoding='utf-8', sep=','):
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df


    def read_prq_to_df(self, key):
        try:
            prq_obj = self._bucket.Object(key=key).get().get('Body').read()
            data = BytesIO(prq_obj)
            df = pd.read_parquet(data)
        except: 
            logger.info("No parquet file exists. Creating new dataframe.")
            df = pd.DataFrame()
        
        return df


    def write_df_as_csv_to_s3(self, df, key):
        out_buffer = StringIO()
        df.to_csv(out_buffer, index=False)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True


    def write_df_as_prq_to_s3(self, df, key):
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True


    def move_files_to_archive(self, file_list):
        for file in file_list:
            copy_source = {'Bucket': S3BucketConfigs.ETL_BUCKET.value, 'Key': file}
            self._client.copy(copy_source, S3BucketConfigs.ETL_BUCKET.value, f'archive/{file}')


