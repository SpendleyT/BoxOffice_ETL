"""Connector and methods for s3 access"""
import os
import boto3
import logging
import pandas as pd
from io import StringIO, BytesIO
from common.constants import S3BucketConfigs

logger = logging.getLogger(__name__)

class s3BucketConnector():
    """Class for interacting with s3 Buckets"""

    def __init__(self, bucket: str, access_key='', secret_key='', endpoint_url=''):
        """
        Constructor for s3BucketConnector

        :param endpoint_url: s3 access url
        :bucket target bucket name
        """
        self.endpoint_url = endpoint_url
        self.access_key = S3BucketConfigs.ETL_ACCESS_KEY_NAME.value if access_key == '' else access_key
        self.secret_key = S3BucketConfigs.ETL_SECRET_KEY_NAME.value if secret_key == '' else secret_key
        self.session = boto3.Session(
            aws_access_key_id=os.environ[self.access_key],
            aws_secret_access_key=os.environ[self.secret_key]
        )
        if not endpoint_url:
            self._s3 = self.session.resource(service_name='s3')
        else:
            self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket_name = bucket
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
        return files


    def read_csv_to_df(self, key: str, decoding='utf-8', sep=','):
        """ 
        Retrieve csv file from s3 and return it as a dataframe

        :param key: file key on s3 bucket for match

        return: 
            df: dataframe of csv file data
        """
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df


    def write_df_as_csv_to_s3(self, df, key):
        """ 
        Converting dataframe to a csv file and storing on S3
        
        :param df: dataframe containing records to store
        :param key: prefix and file name for csv file
        """
        out_buffer = StringIO()
        df.to_csv(out_buffer, index=False)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True


    def move_files_to_archive(self, file_list):
        """ 
        Moves files from data_files folder to archive folder.
        
        :param file_list: list of files processed to be moved
        """
        for file in file_list:
            copy_source = {'Bucket': self._bucket_name, 'Key': file}
            filename = file.split("/")[1]
            self._client.copy(copy_source, self._bucket_name, f'archive/{filename}')
        return True


