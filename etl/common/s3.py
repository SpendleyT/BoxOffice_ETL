"""Connector and methods for s3 access"""
import os
import boto3
import logging
from io import StringIO, BytesIO
from etl.common.constants import S3BucketConfigs


class s3BucketConnector():
    """Class for interacting with s3 Buckets"""

    def __init__(self, bucket: str, endpoint_url=''):
        """
        Constructor for s3BucketConnector

        :param endpoint_url: s3 access url
        :bucket target bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        #self.access_key = S3BucketConfigs.ETL_ACCESS_KEY_NAME
        #self.secret_key = S3BucketConfigs.ETL_SECRET_KEY_NAME
        self.session = boto3.Session(
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
        )
        if not endpoint_url:
            self._s3 = self.session.resource(service_name='s3')
        else:
            self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)


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


    def read_csv_to_df(self):
        pass


    def write_df_to_s3(self, df, key):
        out_buffer = StringIO()
        df.to_csv(out_buffer, index=False)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True