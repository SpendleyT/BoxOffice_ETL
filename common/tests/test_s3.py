""" Test s3 bucket connector methods"""
import os
import unittest
import pandas as pd
import boto3
from moto import mock_s3
from common.s3 import s3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3 bucket connector class
    """
    def setUp(self) -> None:
        #Mock s3  connection 
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        #Defining class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY'
        self.s3_secret_key = 'AWS_SECRET_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        #Defining test content variables
        self.prefix_exp = 'prefix/'
        self.key1_exp = f'{self.prefix_exp}test1.csv'
        self.key2_exp = f'{self.prefix_exp}test2.csv'
        self.prefix_arc = 'archive/'
        self.key1_arc = f'{self.prefix_arc}test1.csv'
        self.key2_arc = f'{self.prefix_arc}test2.csv'
        #Creating the s3 access keys
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        #Creating a bucket on mocked s3
        self.s3 = boto3.resource(service_name="s3", endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-central-1'
                              })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        #Creating a testing instance
        self.s3_bucket_conn = s3BucketConnector(self.s3_bucket_name, self.s3_access_key, self.s3_secret_key,
                                                 self.s3_endpoint_url)

    def tearDown(self) -> None:
        self.mock_s3.stop()
    
    def populate_bucket(self):
        """Set up test files in bucket"""
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=self.key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=self.key2_exp)

    
    def cleanup_bucket(self):
        """Remove test files from bucket"""
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': self.key1_exp
                    },
                    {
                        'Key': self.key2_exp
                    }
                ]
            }
        )


    def test_list_files_in_prefix_ok(self):
        """ Test list_files_in_prefix method"""
        #populate_bucket
        self.populate_bucket()
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(self.prefix_exp)
        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(self.key1_exp, list_result)
        self.assertIn(self.key2_exp, list_result)
        # Cleanup after test
        self.cleanup_bucket()


    def test_list_files_in_prefix_wrong_prefix(self):
        """Negative test for list_files_in_prefix"""
        # Expected results
        prefix_exp = 'no-prefix/'
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertTrue(not list_result)


    def test_read_csv_to_df(self):
        """Confirm csv file read successfully"""
        self.populate_bucket()
        test_df = self.s3_bucket_conn.read_csv_to_df(self.key1_exp)
        self.assertGreaterEqual(test_df.shape[0], 1)
        self.cleanup_bucket()


    def test_write_df_as_csv_to_s3(self):
        #Populate test data
        df = pd.DataFrame({1: ["A", "C"], 2: ["B", "D"]})
        self.s3_bucket_conn.write_df_as_csv_to_s3(df, self.key1_exp)
        list_result = self.s3_bucket_conn.list_files_in_prefix(self.prefix_exp)
        self.assertIn(self.key1_exp, list_result)
        self.cleanup_bucket()


    def test_move_files_to_archive(self):
        self.populate_bucket()
        list_result_exp = self.s3_bucket_conn.list_files_in_prefix(self.prefix_exp)
        self.s3_bucket_conn.move_files_to_archive(list_result_exp)
        list_result_arc = self.s3_bucket_conn.list_files_in_prefix(self.prefix_arc)
        self.assertIn(self.key1_arc, list_result_arc)
        self.assertIn(self.key2_arc, list_result_arc)
        self.assertIn(self.key1_exp, list_result_exp)
        self.assertIn(self.key2_exp, list_result_exp)
        self.cleanup_bucket()


if __name__ == "__main__":
    unittest.main()
