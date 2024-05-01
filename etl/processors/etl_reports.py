"""Classes for report options"""
import logging
from etl.common.s3 import s3BucketConnector
from etl.common.constants import S3BucketConfigs
import time
import os
from boxoffice_api import BoxOffice
import pandas
    
logger = logging.getLogger(__name__)

class BoxOfficeETL():
    """Main class for running necessary ETL processes for Box Office reporting"""
    def __init__(self):
        """
        Class constructor
        
        :param year: year to be extracted
        """
        #get s3 bucket
        #self.bucket_name = S3BucketConfigs.ETL_BUCKET
        self.bucket = s3BucketConnector('de-etl-project')  


    def run_extract(self, year: int) -> None:
        #create box office instance
        #apikey = S3BucketConfigs.ETL_OMDB_KEY_NAME
        box_office = BoxOffice(api_key=os.environ['OMDB_KEY'], outputformat="DF");
        logger.info(f"Year: {year} - {type(year)}")

        #loop thru years and call for each week
        for i in range(1, 53):
            key = "-".join(('data_files/box-office', str(year), str(i))) + ".csv"
            try:
                weekly_data = box_office.get_weekly(year=int(year), week=i)
                self.bucket.write_df_to_s3(weekly_data, key)
            except AttributeError:
                logger.error(f"Attr Error (No Results) for {year}-{i} at " + time.ctime())
            except:
                logger.error(f"Unknown Error for {year}-{i} " + time.ctime())


    def run_transform(self) -> None:
        pass


    def run_load(self) -> None:
        pass


    def run(self, year: int) -> None:
        BoxOfficeETL.run_extract(self, year)

