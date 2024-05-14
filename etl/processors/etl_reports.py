"""Classes for report options"""
import logging
from common.s3 import s3BucketConnector
from common.constants import S3BucketConfigs
from common.database import DatabaseConnection
import time
import os
from boxoffice_api import BoxOffice
import pandas as pd
from pandas import DataFrame

#Create logging object
logger = logging.getLogger(__name__)


class BoxOfficeETL():
    """Main class for running necessary ETL processes for Box Office reporting"""
    def __init__(self):
        """
        Class constructor
        
        :param year: year to be extracted
        """
        #get s3 bucket
        self.bucket_conn = s3BucketConnector(S3BucketConfigs.ETL_BUCKET.value)  


    def run_extract(self, year: int) -> None:
        """
        Leverages BoxOffice-API to scrape box office data by week (all 52) for the
        year provided, and load's as csv files to s3.

        :param year: box office year to be retrieved
        """
        #create box office instance
        box_office = BoxOffice(api_key=os.environ[S3BucketConfigs.ETL_OMDB_KEY_NAME.value], outputformat="DF");
        logger.info(f"Processing year: {year}")

        #loop thru years and call for each week
        for i in range(1, 53):
            key = "-".join((f'data_files/box-office', str(year), str(i))) + ".csv"
            try:
                weekly_data_df = box_office.get_weekly(year=int(year), week=i)
                weekly_data_df['Reference'] = f"{year}-{i}"
                self.bucket_conn.write_df_as_csv_to_s3(weekly_data_df, key)
            except AttributeError:
                logger.error(f"Attr Error (No Results) for {year}-{i} at " + time.ctime())
            except:
                logger.error(f"Unknown Error for {year}-{i} " + time.ctime())
        #Return true for confirmation
        return True

    def run_transform(self) -> DataFrame:
        """
        Pull unprocessed data files and write to database
        """
        #get files currently in data_files bucket and process
        files_to_transform = self.bucket_conn.list_files_in_prefix('data_files/')
        full_df = pd.DataFrame()
        for csv_file in files_to_transform[1:]:
            logger.info(f"Processing file {csv_file} for transform: " + time.ctime())
            try:
                movie_df = self.bucket_conn.read_csv_to_df(csv_file)
                movie_df = movie_df[movie_df.index < 16]
                movie_df = movie_df[movie_df['Response'] == True]
                movie_df = movie_df[['Rank', 'Release', 'Gross', 'Theaters',
                    'Average', 'Total Gross', 'Weeks', 'Distributor', 'Year', 'Runtime', 
                    'Genre', 'Poster', 'Ratings', 'imdbID', 'Reference']]
                movie_df['Year'] = movie_df['Year'].astype('float').astype('Int64')
                movie_df['Ratings'] = movie_df['Ratings'].str.strip('[]')
                #Convert np int64 to python object
                movie_df['Rank'] = movie_df['Rank'].astype('object')
                movie_df['Weeks'] = movie_df['Weeks'].astype('object')
                movie_df['Year'] = movie_df['Year'].astype('object')
                #Convert currency to string
                movie_df['Gross'] = movie_df['Gross'].str.replace('$', '').str.replace(',', '')
                movie_df['Average'] = movie_df['Average'].str.replace('$', '').str.replace(',', '')
                movie_df['Total Gross'] = movie_df['Total Gross'].str.replace('$', '').str.replace(',', '')
                movie_df['Theaters'] = movie_df['Theaters'].str.replace(',', '')
                #Extract ratings into individual columns
                for index, row in movie_df.iterrows():
                    ratings = str(row['Ratings']).replace('},', '}*').split('*')
                    if len(ratings) == 3:
                        movie_df.loc[index, 'imdb_score'] = float(eval(ratings[0])['Value'].split('/')[0])
                        movie_df.loc[index, 'rt_score'] = float(eval(ratings[1])['Value'].split('/')[0].strip("%"))
                        movie_df.loc[index, 'meta_score'] = float(eval(ratings[2])['Value'].split('/')[0])
                movie_df['imdb_score'] = movie_df['imdb_score'].fillna(value=0)
                movie_df['rt_score'] = movie_df['rt_score'].fillna(value=0)
                movie_df['meta_score'] = movie_df['meta_score'].fillna(value=0)
                movie_df['Runtime'] = movie_df['Runtime'].fillna(value='0 min')
                movie_df.drop(['Ratings'], axis=1, inplace=True)
                movie_df.head()
                full_df = pd.concat([full_df, movie_df], ignore_index=True)
            except:
                logger.error(f"Unknown Error processing {csv_file} for load. " + time.ctime())
        #write files to archive folder
        self.bucket_conn.move_files_to_archive(files_to_transform)
        #return full_df for use in load method
        return full_df


    def run_load(self, df) -> None:
        """
        Gets stored movie data and adds the current year's data, then 
        writes the new set back to s3 as .parquet file.

        :params df: dataframe of movie data to be added to storage
        """
        if df.shape[0] == 0:
            logger.info(f"No records to load.")
        else:
            logger.info(f"Number of records loading: {df.shape[0]}")
            try:
                db_conn = DatabaseConnection()
                db_conn._session.rollback()
                for index, row in df.iterrows():
                    #Check for distributor and add if new
                    dist_id = db_conn.get_distributor_id_by_name(row['Distributor'])
                    if dist_id == 0:
                        dist_id = db_conn.add_distributor(row['Distributor'])
                    #Check for movie and add if new
                    movie_id = db_conn.get_movie_id_by_name(row['Release'])
                    if movie_id == 0:
                        movie_id = db_conn.add_movie(row, dist_id)
                    #Add box office info for the week and movie
                    db_conn.add_box_office_entry(row, movie_id)
                db_conn.close()
                logger.info(f"Completed write to database")
            except:
                logger.error(f"Unknown error during database load process.")


    def run(self, year: int) -> None:
        """
        Main ETL process that manages each subtask.

        :params year: year to be scraped (from commandline args)
        """
        BoxOfficeETL.run_extract(self, year)
        full_df = BoxOfficeETL.run_transform(self)
        BoxOfficeETL.run_load(self, full_df)

