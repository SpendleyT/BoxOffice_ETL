"""Classes for report options"""
import logging
from etl.common.s3 import s3BucketConnector
from etl.common.constants import S3BucketConfigs
import time
import os
from boxoffice_api import BoxOffice
import pandas as pd
from pandas import DataFrame

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
        #create box office instance
        box_office = BoxOffice(api_key=os.environ[S3BucketConfigs.ETL_OMDB_KEY_NAME.value], outputformat="DF");
        logger.info(f"Processing year: {year}")

        #loop thru years and call for each week
        for i in range(1, 53):
            key = "-".join((f'{year}/box-office', str(year), str(i))) + ".csv"
            try:
                weekly_data_df = box_office.get_weekly(year=int(year), week=i)
                weekly_data_df['Reference'] = f"{year}-{i}"
                self.bucket_conn.write_df_to_s3(weekly_data_df, key)
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
        files_to_transform = self.bucket_conn.list_files_in_prefix('data_files')
        full_df = pd.DataFrame()
        for csv_file in files_to_transform:
            movie_df = self.bucket_conn.read_csv_to_df(csv_file)
            movie_df = movie_df.dropna()
            movie_df = movie_df[movie_df.index < 16]
            movies_clean_df = movie_df.drop(['LW', '%± LW', 
                'Change', 'Title', 'Rated', 'Released', 'Director', 'Writer',
                'Actors', 'Plot', 'Language', 'Country', 'Awards', 
                'Metascore', 'imdbRating', 'imdbVotes', 'Type', 'DVD',
                'BoxOffice', 'Production', 'Website', 'Response', 'Error'], axis=1)
            movies_clean_df['Year'] = movies_clean_df['Year'].astype('float').astype('Int64')
            #movies_clean_df[['Genre1', 'Genre2', 'Genre3']] = movies_clean_df['Genre'].str.split(',', expand=True)
            #movies_clean_df = movies_clean_df.drop(['Genre'], axis=1)
            movies_clean_df['Ratings'] = movies_clean_df['Ratings'].str.strip('[]')
            for index, row in movies_clean_df.iterrows():
                ratings = row['Ratings'].str.replace('},', '}*').split('*')
                movies_clean_df.loc[index, 'imdb_score'] = eval(ratings[0])['Value'].split('/')[0]
                movies_clean_df.loc[index, 'rt_score'] = eval(ratings[1])['Value'].split('/')[0].strip("%")
                movies_clean_df.loc[index, 'meta_score'] = eval(ratings[2])['Value'].split('/')[0]
            movies_clean_df.drop(['Ratings'], axis=1, inplace=True)
            full_df = pd.concat([full_df, movies_clean_df], ignore_index=True)
        #write files to archive folder
        self.bucket_conn.move_files_to_archive(files_to_transform)
        #return full_df for use in load method
        return full_df


    def run_load(self, df) -> None:
        stored_df = self.bucket_conn.read_prq_to_df("database/movies.parquet")
        stored_df = pd.concat([stored_df, df], ignore_index=True)
        stored_df = stored_df.sort_values(by=['Release', 'Weeks'], ascending=False, ignore_index=True)
        self.bucket_conn.write_df_as_prq_to_s3(df, "database/movies.parquet")
        

    def run(self, year: int) -> None:
        BoxOfficeETL.run_extract(self, year)
        full_df = BoxOfficeETL.run_transform(self)
        BoxOfficeETL.run_load(self, full_df)

