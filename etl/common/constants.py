"""Managing constant values for project"""
from enum import Enum

class S3BucketConfigs(Enum):
    """Config Details for S3 bucket/folders"""
    ETL_BUCKET = 'de-etl-project'
    ETL_PREFIX_EXTRACT = 'data_files'
    ETL_PREFIX_LOAD = 'databases'
    ETL_KEY_EXTRACT = 'BoxOfficeWeekly'
    ETL_KEY_LOAD = ''
    ETL_FORMAT_EXTRACT = ''
    ETL_FORMAT_LOAD = ''
    ETL_ACCESS_KEY_NAME = 'AWS_ACCESS_KEY_ID'
    ETL_SECRET_KEY_NAME = 'AWS_SECRET_ACCESS_KEY'
    ETL_OMDB_KEY_NAME = 'OMDB_KEY'


class S3FileTypes(Enum):
    """Support file types"""
    CSV = 'csv'
    PARQUET = 'parquet'


class MetaProcessFormat(Enum):
    """formation for MetaProcess"""

    META_DATE_FORMAT = ''
    NETA_PROCESS_DATE_FORMAT = ''
    META_SOURCE_DATE_COL = 'source_date'
    META_PROCESS_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'