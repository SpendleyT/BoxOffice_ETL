"""Entry point for ETL project"""
from common import log
import time

from pathlib import Path
from argparse import ArgumentParser
from etl.processors.etl_reports import BoxOfficeETL


def main():
    """
    Entry point to run Xetra ETL
    """
    #Get logger
    logger = log.setup_custom_logger('root')
    
    #parse runtime arguments
    parser = ArgumentParser()
    parser.add_argument('-y', dest='year')
    args = parser.parse_args()
    year = args.year

    #call etl task #1 (add DAG later?)
    logger.info("Start ETL run: " + time.ctime())
    box_office = BoxOfficeETL()
    box_office.run(str(year))
    logger.info("End ETL run: " + time.ctime())


if __name__ == "__main__":
    main()