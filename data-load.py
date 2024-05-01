from pathlib import Path
import pandas as pd
from config import omdb_key
from boxoffice_api import BoxOffice
import time
import logging


#Create logging config 
def initiate_logging(name: str):
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        filename='./logs/' + name + '.log', 
        format='%(levelname)s:%(message)s',
        encoding='utf-8', 
        level=logging.INFO
    )
    return logger


# Use Box Office webscraper + omdbapi to retrieve weekly movie details for 10 years * 52 weeks
# And write each entry to csv
def main():
    main_logger = initiate_logging("main")
    main_logger.info(f"Starting file import run ------------------------ " + time.ctime())

    #create box office instance
    box_office = BoxOffice(api_key=omdb_key, outputformat="DF");
    #define dataset
    years = [2017, 2018, 2019, 2020, 2021, 2022]

    #loop thru years and call for each week
    for year in years:
        for i in range(1, 53):
            file_name = "-".join(("./data/boxOffice", str(year), str(i))) + ".csv"
            main_logger.info(f"Starting file {year}-{i} read ... " + time.ctime())
            try:
                weekly_data = box_office.get_weekly(year=year, week=i)

                main_logger.info(f"Starting file {i} write ... " + time.ctime())
                #write dataframe to csv file
                weekly_data.to_csv(file_name)
            except AttributeError:
                main_logger.error(f"Attr Error (No Results) for {year}-{i} at " + time.ctime())
            except:
                main_logger.error(f"Unknown Error for {year}-{i} at " + time.ctime())
            else:
                main_logger.info(f"End file {i} write ... " + time.ctime())


if __name__ == "__main__":
    main()