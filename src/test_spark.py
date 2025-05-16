import logging
import os

from pyspark.sql import SparkSession
from src.shared import config_logger


config_logger('info')
logger = logging.getLogger(__name__)

def main():

    # set datapath and fetch parquet files
    rootpath = os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)))
    datapath= os.path.join(rootpath, 'data')
    
    logger.info(f'datpath determined: {datapath}')
    # .master("spark://L54Ku2004:7077") \
    spark = SparkSession.builder \
        .appName("test spark example") \
        .getOrCreate()
    
    df = spark.read.csv(
        os.path.join(datapath, 'default_ETFs.csv'), 
        header="true")
    df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()