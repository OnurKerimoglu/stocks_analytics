import logging
import os

import pyarrow.csv as pv
from pyarrow import json
import pyarrow.parquet as pq

def config_logger(log_level):
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    if log_level == 'debug':
        logging.basicConfig(
            level=logging.DEBUG,
            format=log_format)
    elif log_level == 'info':
        logging.basicConfig(
            level=logging.INFO,
            format=log_format)
    else:
        raise Exception ('log_level must be "debug" or "info"')
    
def reformat_csv_to_parquet(src_file):
    if not (src_file.endswith('.csv') or src_file.endswith('.csv.gz')):
        logging.error("Can only accept source files in CSV or CSV.GZ format")
        return
    elif src_file.endswith('.csv'):
        parquet_file = src_file.replace('.csv', '.parquet')
    elif src_file.endswith('.csv.gz'):
        parquet_file = src_file.replace('.csv.gz', '.parquet')
    table = pv.read_csv(src_file)
    pq.write_table(table, parquet_file)
    os.remove(src_file)
    logging.info(f"Reformatted {src_file} as {parquet_file}")

def reformat_json_to_parquet(src_file):
    if not (src_file.endswith('.json')):
        logging.error("Can only accept source files in json format")
        return
    elif src_file.endswith('.json'):
        parquet_file = src_file.replace('.json', '.parquet')
    table = json.read_json(src_file)
    pq.write_table(table, parquet_file)
    os.remove(src_file)
    logging.info(f"Reformatted {src_file} as {parquet_file}")
