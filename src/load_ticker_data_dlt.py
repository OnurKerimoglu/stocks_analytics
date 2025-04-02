import logging
import os

import dlt
import json
import pandas as pd

from shared import config_logger

class LoadTickerData():
    def __init__(
            self,
            full_load=False,
            dest='duckdb',
            dev_mode=True,
            log_level='info'):
        # configure and start logger
        config_logger(log_level)
        self.logger = logging.getLogger(__name__)
        mode = 'dev' if dev_mode==True else 'prod'
        loadtype = 'full' if full_load else 'incremental'
        self.logger.info(
            f'Initialized LoadTicker in {mode} mode for {loadtype} load to {dest}')
        
        # input arguments
        self.full_load = full_load
        self.dest = dest
        self.dev_mode = dev_mode
            
        # set datapath and fetch parquet files
        self.rootpath = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))
        self.datapath_price = os.path.join(self.rootpath, 'data', 'price')
        self.datapath_fund = os.path.join(self.rootpath, 'data', 'info')
        self.price_paths = self.fetch_data_paths(self.datapath_price, 'parquet')
        self.fund_paths = self.fetch_data_paths(self.datapath_fund, 'json')

        # Define dlt pipelines
        # Price
        self.price_pipeline_duckdb = dlt.pipeline(
            pipeline_name='load_stock_prices_raw_duckb',
            destination='duckdb',
            dataset_name='stocks_raw',
            dev_mode=self.dev_mode
        )
        self.price_pipeline_bq = dlt.pipeline(
            pipeline_name="load_stock_prices_raw_bq",
            destination="bigquery",
            dataset_name="stocks_raw",
            dev_mode=self.dev_mode
        )
        # Fundamentals
        self.fundamentals_pipeline_duckdb = dlt.pipeline(
            pipeline_name='load_stock_fundamentals_raw_duckb',
            destination='duckdb',
            dataset_name='stocks_raw',
            dev_mode=self.dev_mode
        )
        self.fundamentals_pipeline_bq = dlt.pipeline(
            pipeline_name="load_stock_fundamentals_raw_bq",
            destination="bigquery",
            dataset_name="stocks_raw",
            dev_mode=self.dev_mode
        )
    
    def fetch_data_paths(self, datapath, ext):
        # construct a list of absolute paths based on the 
        # (non-test) parquet files in the self.datapth folder
        fpaths = []
        for f in os.listdir(datapath):
            if f.endswith(f'.{ext}') and not f.endswith(f'_test.{ext}'):
                fpaths.append(os.path.join(datapath, f))
        if len(fpaths) == 0:
            self.logger.warning(f'No {ext} files found at {datapath}')
        else:
            self.logger.info(f'Found {len(fpaths)} {ext} files at {datapath}')
        return fpaths

    @dlt.resource(name="prices")
    @staticmethod
    def stock_prices_raw(
        parquet_paths,
        load_type,
        cursor_date=dlt.sources.incremental(
            "Date"   # <--- field to track, our timestamp
            # initial_value="2009-06-15",   # <--- start date June 15, 2009
            )
        ):
        for fpath in parquet_paths:
            df = pd.read_parquet(fpath)
            print(f'{load_type}-load from {fpath}')
            yield df.to_dict(orient="records")
    
    @dlt.resource(name="fundamentals", primary_key="symbol")
    @staticmethod
    def stock_fundamentals_raw(
        json_paths
        ):
        for fpath in json_paths:
            with open(fpath, encoding='utf-8') as f:
                d = json.load(f)
            print(f'Full-load from {fpath}')
            yield d

    def run_price_pipeline(self):
        if self.dest == 'duckdb':
            pipeline = self.price_pipeline_duckdb
        elif self.dest == 'bigquery':
            pipeline = self.price_pipeline_bq
        else:
            raise ValueError(f"Unknown dest: {self.dest}. Accepted: 'duckdb', 'bigquery'")
        if self.full_load:
            write_disp = 'replace'
            load_type = 'full'
        else:
            write_disp = 'append'
            load_type = 'incremental'
        info = pipeline.run(
            self.stock_prices_raw(self.price_paths, load_type),
            table_name='stock_prices',
            loader_file_format="jsonl",
            write_disposition=write_disp)
        # self.logger.info(info)
        self.logger.info(pipeline.last_trace)


    def run_fundamentals_pipeline(self):
        if self.dest == 'duckdb':
            pipeline = self.fundamentals_pipeline_duckdb
        elif self.dest == 'bigquery':
            pipeline = self.fundamentals_pipeline_bq
        else:
            raise ValueError(f"Unknown dest: {self.dest}. Accepted: 'duckdb', 'bigquery'")
        write_disp = 'merge'
        info = pipeline.run(
            self.stock_fundamentals_raw(self.fund_paths),
            table_name='stock_fundamentals',
            write_disposition=write_disp)
        # self.logger.info(info)
        self.logger.info(pipeline.last_trace)


if __name__ == '__main__':
    # needed for uploading to bigquery:
    gcp_key_fpath = "/home/onur/gcp-keys/stocks-455113-eb2c3f563c78.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key_fpath
    # initialize:
    load_ticker = LoadTickerData(
        full_load=False,
        # dest='duckdb',
        dest='bigquery',
        dev_mode=False,
        log_level='info')
    load_ticker.run_price_pipeline()
    load_ticker.run_fundamentals_pipeline()
    