import logging
import os

import dlt
import pandas as pd

from shared import config_logger

class LoadTickerPrices():
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
        self.datapath = os.path.join(self.rootpath, 'data', 'price')
        self.parquet_paths = self.fetch_parquet_paths()

        # Define dlt pipelines
        self.pipeline_duckdb = dlt.pipeline(
            pipeline_name='load_stock_prices_raw_duckb',
            destination='duckdb',
            dataset_name='stocks_raw',
            dev_mode=self.dev_mode
        )
        
        gcp_key_fpath = "/home/onur/gcp-keys/stocks-455113-eb2c3f563c78.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key_fpath
        self.pipeline_bq = dlt.pipeline(
            pipeline_name="load_stock_prices_raw_bq",
            destination="bigquery",
            dataset_name="stocks_raw",
            dev_mode=self.dev_mode
        )
    
    def fetch_parquet_paths(self):
        # construct a list of absolute paths based on the 
        # (non-test) parquet files in the self.datapth folder
        fpaths = []
        for f in os.listdir(self.datapath):
            if f.endswith('.parquet') and not f.endswith('_test.parquet'):
                fpaths.append(os.path.join(self.datapath, f))
        if len(fpaths) == 0:
            self.logger.warning(f'No parquet files found at {self.datapath}')
        else:
            self.logger.info(f'Found {len(fpaths)} parquet files at {self.datapath}')
        return fpaths

    @dlt.resource(name="prices")
    @staticmethod
    def stocks_raw(
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

    def run_pipeline(self):
        if self.dest == 'duckdb':
            pipeline = self.pipeline_duckdb
        elif self.dest == 'bigquery':
            pipeline = self.pipeline_bq
        else:
            raise ValueError(f"Unknown dest: {self.dest}. Accepted: 'duckdb', 'bigquery'")
        if self.full_load:
            write_disp = 'replace'
            load_type = 'full'
        else:
            write_disp = 'append'
            load_type = 'incremental'
        info = pipeline.run(
            self.stocks_raw(self.parquet_paths, load_type),
            table_name='stock_prices',
            write_disposition=write_disp)
        # self.logger.info(info)
        self.logger.info(pipeline.last_trace)


if __name__ == '__main__':
    load_ticker = LoadTickerPrices(
        full_load=False,
        # dest='duckdb',
        dest='bigquery',
        dev_mode=False,
        log_level='info')
    load_ticker.run_pipeline()
    