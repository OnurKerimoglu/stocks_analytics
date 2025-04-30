import logging
import os

import dlt
from dlt.destinations.adapters import bigquery_adapter
import json
import pandas as pd

from src.shared import config_logger

class LoadTickerData():
    def __init__(
            self,
            full_load=False,
            dest='duckdb',
            dataset_name='stocks_raw',
            dev_mode=False,
            log_level='info'):
        # configure and start logger
        config_logger(log_level)
        self.logger = logging.getLogger(__name__)
        self.dataset_name = dataset_name
        self.dataset_suf = '_dev' if dev_mode else ''
        loadtype = 'full' if full_load else 'incremental'
        
        mode = 'dev' if dev_mode==True else 'prod'
        self.logger.info(
            f'Initialized LoadTicker in {mode} mode for {loadtype} load to {dest}')
        
        # input arguments
        self.full_load = full_load
        if dest not in ['duckdb', 'bigquery']:
            raise ValueError(f"Unknown dest: {dest}. Accepted: 'duckdb', 'bigquery'")
        self.dest = dest
        self.dev_mode = dev_mode
            
        # set datapath and fetch parquet files
        self.rootpath = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))
        self.datapath_etf= os.path.join(self.rootpath, 'data', 'etf')
        self.datapath_price = os.path.join(self.rootpath, 'data', 'price')
        self.datapath_info = os.path.join(self.rootpath, 'data', 'info')

    def define_pipeline(self, source_type):
        if source_type == 'etf':
            if self.dest == 'duckdb':
                pipeline = dlt.pipeline(
                    pipeline_name='load_etfs_raw_duckb',
                    destination='duckdb',
                    dataset_name=f'{self.dataset_name}{self.dataset_suf}'
                    # dev_mode=self.dev_mode
                )
            elif self.dest == 'bigquery':
                pipeline = dlt.pipeline(
                    pipeline_name="load_etfs_raw_bq",
                    destination="bigquery",
                    staging='filesystem',
                    dataset_name=f'{self.dataset_name}{self.dataset_suf}'
                    # dev_mode=self.dev_mode
                )
        elif source_type == 'info':
            if self.dest == 'duckdb':
                pipeline = dlt.pipeline(
                pipeline_name='load_stock_info_raw_duckb',
                destination='duckdb',
                dataset_name=f'{self.dataset_name}{self.dataset_suf}'
                # dev_mode=self.dev_mode
                )
            elif self.dest == 'bigquery':
                pipeline = dlt.pipeline(
                pipeline_name="load_stock_info_raw_bq",
                destination="bigquery",
                staging='filesystem',
                dataset_name=f'{self.dataset_name}{self.dataset_suf}'
                # dev_mode=self.dev_mode
                )
        elif source_type == 'price':
            if self.dest == 'duckdb':
                pipeline = dlt.pipeline(
                pipeline_name='load_stock_prices_raw_duckb',
                destination='duckdb',
                dataset_name=f'{self.dataset_name}{self.dataset_suf}'
                # dev_mode=self.dev_mode
                )  
            elif self.dest == 'bigquery':
                pipeline = dlt.pipeline(
                pipeline_name="load_stock_prices_raw_bq",
                destination="bigquery",
                staging='filesystem',
                dataset_name=f'{self.dataset_name}{self.dataset_suf}'
                # dev_mode=self.dev_mode
                )
        else:
            raise ValueError(f"Unknown source_type: {source_type}. Accepted: 'etf','info','price'")
        return pipeline
            
    def fetch_data_paths(self, datapath, ext):
        # construct a list of absolute paths based on the 
        # (non-test) files with the specified extension in the self.datapth folder
        fpaths = []
        for f in os.listdir(datapath):
            if f.endswith(f'.{ext}') and not f.endswith(f'_test.{ext}'):
                fpaths.append(os.path.join(datapath, f))
        if len(fpaths) == 0:
            self.logger.warning(f'No {ext} files found at {datapath}')
        else:
            self.logger.info(f'Found {len(fpaths)} {ext} files at {datapath}')
        return fpaths

    @dlt.resource(name="etfs") # , primary_key="symbol")
    @staticmethod
    def etfs_raw(
        fpaths
        ):
        for fpath in fpaths:
            if fpath.endswith('.parquet'):
                df = pd.read_parquet(fpath)
                d = df.to_dict(orient="records")
            print(f'Full-load from {fpath}')
            yield d

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
    
    @dlt.resource(name="info", primary_key="symbol")
    @staticmethod
    def stock_info_raw(
        fpaths
        ):
        for fpath in fpaths:
            if fpath.endswith('.json'):
                with open(fpath, encoding='utf-8') as f:
                    d = json.load(f)
            elif fpath.endswith('.parquet'):
                df = pd.read_parquet(fpath)
                d = df.to_dict(orient="records")
            print(f'Full-load from {fpath}')
            yield d

    def run_etf_pipeline(self):
        pipeline = self.define_pipeline(source_type='etf')
        paths = self.fetch_data_paths(self.datapath_etf, 'parquet')
        write_disp = 'replace'
        info = pipeline.run(
            self.etfs_raw(paths),
            table_name='etfs',
            write_disposition=write_disp)
        # self.logger.info(info)
        self.logger.info(pipeline.last_trace)

    def run_price_pipeline(self):
        pipeline = self.define_pipeline(source_type='price')
        paths = self.fetch_data_paths(self.datapath_price, 'parquet')
        if self.full_load:
            write_disp = 'replace'
            load_type = 'full'
        else:
            write_disp = 'append'
            load_type = 'incremental'
        if self.dest == 'duckdb':
            data = self.stock_prices_raw(paths, load_type)
        elif self.dest == 'bigquery':
            data = bigquery_adapter(
                    self.stock_prices_raw(paths, load_type),
                    cluster = "symbol",
                    autodetect_schema=True
                    )
        info = pipeline.run(
            data,
            table_name='stock_prices',
            loader_file_format="parquet",
            write_disposition=write_disp,
            )
        # self.logger.info(info)
        self.logger.info(pipeline.last_trace)

    def run_info_pipeline(self):
        pipeline = self.define_pipeline(source_type='info')
        paths = self.fetch_data_paths(self.datapath_info, 'json') 
        write_disp = 'replace'
        info = pipeline.run(
            self.stock_info_raw(paths),
            table_name='stock_info',
            loader_file_format="jsonl",
            write_disposition=write_disp)
        # self.logger.info(info)
        self.logger.info(pipeline.last_trace)


if __name__ == '__main__':
    # not needed if google variables are provided in ~/.dlt/secrets.toml
    gcp_key_fpath = "/home/onur/gcp-keys/stocks-455113-eb2c3f563c78.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key_fpath
    # initialize:
    load_ticker = LoadTickerData(
        full_load=True,
        # dest='duckdb',
        dest='bigquery',
        dev_mode=True,
        log_level='info')
    # load_ticker.run_etf_pipeline()
    load_ticker.run_price_pipeline()
    # load_ticker.run_info_pipeline()
    