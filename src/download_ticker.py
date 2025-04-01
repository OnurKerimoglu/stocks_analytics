import contextlib
import logging
import os

import yfinance as yf


class DownloadTicker():
    def __init__(
              self,
              ticker,
              period, # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
              test=False,
              out_format='parquet',  # csv, parquet
              log_level='info' # valid levels: debug, info
              ):
        self.__config_logger(log_level)
        self.logger = logging.getLogger(__name__)
        if test:
            self.logger.info(
                'Initialized DownloadTicker in test mode')
        else:
            self.logger.info(
                'Initialized DownloadTicker in production mode')

        # input arguments
        if ticker == 'default_list':
            self.ticker = self.get_default_tickers()
        else:
            if type(ticker) == list:
                self.ticker = ticker
            else:
                self.ticker = [ticker]
        self.test = test
        if self.test:
            self.period = '1d'
        self.out_format = out_format

        # set datapath and create data directory
        self.rootpath = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))
        self.datapath = os.path.join(self.rootpath, 'data')
        if not os.path.exists(self.datapath):
            os.makedirs(self.datapath)
            self.logger.info(
                'Created data directory {}'.format(self.datapath))

    def get_default_tickers(self):
        tickers = ["AAPL", "GOOGL", "MSFT", "AMZN"]
        return tickers

    def __config_logger(self, log_level):
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

    def download(self):
        for ticker in self.ticker:
            self.download_single(ticker)
    
    def download_single(self, ticker):
        self.logger.info(
            'Downloading data for {}'.format(ticker))
        data = yf.download(
            ticker,
            period=self.period)
        if len(data.index) == 0:
            self.logger.warning(
                'No data downloaded')
        else:
            suffix = '_test' if self.test else ''
            fpath = os.path.join(
                    self.datapath,'{}{}.{}'.format(ticker, suffix, self.out_format))
            if self.out_format == 'csv':
                with open(fpath, 'w') as f:
                    data.to_csv(f)
            elif self.out_format == 'parquet':
                data.to_parquet(fpath)
            else:
                raise Exception ('out_format must be "csv" or "parquet"')
            self.logger.info(
                'Data downloaded to {}'.format(fpath))



if __name__ == '__main__':
    DownloadTicker(
        ticker='default_list',
        # ticker=['MSFT', 'AAPL'],
        # ticker='MSFT',
        period='max',
        test=True,
        out_format='parquet').download()
