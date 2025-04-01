import contextlib
import logging
import os

import yfinance as yf

from src.shared import config_logger

class DownloadTickerPrices():
    def __init__(
              self,
              ticker,
              period='5d', # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
              test=False,
              out_format='parquet',  # csv, parquet
              log_level='info' # valid levels: debug, info
              ):
        config_logger(log_level)
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
            self.period = '5d'
        else:
            self.period = period
        self.out_format = out_format

        # set datapath and create data directory
        self.rootpath = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))
        self.datapath = os.path.join(self.rootpath, 'data', 'prices')
        if not os.path.exists(self.datapath):
            os.makedirs(self.datapath)
            self.logger.info(
                'Created data directory {}'.format(self.datapath))

    def get_default_tickers(self):
        tickers = ["AAPL", "GOOGL", "MSFT", "AMZN"]
        return tickers

    def download(self):
        for ticker in self.ticker:
            self.download_single(ticker)
    
    def download_single(self, ticker):
        self.logger.info(
            'Downloading data for {}'.format(ticker))
        df = yf.download(
            ticker,
            period=self.period)
        if len(df.index) == 0:
            self.logger.warning(
                'No data downloaded')
        else:
            # This is multi-index, with the second level being the ticker name, which we want to add as a column
            tickers = list(set([col[1] for col in df.columns.values]))
            if len(tickers) > 1:
                self.logger.error(f'Dataframe contains multiple tickers')
                return
            df['Symbol'] = tickers[0]
            # time index is not needed
            df = df.reset_index()
            # The first level is the actual column names
            df.columns = [col[0] for col in df.columns.values]
            suffix = '_test' if self.test else ''
            fpath = os.path.join(
                    self.datapath,'{}{}.{}'.format(ticker, suffix, self.out_format))
            if self.out_format == 'csv':
                with open(fpath, 'w') as f:
                    df.to_csv(f)
            elif self.out_format == 'parquet':
                df.to_parquet(fpath)
            else:
                raise Exception ('out_format must be "csv" or "parquet"')
            self.logger.info(
                'Data downloaded to {}'.format(fpath))


if __name__ == '__main__':
    DownloadTickerPrices(
        ticker='default_list',
        # ticker=['MSFT', 'AAPL'],
        # ticker='MSFT',
        period='max',
        test=False,
        out_format='parquet').download()
