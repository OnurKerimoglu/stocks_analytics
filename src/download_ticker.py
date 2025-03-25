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
        self.ticker = ticker
        self.period = period
        self.test = test

        # set datapath and create data directory
        self.rootpath = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))
        self.datapath = os.path.join(self.rootpath, 'data')
        if not os.path.exists(self.datapath):
            os.makedirs(self.datapath)
            self.logger.info(
                'Created data directory {}'.format(self.datapath))

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

    def download_silent(self):
        self.logger.info(
            'Downloading data for {}'.format(self.ticker))
        # silence the verbose YF-API
        with open(os.devnull, 'w') as devnull:
            with contextlib.redirect_stdout(devnull):
                data = yf.download(
                    self.ticker,
                    period=self.period)
                if len(data.index) == 0:
                    self.logger.warning(
                        'No data downloaded')
                else:
                    suffix = '_test' if self.test else ''
                    fpath = os.path.join(
                            self.datapath,'{}{}.csv'.format(self.ticker, suffix))
                    data.to_csv(fpath)
                    self.logger.info(
                        'Data downloaded to {}'.format(fpath))


if __name__ == '__main__':
    DownloadTicker(
        ticker='MSFT',
        period='1d').download_silent()
