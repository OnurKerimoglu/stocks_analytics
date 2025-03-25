import contextlib
import logging
import os

import yfinance as yf


class DownloadTicker():
    def __init__(
              self,
              ticker,
              period, # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
              log_level='info' # valid levels: debug, info
              ):
        self.ticker = ticker
        self.period = period
        self.rootpath = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.datapath = os.path.join(self.rootpath, 'data')
        if not os.path.exists(self.datapath):
            os.makedirs(self.datapath)

        self.__config_logger(log_level)
        self.logger = logging.getLogger(__name__)

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
        # silence the verbose YF-API
        with open(os.devnull, 'w') as devnull:
            with contextlib.redirect_stdout(devnull):
                data = yf.download(
                    self.ticker,
                    period=self.period)
                if len(data.index) == 0:
                    self.logger.warning(
                        'No data downloaded for {}'.format(self.ticker))
                else:
                    fpath = os.path.join(
                            self.datapath,'{}.csv'.format(self.ticker)
                            )
                    data.to_csv(fpath)
                    self.logger.info(
                        'Data downloaded for {}'.format(self.ticker))


if __name__ == '__main__':
    DownloadTicker(
        ticker='MSFT',
        period='1d').download_silent()
