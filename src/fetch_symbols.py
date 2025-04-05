import logging
import os

import pandas as pd

from src.shared import config_logger


class FetchSymbols():
    def __init__(
            self,
            file='',
            log_level='info'
        ):
    
        config_logger(log_level)
        self.logger = logging.getLogger()
        
        # input args
        self.file = file
        
        self.logger.info('Instantiated FetchSymbols, fetching')
        self.symbols = self._fetch_symbols()

    def _fetch_symbols(self):
        if self.file != '':
            self.logger.info(f'Attempting to fetch symbols from {self.file}')
            if os.path.exists(self.file):
                symbols = self._fetch_symbols_from_file()
            else:
                raise FileNotFoundError(f'Specified file could not be found at {self.file}')
        else:
            self.logger.info('No file path provided, fetching default symbols')
            symbols = self._fetch_default_symbols()
        return symbols

    def _fetch_symbols_from_file(self):
        with open(self.file, 'r') as f:
            df = pd.read_csv(f)
        cols_lower = [col.lower() for col in list(df.columns)]
        if 'symbol' in cols_lower:
            symbols = list(df.symbol.values)
        else:
            symbols = list(df.iloc[:,0])
        self.logger.info('Fetched symbols from file')
        return symbols

    def _fetch_default_symbols(self):
        self.logger.info('Fetching default symbols')
        default_symbols = [
            "AAPL",
            "GOOGL",
            "MSFT",
            "AMZN"]
        return default_symbols


if __name__ == '__main__':
    rootpath = os.path.dirname(os.path.dirname(__file__))
    fpath = os.path.join(rootpath, 'data', 'default_stocks.csv')
    symbols = FetchSymbols(
        file = fpath
    ).symbols
    print(','.join(symbols))
