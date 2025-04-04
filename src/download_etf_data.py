import logging
import os

from pyarrow import parquet

from etf_scraper import ETFScraper

from src.shared import config_logger, create_dir_if_not_exist

class DownloadETFData():
    def __init__(
            self,
            ticker,
            log_level='info'):
        config_logger(log_level)
        self.logger = logging.getLogger(__name__)
        self.logger.info(
                'Initialized FetchETFData test mode')
        
        if ticker.startswith('ETFholdings_'):
            self.fund_ticker = ticker.split('ETFholdings_')[1]
        else:
            self.fund_ticker = ticker
        
        # set datapath and create data directory
        self.rootpath = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))
        self.datapath = os.path.join(self.rootpath, 'data')
        create_dir_if_not_exist(self.datapath, self.logger)
        self.datapath_etf = os.path.join(self.rootpath, 'data', 'etf')
        create_dir_if_not_exist(self.datapath_etf, self.logger)

        self.etf_scraper = ETFScraper()
        self.etfs_df = self.etf_scraper.listings_df

    def download_etf_tickers(self):
        if not self.etfs_df.ticker.str.contains(self.fund_ticker).any():
            raise ValueError(f"ETF {self.fund_ticker} not found")
        holdings_df = self.etf_scraper.query_holdings(self.fund_ticker, None)
        ticker_name = self.etfs_df.fund_name.loc[self.etfs_df.ticker == self.fund_ticker].values[0]
        self.logger.info(
            f'Fetched {holdings_df.shape[0]} holding tickers for ETF ticker: {self.fund_ticker} ({ticker_name})')
        self.save_etf_df(holdings_df)
        self.save_get_etf_holdings(holdings_df)
    
    def save_etf_df(self, holdings_df):
        fpath = os.path.join(self.datapath_etf, f'{self.fund_ticker}.parquet')
        holdings_df.to_parquet(fpath)
        self.logger.info(f'ETF holdings stored in {fpath}')

    def save_get_etf_holdings(self, holdings_df):
        # write the ticker list to a local csv file
        fpath = os.path.join(self.datapath, f'ETF_holdings_{self.fund_ticker}.csv')
        holdings_df_reduced = holdings_df['ticker']
        holdings_df_reduced.rename('symbol', inplace=True)
        holdings_df_reduced.to_csv(fpath, index=False)
        self.logger.info(f'Holding tickers stored in {fpath}')


if __name__ == "__main__":
    ded = DownloadETFData('IVV')
    ded.download_etf_tickers()
    