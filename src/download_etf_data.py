import logging
import os

from pyarrow import parquet

from etf_scraper import ETFScraper

from src.shared import config_logger, create_dir_if_not_exist
from src.fetch_symbols import FetchSymbols

class DownloadETFData():
    def __init__(
            self,
            ticker,
            log_level='info'):
        config_logger(log_level)
        self.logger = logging.getLogger(__name__)
        self.logger.info(
                'Initialized FetchETFData test mode')
        
        # set datapath and create data directory
        self.rootpath = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))
        self.datapath = os.path.join(self.rootpath, 'data')
        create_dir_if_not_exist(self.datapath, self.logger)
        self.datapath_etf = os.path.join(self.rootpath, 'data', 'etf')
        create_dir_if_not_exist(self.datapath_etf, self.logger)

        if type(ticker) == list:
            self.fund_tickers = ticker
        else:
            if ticker.endswith('.csv'):
                self.ticker = FetchSymbols(
                    file=os.path.join(self.datapath, ticker)
                    ).symbols
            else:
                self.fund_tickers = [ticker]

        self.etf_scraper = ETFScraper()
        self.etfs_df = self.etf_scraper.listings_df

    def download_etf_tickers(self):
        for fund_ticker in self.fund_tickers:
            self.download_tickers_for_etf(fund_ticker)
    
    def download_tickers_for_etf(self, fund_ticker):
        if not self.etfs_df.ticker.str.contains(fund_ticker).any():
            raise ValueError(f"ETF {fund_ticker} not found")
        holdings_df = self.etf_scraper.query_holdings(fund_ticker, None)
        ticker_name = self.etfs_df.fund_name.loc[self.etfs_df.ticker == fund_ticker].values[0]
        # Preserve only non-equity holdings
        holdings_df = holdings_df[holdings_df['asset_class'] == 'Equity']
        self.logger.info(
            f'Fetched {holdings_df.shape[0]} holding tickers for ETF ticker: {fund_ticker} ({ticker_name})')
        self.save_etf_df(holdings_df, fund_ticker)
        self.save_get_etf_holdings(holdings_df, fund_ticker)
    
    def save_etf_df(self, holdings_df, fund_ticker):
        fpath = os.path.join(self.datapath_etf, f'{fund_ticker}.parquet')
        holdings_df.to_parquet(fpath)
        self.logger.info(f'ETF holdings stored in {fpath}')

    def save_get_etf_holdings(self, holdings_df, fund_ticker):
        # write the ticker list to a local csv file
        # Note: it is critical that the filename starts with 'ETF_holdings_' as this will indicate .. 
        # in another step (download_ticker_data.py) that the data of the ETF symbol itself is needed too
        fpath = os.path.join(self.datapath, f'ETF_holdings_{fund_ticker}.csv')
        holdings_df_reduced = holdings_df['ticker']
        holdings_df_reduced.rename('symbol', inplace=True)
        holdings_df_reduced.to_csv(fpath, index=False)
        self.logger.info(f'Holding tickers stored in {fpath}')


if __name__ == "__main__":
    # ded = DownloadETFData(['OEF', 'QTOP'])  # eg ['QTOP', 'OEF'] or 'default_ETFs.csv'
    # ded = DownloadETFData('default_ETFs.csv')  # eg IVV, QTOP, OEF
    ded = DownloadETFData('QTOP')  # eg IVV, QTOP, OEF
    ded.download_etf_tickers()
    