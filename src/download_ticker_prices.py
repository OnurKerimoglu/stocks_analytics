import json
import logging
import os

import yfinance as yf

from src.shared import config_logger

class DownloadTickerData():
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
        self.datapath_price = os.path.join(self.rootpath, 'data', 'price')
        if not os.path.exists(self.datapath_price):
            os.makedirs(self.datapath_price)
            self.logger.info(
                'Created data directory {}'.format(self.datapath_price))
        self.datapath_info = os.path.join(self.rootpath, 'data', 'info')
        if not os.path.exists(self.datapath_info):
            os.makedirs(self.datapath_info)
            self.logger.info(
                'Created data directory {}'.format(self.datapath_info))

    def get_default_tickers(self):
        tickers = ["AAPL", "GOOGL", "MSFT", "AMZN"]
        return tickers

    def download_infos(self):
        for ticker in self.ticker:
            self.download_info_single(ticker)

    def download_prices(self):
        for ticker in self.ticker:
            self.download_price_single(ticker)
    
    def download_info_single(self, ticker):
        self.logger.info(
            'Downloading info for {}'.format(ticker))
        try:
            stock = yf.Ticker(ticker)
            info = stock.info  # Fetch all available info
        except Exception as e:
            self.logger.error(f"Error fetching stock info for {ticker}: {str(e)}")
            info = {}
        if len(info) == 1:
            fundamentals = {}
        else:
            self.logger.info(f"Fetched stock info for {ticker}")
            fundamentals = self.get_stock_fundamentals(info)

        fpath = os.path.join(self.datapath_info, '{}.json'.format(ticker))
        with open(fpath, 'w') as fp:
            json.dump(fundamentals, fp)
        self.logger.info(
            'Data downloaded to {}'.format(fpath))

    def get_stock_fundamentals(
            self,
            info: dict
            ) -> dict:
        """
        Attempts to fetches key fundamental metrics for a given stock ticker using yfinance.
        Args:
        info: dictionary of stock info
        Returns:
        dict: Dictionary of fundamental metrics with human-readable keys
        """

        # Define a safe fetch method to avoid entire function failure
        def safe_get(key, transform=None):
            value = info.get(key, "Metric not available")
            if value != "Metric not available" and transform:
                try:
                    return transform(value)
                except Exception:
                    return "metric not available"
            return value

        # Extract key financial metrics
        fundamentals = {
            "Company Name": safe_get("longName"),
            "Sector": safe_get("sector"),
            "Industry": safe_get("industry"),
            "Market Capitalization": safe_get("marketCap"),
            
            # Valuation Metrics
            "P/E Ratio (Trailing)": safe_get("trailingPE"),
            "P/E Ratio (Forward)": safe_get("forwardPE"),
            "P/B Ratio (Price to Book)": safe_get("priceToBook"),
            "P/S Ratio (Price to Sales)": safe_get("priceToSalesTrailing12Months"),
            "Dividend Yield (%)": safe_get("dividendYield", lambda x: round(x * 100, 2)),

            # Profitability Metrics
            "Earnings Per Share (EPS)": safe_get("trailingEps"),
            "Return on Equity (ROE)": safe_get("returnOnEquity"),
            "Return on Assets (ROA)": safe_get("returnOnAssets"),
            "Gross Margin (%)": safe_get("grossMargins", lambda x: round(x * 100, 2)),
            "Operating Margin (%)": safe_get("operatingMargins", lambda x: round(x * 100, 2)),

            # Financial Health Metrics
            "Debt-to-Equity Ratio": safe_get("debtToEquity"),
            "Current Ratio": safe_get("currentRatio"),
            "Quick Ratio": safe_get("quickRatio"),
            "Interest Coverage Ratio": safe_get(
                "ebitda",
                lambda ebitda: round(ebitda / self.info["totalDebt"], 2) if self.info.get("totalDebt") else "Metric not available"
            ),

            # Growth Metrics
            "Revenue Growth (%)": safe_get("revenueGrowth", lambda x: round(x * 100, 2)),
            "EPS Growth (%)": safe_get("earningsGrowth", lambda x: round(x * 100, 2)),

            # Market & Ownership
            "Institutional Ownership (%)": safe_get("heldPercentInstitutions", lambda x: round(x * 100, 2)),
            "Insider Ownership (%)": safe_get("heldPercentInsiders", lambda x: round(x * 100, 2)),
        }
        
        return fundamentals

    def download_price_single(self, ticker):
        self.logger.info(
            'Downloading price data for {}'.format(ticker))
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
                    self.datapath_price,'{}{}.{}'.format(ticker, suffix, self.out_format))
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
    dtd = DownloadTickerData(
        ticker='default_list',
        # ticker=['MSFT', 'AAPL'],
        # ticker='MSFT',
        period='max',
        test=False,
        out_format='parquet')
    dtd.download_prices()
    # dtd.download_infos()
