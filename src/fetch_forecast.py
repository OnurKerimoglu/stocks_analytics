from datetime import datetime
import logging
import os

import pandas as pd
import requests

from src.shared import config_logger


class FetchForecast:
    def __init__(
            self,
            api_url: str,
            ticker: str,
            df_hist: pd.DataFrame | None = None,
            log_level='info') -> None:
        config_logger(log_level)
        self.logger = logging.getLogger(__name__)

        # args
        self.api_url = api_url
        self.ticker = ticker
        self.df_hist = df_hist
        if df_hist is None:
            self.endpoint = "v1/forecast/from_symbol"
            logdatasuffix = "without data"
        else:
            self.endpoint = "v1/forecast/from_data"
            logdatasuffix = "with historic data"
        self.logger.info(f"Initialized FetchForecast for ticker: {self.ticker} {logdatasuffix}")
        # constants
        self.past_horizon = 1  # number of past business days (wont be used anyway)
        # derived constants
        self.datestr = datetime.now().strftime("%Y-%m-%d")  # to be added to the df as 'asof'
        # set datapath and create data directory
        self.rootpath = os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))
        datapath = os.path.join(self.rootpath, 'data')
        self.datapath_fcst = os.path.join(datapath, 'forecast')
        if not os.path.exists(self.datapath_fcst):
            os.makedirs(self.datapath_fcst)
            self.logger.info('Created forecast directory {}'.format(self.datapath_fcst))
        # construct filename
        self.out_format = "parquet"
        self.fname = '{}_fcst.{}'.format(self.ticker, self.out_format)
        self.fpath = os.path.join(self.datapath_fcst, self.fname)

    def run(self):
        _, fcst_df = self.call_api()
        self.store_df(fcst_df)
        # for testing:
        # df = pd.read_parquet(self.fpath)
        return self.fpath

    def store_df(self, df):
        # add ticker and asof date columns
        df['Ticker'] = self.ticker
        df['asof'] = pd.to_datetime(self.datestr, utc=True).tz_convert(None)
        df.to_parquet(self.fpath)
        self.logger.info(f'Forecast data stored in {self.fpath}')

    def call_api(self) -> tuple:
        if self.endpoint.split("/")[-1] in ["from_symbol"]:
            self.logger.info(f"Sending the ticker sybol to the forecast API: {self.api_url}/{self.endpoint}")
            pl_in = {"ticker": self.ticker, "past_horizon": self.past_horizon}
        elif self.endpoint.split("/")[-1] in ["from_data"]:
            self.logger.info(f"Formatting and sending ticker data to the forecast API: {self.api_url}/{self.endpoint}")
            pl_in = self.build_payload_with_data(ticker=self.ticker, past_horizon=self.past_horizon)
        resp = requests.post(f"{self.api_url}/{self.endpoint}", json=pl_in, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            past_df, fcst_df = self.transform_data(data)
        else:
            self.logger.error(f"Error (status: {resp.status_code}) fetching stock info for {self.ticker}.")
            past_df, fcst_df = None, None
        return past_df, fcst_df
    
    def transform_data(self, data) -> tuple:
        past_df = pd.DataFrame(data["past"]).rename(columns={"index": "Date"})
        fcst_df = pd.DataFrame(data["forecast"]).rename(columns={"index": "Date"})
        # Convert to dates
        past_df["Date"] = pd.to_datetime(past_df["Date"], utc=True).dt.tz_convert(None)
        fcst_df["Date"] = pd.to_datetime(fcst_df["Date"], utc=True).dt.tz_convert(None)
        # move Date to the front
        past_df = past_df[["Date"] + [col for col in past_df.columns if col != "Date"]]
        fcst_df = fcst_df[["Date"] + [col for col in fcst_df.columns if col != "Date"]]
        return past_df, fcst_df
    
    def build_payload_with_data(self, ticker: str, past_horizon: int) -> dict:
        """
        Takes a dataframe
        and returns the columnar JSON dict expected by the /from_data endpoint.
        """
        df = self.df_hist

        df.columns.name = None
        # Re-introduce the ticker as a regular column
        df["Ticker"] = ticker
        # Make 'Date' a regular column by resetting the index
        df.reset_index(inplace=True)

        if "Date" not in df.columns or "Close" not in df.columns:
            raise ValueError("DataFrame must contain 'Date' and 'Close' columns.")

        df = df[["Date", "Close"]].copy()
        df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")

        # Clean + order
        df = (
            df.dropna(subset=["Date", "Close"])
            .sort_values("Date")
            .drop_duplicates(subset=["Date"], keep="last")
        )

        payload = {
            "ticker": ticker,
            "series": {
                "date": df["Date"].dt.strftime("%Y-%m-%d").tolist(),
                "close": df["Close"].astype(float).tolist(),
            },
            "past_horizon": past_horizon,
        }
        return payload
    

if __name__ == "__main__":
    API_URL_TEMPLATE = os.environ.get("API_URL_TEMPLATE", 'not_found')
    api_url = API_URL_TEMPLATE.replace("ENV", 'prod')
    fpath = FetchForecast(api_url, "AAPL").run()
