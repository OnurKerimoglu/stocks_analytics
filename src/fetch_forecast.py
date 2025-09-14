from datetime import datetime
import logging
import os
import uuid

import pandas as pd
import pyarrow.parquet as pq
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
            self.endpoint = "v2/forecast"
            self.api_signature_name = "from_symbol"
            logdatasuffix = "without data"
        else:
            self.endpoint = "v2/forecast"
            self.api_signature_name = "from_data"
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
        _, fcst_df, meta = self.call_api()
        if fcst_df is not None:
            self.store_df(fcst_df, meta)
            # df = pd.read_parquet(self.fpath)  # for testing
            schema_str = pq.read_schema(self.fpath)
            self.logger.info(f"schema: {schema_str}")
            return self.fpath

    def store_df(self, df, meta):
        # add ticker and asof date columns
        df['Ticker'] = self.ticker
        df['asof'] = pd.to_datetime(self.datestr, utc=True).tz_convert(None)
        df['model_registry_name'] = meta['model_registry_name']
        df['model_alias'] = meta['model_alias']
        df['model_version'] = meta['model_version']
        df['model_trained_at'] = meta['model_trained_at']
        df['model_run_id'] = meta['model_run_id']
        df.to_parquet(
            self.fpath,
            engine="pyarrow",
            index=False,
            coerce_timestamps="us",         # convert from ns to microsecond
            allow_truncated_timestamps=True # drop any sub-microsecond precision
        )
        self.logger.info(f'Forecast data stored in {self.fpath}')

    def call_api(self) -> tuple:
        if self.api_signature_name == "from_symbol":
            self.logger.info(f"Sending the ticker sybol to the forecast API: {self.api_url}/{self.endpoint}")
            pl_in = {"ticker": self.ticker, "past_horizon": self.past_horizon, "signature_name": self.api_signature_name}
        elif self.api_signature_name == "from_data":
            self.logger.info(f"Formatting and sending ticker data to the forecast API: {self.api_url}/{self.endpoint}")
            pl_in = self.build_payload_with_data(ticker=self.ticker, past_horizon=self.past_horizon)
        # resp = requests.post(f"{self.api_url}/{self.endpoint}", json=pl_in, timeout=30)
        pl_out = self.post_json(f"{self.api_url}/{self.endpoint}", pl_in)
        if pl_out is None:
            past_df, fcst_df, meta = None, None, None
        else:
            meta = pl_out.get("meta", {})
            data = pl_out["data"]
            past_df, fcst_df = self.transform_data(data)
        return past_df, fcst_df, meta

    def post_json(
        self,
        url: str,
        payload: dict,
        *,
        request_id: str | None = None,
        timeout: float = 30.0,
        session: requests.Session | None = None,
    ) -> dict:
        """POST JSON, include/echo X-Request-ID, raise ApiError on non-2xx."""
        rid = request_id or str(uuid.uuid4())
        headers = {
            "Accept": "application/json, application/problem+json",
            "X-Request-ID": rid,
        }
        http = session or requests
        resp = http.post(url, json=payload, headers=headers, timeout=timeout)

        server_rid = resp.headers.get("X-Request-ID", "Unknown")
        if resp.status_code == 200:
            # Success: return JSON
            pl_out = resp.json()
        else:
            self.logger.error(f"Error (status: {resp.status_code}) fetching stock info for {self.ticker}.")
            pl_out = None

        if isinstance(pl_out, dict):
            meta = pl_out.setdefault("meta", {})
            # if meta doesn't contain request_id, inject server_rid read from the header
            if "request_id" not in meta:
                meta["request_id"] = server_rid
        return pl_out
    
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
            "signature_name": self.api_signature_name
        }
        return payload


if __name__ == "__main__":
    API_URL_TEMPLATE = os.environ.get("API_URL_TEMPLATE", "https://stocks-forecasting-service-ENV-228341556620.europe-west1.run.app")
    api_url = API_URL_TEMPLATE.replace("ENV", 'prod')
    fpath = FetchForecast(api_url, "AAPL").run()
