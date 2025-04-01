from src.download_ticker_prices import DownloadTickerData
import os

ticker = 'MSFT'
dlp = DownloadTickerData(
        ticker=ticker,
        period='1d',
        out_format='parquet',
        test=True)

def test_download_prices():
    dlp.download_prices()
    fpath = os.path.join(dlp.datapath_price, '{}_test.{}'.format(ticker, dlp.out_format))
    assert os.path.exists(fpath)
    os.remove(fpath)