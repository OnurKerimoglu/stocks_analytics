from src.download_ticker_prices import DownloadTickerPrices
import os

ticker = 'MSFT'
dlp = DownloadTickerPrices(
        ticker=ticker,
        period='1d',
        out_format='parquet',
        test=True)

def test_download():
    dlp.download()
    fpath = os.path.join(dlp.datapath, '{}_test.{}'.format(ticker, dlp.out_format))
    assert os.path.exists(fpath)
    os.remove(fpath)