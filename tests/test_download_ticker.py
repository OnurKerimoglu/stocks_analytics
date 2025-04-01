from src.download_ticker import DownloadTicker
import os

ticker = 'MSFT'
download_ticker = DownloadTicker(
        ticker=ticker,
        period='1d',
        out_format='parquet',
        test=True)

def test_download():
    download_ticker.download()
    fpath = os.path.join(download_ticker.datapath, '{}_test.{}'.format(ticker, download_ticker.out_format))
    assert os.path.exists(fpath)
    os.remove(fpath)