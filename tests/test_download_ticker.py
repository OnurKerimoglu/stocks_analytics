from src.download_ticker import DownloadTicker
import os

ticker = 'MSFT'
download_ticker = DownloadTicker(
        ticker=ticker,
        period='1d',
        test=True)

def test_download_silent():
    download_ticker.download_silent()
    fpath = os.path.join(download_ticker.datapath, '{}_test.csv'.format(ticker))
    assert os.path.exists(fpath)
    os.remove(fpath)