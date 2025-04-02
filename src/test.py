import logging

def fetch_tickers_function(num_tickers):
    TICKERS_ALL = ["AAPL", "GOOGL", "MSFT", "AMZN"]
    tickers = TICKERS_ALL[:num_tickers]
    # print(f'type(tickers): {type(tickers)}')
    # print(f'len(tickers): {len(tickers)}')
    logging.info(f"returning tickers: {','.join(tickers)}")
    return tickers

class TestClass():
    def __init__(self, num_tickers):
        self.num_tickers = num_tickers
        self.default_tickers = ["AAPL", "GOOGL", "MSFT", "AMZN"]
    
    def fetch_tickers(self):
        tickers = self.default_tickers[:self.num_tickers]
        return tickers

if __name__=='__main__':
    tickers = fetch_tickers_function(2)
    
