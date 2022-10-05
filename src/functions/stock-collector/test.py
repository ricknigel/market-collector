import yfinance as yf

data = yf.download(tickers="000001.SS", start="2022-09-01", end="2022-10-01")

print(data)
