import yfinance as yf
import sys
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import urllib.request
import os
from dotenv import load_dotenv
load_dotenv()

proxy = os.environ.get("proxy")

proxy_support = urllib.request.ProxyHandler({'http': proxy,'https': proxy})

msft = yf.Ticker("MSFT")
print(msft.info)
# print(msft.get_analyst_price_target(proxy=proxy_support))
print(msft.get_analyst_price_target)
# print(msft.analyst_price_target(proxy=proxy_support))