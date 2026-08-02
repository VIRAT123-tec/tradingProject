from kiteconnect import KiteConnect
from dotenv import load_dotenv
import os

load_dotenv()

kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

quote = kite.quote("NSE:NIFTY 50")

print("NIFTY =", quote["NSE:NIFTY 50"]["last_price"])
