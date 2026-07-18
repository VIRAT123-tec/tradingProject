from kiteconnect import KiteTicker
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("KITE_API_KEY")
access_token = os.getenv("KITE_ACCESS_TOKEN")

kws = KiteTicker(api_key, access_token)


def on_ticks(ws, ticks):
    print(ticks)


def on_connect(ws, response):
    ws.subscribe([
        256265,      # NIFTY 50
        # 738561,      # Example: Reliance (example token)
        # 2953217,     # Example: TCS (example token)
    ])

    ws.set_mode(ws.MODE_FULL, [
        256265,
        # 738561,
        # 2953217
    ])

kws.on_ticks = on_ticks
kws.on_connect = on_connect

kws.connect()
