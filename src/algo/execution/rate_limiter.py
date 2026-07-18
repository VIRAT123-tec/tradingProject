"""Throttles outgoing broker API calls to stay within the broker's documented rate
limits (order placement, quotes, historical data).

TODO: implement rate limiting; confirm current Kite API rate-limit numbers before tuning.
"""
