"""Provides current CE/PE LTPs for a given strike/expiry, preferring live websocket
ticks and falling back to polling. Feeds combined_premium.py's per-tick recomputation.

TODO: implement tick-preferred / poll-fallback LTP resolution per option leg.
TODO: expose last-updated-at per leg so monitor.py can detect stale/mismatched pairs.
"""
