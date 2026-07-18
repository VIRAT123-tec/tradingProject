"""Provides the current underlying spot LTP for an instrument, preferring live
websocket ticks and falling back to a polled quote when the feed is unavailable.

TODO: implement tick-preferred / poll-fallback LTP resolution used by entry_logic.py
      to compute the ATM strike at the configured entry time.
"""
