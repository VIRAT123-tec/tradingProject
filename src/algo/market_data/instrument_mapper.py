"""Maps between broker instrument tokens and internal instrument/trading-symbol
identifiers. Backed by the instrument master synced via scripts/sync_instruments.py.

TODO: implement token <-> trading_symbol <-> internal instrument id resolution.
TODO: keep Nifty (NFO) and Sensex (BFO) mapping logic separate — do not assume shared rules.
"""
