"""Options Market Data Collection System.

A fully independent, additive subsystem that continuously collects live options
market data (ATM ±N strikes, CE+PE, per configured underlying) over its own Kite
websocket and stores it in a separate TimescaleDB (or plain Postgres) database.

It shares NO runtime state with the trading platform: its own websocket, its own
database engine, its own queue/writer threads, its own lifecycle. It only reuses
stateless libraries (token/instrument resolution, ATM math, config, engine
builder, trading calendar). Run it as its own process: ``python -m algo.start_collector``.
"""
