"""Logs order events (placed, filled, rejected, cancelled) tagged with
strategy_id, instrument, and account_id, to support reconciliation.py.

TODO: implement order-event logging hooks called from order_manager.py/order_tracker.py.
"""
