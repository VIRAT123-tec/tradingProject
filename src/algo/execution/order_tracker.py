"""Tracks the lifecycle of placed orders (pending/open/complete/rejected/cancelled)
against broker updates, keeping the orders table current.

TODO: implement order status tracking, reconciled against broker order updates/postbacks.
"""
