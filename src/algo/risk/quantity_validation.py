"""Validates order quantity against instrument lot size and configured lots,
guarding against a miscomputed or stale quantity value reaching the broker.

TODO: implement quantity validation against configs/instruments/*.yaml lot_size.
"""
