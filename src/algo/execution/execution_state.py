"""Represents the in-flight execution state of a multi-leg order sequence (e.g.
"leg 1 filled, leg 2 pending") distinct from the strategy-level state_machine.py,
used to detect and recover from a crash mid-entry/mid-exit.

TODO: implement execution state tracking for partial multi-leg completion scenarios.
"""
