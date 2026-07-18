"""Coordinates a full execution flow (e.g. both legs of a straddle entry/exit)
across order_manager.py, order_tracker.py, and fill_manager.py, applying risk
checks before every order placement.

TODO: implement coordinated multi-leg execution with risk_manager.py checks before
      both entry and exit orders (exits that reduce risk must never be blocked by a
      false daily-loss-limit trip).
"""
