"""Executes a forced close-out of one or more positions when triggered by
kill_switch.py, bypassing the strategy's own exit_logic.py decision path.

TODO: implement forced multi-leg close-out with the same auto-unwind-on-partial-
      failure care as entry_logic.py's own partial-fill handling.
"""
