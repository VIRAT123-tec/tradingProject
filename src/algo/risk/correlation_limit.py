"""Guards against correlated risk across instruments (Nifty and Sensex are highly
correlated; treating their loss budgets as fully independent understates real
combined risk on a large single-direction move).

TODO: implement correlation-aware exposure/loss checks once daily_loss_limit.py's
      aggregation scope is finalized.
"""
