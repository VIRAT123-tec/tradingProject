"""Margin checks and broker-side square-off detection.

The *pre-trade margin sufficiency* check is IMPLEMENTED, but not in this file:
it lives in ``risk/risk_core.py`` (``RiskCore._check_available_margin``), which
compares the broker's available cash against the estimated required margin
before an entry, as one of the ordered ``validate_entry`` checks.

Detection of *broker-side involuntary square-offs* (RMS closing a position our
own state machine did not initiate) is handled by the startup/periodic
``services/reconciliation_engine.py``, which compares broker truth against the
database and records a reconciliation break when they diverge.

This module is intentionally left empty: like ``daily_loss_limit.py``, its
concern was folded into the cohesive ``risk_core.py`` / reconciliation engine
rather than a thin standalone file. It remains as a placeholder for a future,
more granular decomposition.

Note: the margin figure ``risk_core`` uses is a rough pre-trade sufficiency
estimate from ``configs/risk.yaml``, not a real SPAN calculation -- confirm
before live use (flagged in the config and docs/deployment.md).
"""
