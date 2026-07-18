"""Daily loss-limit enforcement.

The daily loss limit is IMPLEMENTED, but not in this file: the enforcement
lives in ``risk/risk_core.py`` (``RiskCore._check_daily_loss_limit``), which
recomputes the account's realized P&L from the source-of-truth position rows on
every entry attempt, persists it to ``DailyRiskState``, and latches a sticky
``breached`` flag that blocks all new entries for the rest of the day. It runs
as one of the ordered pre-trade checks in ``validate_entry`` and so is wired
into the entry path exactly as the spec requires.

This module is intentionally left empty: the pre-trade risk surface was
consolidated into the cohesive ``risk_core.py`` rather than spread across one
thin file per check. It remains as a placeholder in case the risk surface later
outgrows one module and a more granular decomposition is wanted.

Not yet covered anywhere: an *intraday unrealized-P&L* loss limit (an open
position sitting at a large paper loss). That is a documented follow-up -- see
docs/deployment.md.
"""
