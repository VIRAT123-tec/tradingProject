"""Orchestrates risk checks (daily_loss_limit, exposure_limit, margin_monitor,
correlation_limit) before every order placement — entry and exit alike.

Exit orders that reduce risk must never be blocked by a check meant to gate new
exposure (e.g. a daily-loss-limit trip must not prevent closing an existing position).

TODO: implement the check pipeline and the entry-vs-exit gating distinction.
"""
