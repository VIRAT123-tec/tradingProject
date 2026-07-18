"""Watches heartbeat.py output and tick_router.py's last-tick timestamps to detect
a stalled process or stale market data, escalating to alert_dispatcher.py.

TODO: implement watchdog checks, including the tick-staleness watchdog flagged in
      the engineering review.
"""
