"""Structured logging for the platform, tagged with strategy_id, instrument, and
account_id at every state transition and order event, to support reconciliation.py.

Note: this package is named `algo.logging`, not the top-level `logging` module.
Python 3's absolute-import default means `import logging` from anywhere in this
codebase still resolves to the stdlib module, not this package — no shadowing risk,
but worth remembering when reading tracebacks."""
