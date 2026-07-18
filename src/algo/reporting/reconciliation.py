"""Compares broker positions/orders against DB state to detect drift (e.g. from a
crash mid-entry, a broker-side involuntary square-off, or a DB write failure after
a successful broker order).

TODO: implement reconciliation checks; needed early despite being under reporting/,
      since it underpins the crash-recovery and duplicate-entry mitigations discussed
      in the engineering review.
"""
