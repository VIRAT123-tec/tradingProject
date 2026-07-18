"""Custom exception hierarchy, distinguishing retryable (e.g. network) errors from
non-retryable ones (e.g. invalid symbol, insufficient margin) per the non-negotiable
production requirement that these be handled differently.

TODO: implement exception classes; no bare except: pass anywhere in this codebase.
"""
