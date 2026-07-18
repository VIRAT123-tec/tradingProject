"""Places and cancels broker orders on behalf of strategies, wrapping every broker
call with retry + timeout + explicit error handling.

TODO: implement order placement/cancellation, distinguishing retryable (network)
      errors from non-retryable (invalid symbol, insufficient margin) errors —
      the latter must go straight to risk/alerting, not retry silently.
"""
