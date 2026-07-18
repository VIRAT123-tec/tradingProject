"""Shared decorators (e.g. retry-with-backoff wrapping broker calls, timing/
logging decorators).

TODO: implement a retry/timeout decorator built on tenacity, parameterized by
      config rather than hardcoded attempt counts/delays.
"""
