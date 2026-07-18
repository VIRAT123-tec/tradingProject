"""Daily Kite Connect login: ``python scripts/generate_token.py``.

Runs the official Kite Connect authentication flow -- and *only* that flow.
This script never asks for, collects, or stores a password or a TOTP secret:
it opens Kite's own hosted login page in your browser, you log in there
(including your own 2FA, however your Kite account is configured), and Kite
redirects back with a one-time ``request_token`` that this script exchanges
for the day's ``access_token`` via the official API
(``KiteConnect.generate_session``).

What it does, in order:

1. Load ``KITE_API_KEY`` / ``KITE_API_SECRET`` from ``.env`` (never hardcoded,
   never prompted for).
2. Check whether a still-valid token already exists for today (a local
   same-day check, backed by a live ``profile()`` call) -- and skip the login
   entirely if so, unless ``--force`` is given.
3. If a fresh login is required: print and open the official Kite login URL,
   accept the ``request_token`` (or the full redirect URL) you paste back,
   and exchange it for the access token.
4. Validate the new token against the broker's profile API.
5. Store it durably in ``.env`` (via ``EnvFileTokenStore``), so the trading
   process picks it up on its next start.

Exit code is 0 on success, 1 on any failure, so this composes in a morning
startup script. Runs identically on Windows, macOS, and Linux: the browser is
opened via the standard-library ``webbrowser`` module (``os.startfile`` under
the hood on Windows, never a shell command), and all file I/O goes through
``pathlib``/``python-dotenv``.

The interactive step (``_prompt_for_request_token``) is the only part of this
file that performs real I/O (print/open browser/input); everything else is
delegated to :class:`~algo.brokers.kite.token_manager.TokenManager`, which is
what is actually unit-tested -- this script's own logic is intentionally thin.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import webbrowser
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

_logger = logging.getLogger("algo.scripts.generate_token")

#: Kite's redirect after login carries the token as a query parameter, e.g.
#: ``https://your-redirect-url/?request_token=XXXX&action=login&status=success``.
_REQUEST_TOKEN_PARAM = "request_token"


def _prompt_for_request_token(login_url: str) -> str:
    """The interactive step: open the official Kite login page, and accept
    either the bare ``request_token`` or the full redirect URL pasted back.

    This is the only function in this script that performs real I/O
    (printing, opening a browser, reading input) -- it is passed into
    ``TokenManager.ensure_valid_token`` as the ``on_login_required`` callback,
    which is what keeps ``TokenManager`` itself free of any UI concerns and
    fully unit-testable.
    """
    print("\nOpening the official Kite Connect login page in your browser...")
    print(f"If it does not open automatically, visit:\n  {login_url}\n")
    try:
        webbrowser.open(login_url)
    except Exception:  # noqa: BLE001 -- a browser-launch failure is not fatal, just log and continue
        _logger.warning("could not auto-open a browser; open the URL above manually", exc_info=True)

    print(
        "After logging in (with your own Kite credentials and 2FA -- this script "
        "never asks for them), Kite will redirect your browser to a URL containing "
        "'request_token=...'. Paste either that FULL URL or just the token value below."
    )
    raw = input("request_token (or redirect URL): ").strip()
    return _extract_request_token(raw)


def _extract_request_token(raw: str) -> str:
    """Accept either a bare token or a full redirect URL and return the token.

    Pure parsing, no I/O -- kept separate from the prompt so it is directly
    unit-testable against both input shapes.
    """
    if _REQUEST_TOKEN_PARAM in raw and ("://" in raw or "?" in raw):
        query = urlparse(raw).query or raw.split("?", 1)[-1]
        values = parse_qs(query).get(_REQUEST_TOKEN_PARAM)
        if values:
            return values[0].strip()
    return raw.strip()


def _build_token_manager():  # -> TokenManager, import kept local to avoid a hard kiteconnect dependency at module import time
    """Construct a ``TokenManager`` wired to the real Kite client and a
    durable, ``.env``-backed token store. Raises ``RuntimeError`` with a clear
    message if the required credentials are missing.
    """
    from kiteconnect import KiteConnect

    from algo.brokers.kite.kite_auth import KiteSession
    from algo.brokers.kite.token_manager import TokenManager
    from algo.brokers.kite.token_store import EnvFileTokenStore
    from algo.services.time_service import SystemTimeProvider

    load_dotenv()
    api_key = os.environ.get("KITE_API_KEY")
    api_secret = os.environ.get("KITE_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError(
            "KITE_API_KEY and KITE_API_SECRET must both be set in .env before "
            "running the login flow -- see docs/configuration.md."
        )

    client = KiteConnect(api_key=api_key)
    token_store = EnvFileTokenStore()
    session = KiteSession(client=client, api_secret=api_secret, token_store=token_store)
    return TokenManager(
        client=client, session=session, token_store=token_store, time_provider=SystemTimeProvider(),
    )


def main(argv: list[str] | None = None) -> int:
    """Run the login flow. Returns a process exit code (0 success, 1 failure)."""
    parser = argparse.ArgumentParser(
        prog="generate_token", description="Run the official Kite Connect daily login flow."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Run the interactive login even if a still-valid token already exists for today.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")

    try:
        token_manager = _build_token_manager()
    except Exception:
        _logger.critical("failed to initialize the Kite token manager", exc_info=True)
        return 1

    try:
        if not args.force:
            existing = token_manager.check_existing()
            if existing is not None:
                print(f"A valid Kite access token already exists for today (ends in ...{existing[-4:]}).")
                print("Nothing to do. Pass --force to log in again anyway.")
                return 0

        access_token = token_manager.ensure_valid_token(on_login_required=_prompt_for_request_token)
    except Exception:
        _logger.critical("Kite login failed", exc_info=True)
        return 1

    print(f"\nLogin successful. Access token stored (ends in ...{access_token[-4:]}).")
    print("This token is valid until Kite's next daily session reset; run this script again tomorrow.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
