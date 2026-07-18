"""Live-trading process entrypoint: ``python -m algo.start_live`` (or
``python src/algo/start_live.py``).

Runs the platform against the real Kite broker -- real orders, real capital.
Refuses to start unless ``brokers.yaml``'s ``active_broker`` is KITE
(``app.Application``'s ``expected_broker`` check), and additionally refuses to
start unless the ``I_UNDERSTAND_THIS_TRADES_REAL_MONEY`` environment variable
is set to exactly ``"yes"``.

That second gate is deliberately stricter than anything the other tasks in
this project asked for -- it is a judgment call, not a stated requirement, and
is flagged as such in this task's "assumptions" summary. It exists because
this is the one script whose entire purpose is placing real orders with real
money, and the platform's own standing correctness bar treats "silently do
the dangerous thing because a config file happened to say so" as an
unacceptable failure mode -- a stray cron job, a copy-pasted command, or an
operator running this instead of start_paper.py by habit should never be
enough, on its own, to start live trading.

Not yet fully runnable -- see ``build_seams()`` below.
"""

from __future__ import annotations

import logging
import os
import sys

from algo.app import Application
from algo.common.enums import BrokerName
from algo.dependency_container import DependencyContainer

_logger = logging.getLogger("algo.start_live")

_CONFIRMATION_ENV_VAR = "I_UNDERSTAND_THIS_TRADES_REAL_MONEY"
_CONFIRMATION_VALUE = "yes"


def build_seams() -> dict[str, object]:
    """Construct the concrete seams and the Kite access-token store the live
    container needs.

    Returns config-backed ``InstrumentService``/``ExpiryService`` (from
    ``configs/instruments/*.yaml``), the shared live Kite ``tick_stream`` (see
    ``services.live_seams.build_kite_tick_stream`` -- the same real websocket
    ``start_paper.py`` now also uses), and an ``EnvAccessTokenStore`` (the
    day's token from the ``KITE_ACCESS_TOKEN`` environment variable, reused for
    both the broker session and the tick stream's own Kite client). The
    container builds the broker-backed ``SpotPriceProvider`` itself.

    Before a real live run, confirm: the instrument-config values (lot size,
    strike interval, and especially the expiry weekday -- see the warnings in
    ``configs/instruments/*.yaml``), and that a valid ``KITE_ACCESS_TOKEN`` was
    minted by the daily login.
    """
    from algo.brokers.kite.kite_auth import EnvAccessTokenStore
    from algo.scheduler.trading_calendar import WeekdayTradingCalendar
    from algo.services.live_seams import (
        ConfigExpiryService,
        ConfigInstrumentService,
        build_kite_tick_stream,
    )

    instruments = ConfigInstrumentService()
    access_token_store = EnvAccessTokenStore()
    return {
        "instrument_service": instruments,
        "expiry_service": ConfigExpiryService(
            instrument_service=instruments, trading_calendar=WeekdayTradingCalendar()
        ),
        "tick_stream": build_kite_tick_stream(access_token_store=access_token_store),
        "access_token_store": access_token_store,
    }


def _check_live_trading_confirmed() -> None:
    """Refuse to proceed unless the operator has explicitly acknowledged this
    run trades real money. See the module docstring for why this exists."""
    if os.environ.get(_CONFIRMATION_ENV_VAR) != _CONFIRMATION_VALUE:
        raise RuntimeError(
            f"refusing to start live trading: set the {_CONFIRMATION_ENV_VAR} "
            f"environment variable to {_CONFIRMATION_VALUE!r} to confirm this "
            "run places real orders with real money."
        )


def main() -> int:
    """Build the container, run the application to completion (blocks until
    a shutdown signal), and return a process exit code. Startup failures --
    the confirmation gate, container construction, or ``Application.start()``
    -- are caught here, logged, and turned into a non-zero exit code rather
    than a raw traceback."""
    from algo.logging.alerting import RecordingAlertDispatcher
    from algo.logging.logger import configure_logging

    configure_logging(logging.INFO, alert_dispatcher=RecordingAlertDispatcher())

    try:
        _check_live_trading_confirmed()
        container = DependencyContainer(**build_seams())
    except Exception:
        _logger.critical("live trading failed to initialize", exc_info=True)
        return 1

    application = Application(container, expected_broker=BrokerName.KITE)
    try:
        application.run()
    except Exception:
        _logger.critical("live trading terminated abnormally", exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
