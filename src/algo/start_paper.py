"""Paper-trading process entrypoint: ``python -m algo.start_paper`` (or
``python src/algo/start_paper.py``).

Runs the platform against the Simulation broker. Refuses to start unless
``brokers.yaml``'s ``active_broker`` is SIMULATION (``app.Application``'s
``expected_broker`` check) -- running *this script* is itself the
safety-relevant act of choosing paper mode, and must not silently defer to
whatever the config file happens to say.

Not yet fully runnable -- see ``build_seams()`` below.
"""

from __future__ import annotations

import logging
import sys

from algo.app import Application
from algo.common.enums import BrokerName
from algo.dependency_container import DependencyContainer

_logger = logging.getLogger("algo.start_paper")


def build_seams() -> dict[str, object]:
    """Construct the concrete seams the container needs.

    Returns a config-backed ``InstrumentService`` and ``ExpiryService`` (from
    ``configs/instruments/*.yaml``), the shared live Kite ``tick_stream``
    (see ``services.live_seams.build_kite_tick_stream``, the same one
    ``start_live.py`` uses), and ``broker`` -- ``PaperTradingBroker`` (see
    ``services.paper_trading_seams.build_paper_trading_broker`` and
    ``brokers/paper_trading_broker.py``): it resolves option contracts and
    reads prices through a real, read-only Kite broker (the identical
    ``find_option_contract``/``get_ltp`` code path live trading uses), while
    every order it places is fully simulated. Passing ``broker`` explicitly
    makes ``DependencyContainer`` use this pre-built broker instead of
    constructing one from ``brokers.yaml`` itself -- exactly the override
    its own constructor documents for this kind of external composition. The
    ``SpotPriceProvider`` is intentionally *not* returned here: it needs the
    broker, so the container builds the broker-backed default itself (which
    will now also read the real spot price, via the same composed broker).

    ⚠️ This means paper mode requires a valid daily Kite login
    (``KITE_API_KEY``/``KITE_ACCESS_TOKEN``, via ``scripts/generate_token.py``)
    even though it never places a real order -- both the tick stream and the
    broker's read side need a live Kite session.

    Because contract resolution is now real, the live Kite tick stream can
    genuinely subscribe to a paper position's option legs (they are real
    Kite instruments) -- this is what closes the
    ``InstrumentNotFoundError: no Kite instrument_token found`` gap that
    existed when paper mode resolved purely synthetic, fabricated
    tradingsymbols instead.
    """
    from algo.brokers.kite.kite_auth import EnvAccessTokenStore
    from algo.scheduler.trading_calendar import WeekdayTradingCalendar
    from algo.services.live_seams import (
        ConfigExpiryService,
        ConfigInstrumentService,
        build_kite_tick_stream,
    )
    from algo.services.paper_trading_seams import build_paper_trading_broker

    access_token_store = EnvAccessTokenStore()
    instruments = ConfigInstrumentService()
    return {
        "instrument_service": instruments,
        "expiry_service": ConfigExpiryService(
            instrument_service=instruments, trading_calendar=WeekdayTradingCalendar()
        ),
        "tick_stream": build_kite_tick_stream(access_token_store=access_token_store),
        "broker": build_paper_trading_broker(access_token_store=access_token_store),
    }


def main() -> int:
    """Build the container, run the application to completion (blocks until
    a shutdown signal), and return a process exit code. Startup failures --
    whether in container construction or in ``Application.start()`` -- are
    caught here, logged, and turned into a non-zero exit code rather than a
    raw traceback."""
    from algo.logging.alerting import RecordingAlertDispatcher
    from algo.logging.logger import configure_logging

    configure_logging(logging.INFO, alert_dispatcher=RecordingAlertDispatcher())

    try:
        container = DependencyContainer(**build_seams())
    except Exception:
        _logger.critical("paper trading failed to initialize", exc_info=True)
        return 1

    application = Application(container, expected_broker=BrokerName.SIMULATION)
    try:
        application.run()
    except Exception:
        _logger.critical("paper trading terminated abnormally", exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
