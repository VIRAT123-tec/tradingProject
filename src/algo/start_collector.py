"""Options Market Data Collector entrypoint: ``python -m algo.start_collector``.

A standalone process, independent of the trading platform. It builds its own
Kite websocket (FULL mode), its own market-data database engine, its own
queue/writer, and runs the automatic market-hours session.

Flags:
  (none)         run the collector until SIGINT/SIGTERM (graceful drain on stop)
  --init-only    create/upgrade the market-data schema (hypertable etc.) and exit
  --estimate     print a storage-size estimate for the configured window and exit
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from algo.market_data_collector.collector_service import CollectorService
    from algo.market_data_collector.config import CollectorConfig

_logger = logging.getLogger("algo.start_collector")


def build_collector() -> CollectorService:
    """Wire up the full collector from configs/collector.yaml and the environment."""
    from algo.brokers.kite.kite_auth import EnvAccessTokenStore
    from algo.market_data_collector.atm_window import AtmWindowManager
    from algo.market_data_collector.collector_service import CollectorService
    from algo.market_data_collector.config import load_collector_config
    from algo.market_data_collector.db import build_market_data_engine
    from algo.market_data_collector.full_tick_stream import CollectorTickStream
    from algo.market_data_collector.init_timescale import initialize_schema
    from algo.market_data_collector.instrument_chain import KiteMarketReader
    from algo.market_data_collector.market_hours import MarketHoursController
    from algo.market_data_collector.metrics import CollectorMetrics
    from algo.market_data_collector.tick_writer import TickWriter
    from algo.services.holiday_service import HolidayAwareTradingCalendar, HolidayService
    from algo.services.live_seams import ConfigExpiryService, ConfigInstrumentService
    from algo.services.time_service import SystemTimeProvider

    config = load_collector_config()
    engine = build_market_data_engine(config)
    initialize_schema(engine, config)

    instruments = ConfigInstrumentService()
    holiday_service = HolidayService.from_config()
    calendar = HolidayAwareTradingCalendar(holiday_service)
    expiry = ConfigExpiryService(instrument_service=instruments, holiday_service=holiday_service)
    tokens = EnvAccessTokenStore()
    reader = KiteMarketReader(
        client_factory=lambda: _kite_client(tokens), instrument_service=instruments
    )
    atm = AtmWindowManager(
        instrument_service=instruments,
        expiry_resolver=expiry.get_current_weekly_expiry,
        spot_source=reader.spot_ltp,
        chain_source=reader.option_chain,
        strikes_each_side=config.strikes_each_side,
    )
    writer = TickWriter(engine=engine, config=config.writer)

    # Break the stream<->metrics cycle with a late-bound holder.
    holder: dict[str, CollectorMetrics] = {}

    def on_tick(tick):
        m = holder.get("m")
        if m is not None:
            m.on_tick()
        writer.enqueue(tick)

    def on_reconnect():
        m = holder.get("m")
        if m is not None:
            m.on_reconnect()

    stream = CollectorTickStream(
        ticker_factory=lambda: _kite_ticker(tokens),
        mode=config.tick_mode,
        on_tick=on_tick,
        on_reconnect=on_reconnect,
    )
    metrics = CollectorMetrics(stream=stream, writer=writer, atm=atm, underlyings=config.underlyings)
    holder["m"] = metrics

    return CollectorService(
        config=config, engine=engine, stream=stream, writer=writer, atm=atm,
        metrics=metrics, market_hours=MarketHoursController(config.market_hours, calendar),
        time_provider=SystemTimeProvider(),
    )


def _require_env(name: str) -> str:
    import os

    v = os.environ.get(name)
    if not v:
        from algo.brokers.exceptions import BrokerAuthenticationError

        raise BrokerAuthenticationError(f"{name} must be set (in .env) for the collector")
    return v


def _kite_client(tokens):
    from kiteconnect import KiteConnect

    client = KiteConnect(api_key=_require_env("KITE_API_KEY"))
    client.set_access_token(tokens.get_access_token())
    return client


def _kite_ticker(tokens):
    from kiteconnect import KiteTicker

    return KiteTicker(_require_env("KITE_API_KEY"), tokens.get_access_token())


# --- Storage estimate -----------------------------------------------------

# Labelled planning assumptions; real numbers are measured on day 1.
_TICKS_PER_INSTRUMENT_PER_DAY = 15_000   # FULL mode, active option, ~6.25h session
_BYTES_PER_ROW_RAW = 300                 # FULL row incl. JSONB depth
_COMPRESSION_RATIO = 12                  # typical TimescaleDB tick compression
_TRADING_DAYS_MONTH = 21
_TRADING_DAYS_YEAR = 250


def print_storage_estimate(config: CollectorConfig, *, out=sys.stdout) -> None:
    instruments = len(config.underlyings) * config.instruments_per_underlying
    rows_day = instruments * _TICKS_PER_INSTRUMENT_PER_DAY
    gb_day = rows_day * _BYTES_PER_ROW_RAW / 1e9
    comp = gb_day / _COMPRESSION_RATIO
    print("Storage estimate (planning assumptions -- verify on day 1):", file=out)
    print(f"  Underlyings                : {len(config.underlyings)}  {config.underlyings}", file=out)
    print(f"  Instruments (total)        : {instruments:,}  ({config.instruments_per_underlying}/underlying)", file=out)
    print(f"  Assumed ticks/instr/day    : {_TICKS_PER_INSTRUMENT_PER_DAY:,}", file=out)
    print(f"  Assumed bytes/row (raw)    : {_BYTES_PER_ROW_RAW}", file=out)
    print(f"  Assumed compression ratio  : {_COMPRESSION_RATIO}x", file=out)
    print("  ---", file=out)
    print(f"  Rows / day                 : {rows_day:,}", file=out)
    print(f"  Raw   GB/day  GB/month  GB/year : {gb_day:.2f}  {gb_day*_TRADING_DAYS_MONTH:.1f}  {gb_day*_TRADING_DAYS_YEAR:.0f}", file=out)
    print(f"  Compressed GB/day GB/month GB/year : {comp:.2f}  {comp*_TRADING_DAYS_MONTH:.1f}  {comp*_TRADING_DAYS_YEAR:.0f}", file=out)


def main(argv: Sequence[str] | None = None) -> int:
    from algo.logging.logger import configure_logging

    parser = argparse.ArgumentParser(prog="algo.start_collector", description="Options market data collector.")
    parser.add_argument("--init-only", action="store_true", help="Create/upgrade the schema and exit.")
    parser.add_argument("--estimate", action="store_true", help="Print a storage estimate and exit.")
    args = parser.parse_args(argv)

    configure_logging(logging.INFO)

    if args.estimate:
        from algo.market_data_collector.config import load_collector_config

        print_storage_estimate(load_collector_config())
        return 0

    if args.init_only:
        from algo.market_data_collector.config import load_collector_config
        from algo.market_data_collector.db import build_market_data_engine
        from algo.market_data_collector.init_timescale import initialize_schema

        config = load_collector_config()
        applied = initialize_schema(build_market_data_engine(config), config)
        print(f"schema initialised: {applied}")
        return 0

    try:
        service = build_collector()
    except Exception:
        _logger.critical("collector failed to initialize", exc_info=True)
        return 1

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, lambda *_: service.request_stop())
        except (ValueError, OSError):
            pass  # not on the main thread (e.g. tests) -- caller drives stop()

    try:
        service.run_forever()
    except Exception:
        _logger.critical("collector terminated abnormally", exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
