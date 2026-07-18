"""Tests for start_paper.py / start_live.py.

build_seams() in both scripts now returns real, config-backed seams (H4), so
these tests verify that, plus that main() turns any startup failure into a
logged error and a clean non-zero exit (rather than a raw traceback), and
start_live.py's extra confirmation gate.
"""

from __future__ import annotations

import pytest

import algo.start_live as start_live
import algo.start_paper as start_paper


class TestBuildSeams:
    def test_start_paper_build_seams_returns_the_required_seams(self, monkeypatch):
        # build_seams() now composes a real (read-only) Kite broker for
        # contract/price resolution -- construction needs *some* API key/
        # secret present (no network call happens building the objects), so
        # this stays deterministic without requiring real credentials.
        monkeypatch.setenv("KITE_API_KEY", "test_key")
        monkeypatch.setenv("KITE_API_SECRET", "test_secret")
        seams = start_paper.build_seams()
        try:
            assert set(seams) >= {"instrument_service", "expiry_service", "tick_stream", "broker"}
            # The instrument service actually resolves a configured instrument.
            spec = seams["instrument_service"].get_instrument_spec("NIFTY")
            assert spec.lot_size > 0

            from algo.brokers.paper_trading_broker import PaperTradingBroker

            assert isinstance(seams["broker"], PaperTradingBroker)
            assert seams["broker"].broker_name.value == "SIMULATION"
        finally:
            # SimulationBroker's default (non-synchronous) config starts a
            # background matching-engine thread on construction; close it so
            # this test doesn't leak one into the rest of the test session.
            seams["broker"].close()

    def test_start_live_build_seams_includes_a_token_store(self):
        seams = start_live.build_seams()
        assert set(seams) >= {"instrument_service", "expiry_service", "tick_stream", "access_token_store"}


#: A syntactically-valid Postgres URL that is *always* unreachable, on any
#: machine: TCP port 1 on loopback is never bound by a real service (binding
#: it requires elevated privileges nobody grants Postgres), so the connection
#: is refused in milliseconds -- no DNS lookup (it's a literal IP) and no
#: reliance on a slow timeout. Deliberately NOT "just delete DATABASE_URL":
#: load_database_settings() calls load_dotenv(), which silently refills any
#: unset env var from .env -- so deleting it only tests "DB unreachable" for
#: as long as .env's own DATABASE_URL happens to also be unreachable. If a
#: real local Postgres is ever running (as it was found to be, mid-project),
#: that refill makes the "unreachable" premise false and these tests block
#: forever waiting for a shutdown signal that never comes. Pointing at a
#: guaranteed-closed port sidesteps the environment entirely.
_UNREACHABLE_DATABASE_URL = "postgresql+psycopg2://baduser:badpass@127.0.0.1:1/nonexistent_db"


class TestMainHandlesStartupFailureCleanly:
    def test_start_paper_main_returns_nonzero_when_db_unreachable(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", _UNREACHABLE_DATABASE_URL)
        # DependencyContainer.__init__ fails at the DB step, before anything
        # build_seams() returns is ever touched -- stub it out so this test
        # exercises exactly that (a DB failure -> clean exit 1), not the
        # separate concern of build_seams() composing a real Kite-backed
        # broker (covered by TestBuildSeams) or its background thread.
        monkeypatch.setattr(
            start_paper, "build_seams",
            lambda: {"instrument_service": object(), "expiry_service": object(), "tick_stream": object()},
        )
        assert start_paper.main() == 1

    def test_start_live_main_returns_nonzero_without_confirmation(self, monkeypatch):
        monkeypatch.delenv("I_UNDERSTAND_THIS_TRADES_REAL_MONEY", raising=False)
        assert start_live.main() == 1

    def test_start_live_main_returns_nonzero_when_db_unreachable(self, monkeypatch):
        # Confirmation passes, but the DB is unreachable -> clean exit 1.
        monkeypatch.setenv("I_UNDERSTAND_THIS_TRADES_REAL_MONEY", "yes")
        monkeypatch.setenv("DATABASE_URL", _UNREACHABLE_DATABASE_URL)
        assert start_live.main() == 1


class TestLiveTradingConfirmationGate:
    def test_missing_confirmation_raises(self, monkeypatch):
        monkeypatch.delenv("I_UNDERSTAND_THIS_TRADES_REAL_MONEY", raising=False)
        with pytest.raises(RuntimeError, match="refusing to start live trading"):
            start_live._check_live_trading_confirmed()

    def test_wrong_confirmation_value_raises(self, monkeypatch):
        monkeypatch.setenv("I_UNDERSTAND_THIS_TRADES_REAL_MONEY", "true")
        with pytest.raises(RuntimeError, match="refusing to start live trading"):
            start_live._check_live_trading_confirmed()

    def test_correct_confirmation_value_passes(self, monkeypatch):
        monkeypatch.setenv("I_UNDERSTAND_THIS_TRADES_REAL_MONEY", "yes")
        start_live._check_live_trading_confirmed()  # must not raise
