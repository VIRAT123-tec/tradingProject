<<<<<<< HEAD
# algo_platform

An automated options trading platform that runs a **short straddle** strategy on
Nifty and Sensex. It can run against a **simulation broker** (paper trading, no
real money) or the real **Kite / Zerodha** broker (live trading), using the same
strategy code for both.

It's built for one thing above all: **correctness and crash-recovery.** The
database is the single source of truth, and the platform is designed so that a
crash at any moment — even mid-order — leaves no duplicate or forgotten orders.

---

## ⚠️ Current status: not ready for real money

The platform is well-built and thoroughly tested (**624 automated tests
passing**). The one Critical gap, **all four High-severity gaps, and all three
Medium-severity gaps** have now been **fixed and verified**: the intraday
stop-loss/target heartbeat, the enforced daily loss limit, a working kill switch
(with an operator CLI), connected live fill notifications, a
connectivity-verified live startup flow, concurrent instrument entry, bounded
live-order timeouts, and critical-event alerting. What remains is **Low
severity** plus operational wiring (a real alert channel, confirming
instrument-config values like the expiry weekday, and the daily-login script).
It is **not yet cleared for real money** pending those, but no Critical/High/
Medium issues remain.

👉 **Before running this with any money, read
[`docs/deployment.md`](docs/deployment.md) Section 1.** It lists every known
gap, honestly and with severity. Do not skip it.

---

## Documentation

Start with whichever fits what you need:

| I want to… | Read |
|---|---|
| Understand how it works | [`docs/architecture.md`](docs/architecture.md) |
| Know what each folder does | [`docs/modules.md`](docs/modules.md) |
| Install it from scratch | [`docs/installation.md`](docs/installation.md) |
| Configure it | [`docs/configuration.md`](docs/configuration.md) |
| Run / deploy it | [`docs/deployment.md`](docs/deployment.md) |
| Operate it day to day | [`docs/operations.md`](docs/operations.md) |
| Fix a problem | [`docs/troubleshooting.md`](docs/troubleshooting.md) |

New to the project? Read **architecture → installation → deployment** in that
order.

---

## Quick start (gets you to "tests pass")

```bash
# 1. Set up a virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1        # Windows
# source venv/bin/activate          # macOS / Linux

# 2. Install
pip install -e ".[dev]"

# 3. Point at a Postgres database (in a .env file at the repo root)
#    DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/algo

# 4. Create the tables
alembic upgrade head

# 5. Run the tests
pytest src/algo/tests -q
```

Full details, including the database setup, are in
[`docs/installation.md`](docs/installation.md).

---

## Running it

```bash
python -m algo.start_paper     # paper mode (simulation broker)
python -m algo.start_live      # live mode (real Kite broker)
```

Paper mode now starts end-to-end (connectivity-verified). A full paper *trade*
additionally needs the simulation broker seeded with an option chain + prices;
live mode needs a valid `KITE_ACCESS_TOKEN` and confirmed instrument config. See
[`docs/deployment.md`](docs/deployment.md) for the details.

---

## What's built

- ✅ Database layer (schema, models, repositories, migrations)
- ✅ Broker abstraction + simulation broker + Kite broker
- ✅ Market data layer (with polling fallback)
- ✅ Strategy engine + Strategy-1 (straddle: strike selection, entry, exit,
  monitoring, state machine)
- ✅ Risk core (pre-trade checks)
- ✅ Reconciliation engine (crash cleanup)
- ✅ Scheduler, dependency injection, application bootstrap
- ✅ Intraday stop-loss/target monitoring (heartbeat + tick paths)
- ✅ Enforced daily loss limit + kill switch (with operator CLI)
- ✅ Live order-update (fill) integration, idempotent
- ✅ Connectivity-verified live startup flow + config-backed seams
- ✅ Concurrent instrument entry, bounded live-order timeouts, critical-event alerting
- ✅ 624 unit + integration tests

## What's not built yet (Low severity / operational)

- ❌ A real external alert channel (a drop-in `AlertDispatcher` — the mechanism exists)
- ❌ Interactive daily-login script (`generate_token.py`) — token set manually for now
- ❌ Instrument-master sync + validated expiry rules (config values need confirming)
- ❌ Process watchdog and unrealized-P&L loss limit (deliberate follow-ups)

See [`docs/modules.md`](docs/modules.md) and
[`docs/deployment.md`](docs/deployment.md) for the complete picture.

---

## Tech at a glance

Python 3.11+ · PostgreSQL · SQLAlchemy 2.0 (sync) · Pydantic v2 · Alembic ·
KiteConnect · pytest. Thread-based concurrency (no asyncio). Money is always
`Decimal`, never `float`.
=======
# tradingProject
>>>>>>> de04499030a0c71ae6466e0bc438f3d1d049f9b4
