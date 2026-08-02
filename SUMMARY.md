# algo_platform — Master Technical Summary

> **Purpose of this document set.** This is a self-contained knowledge base for the
> `algo_platform` project. If you (a human or another AI) have never seen this repo,
> reading these files should give you enough to **safely modify, debug, extend, and
> maintain** it without any other context.
>
> This file (`SUMMARY.md`) is the **index + master overview**. Deep detail lives in
> four companion files:
>
> | File | Contents |
> |---|---|
> | **[SUMMARY_ARCHITECTURE.md](./SUMMARY_ARCHITECTURE.md)** | Folder tree, per-file responsibilities, startup/shutdown/runtime flow, threading model, dependency & data/event flow |
> | **[SUMMARY_DATABASE.md](./SUMMARY_DATABASE.md)** | PostgreSQL + TimescaleDB schema, every table/column/constraint, ORM models, repositories, session/unit-of-work, migrations, optimistic locking |
> | **[SUMMARY_STRATEGY.md](./SUMMARY_STRATEGY.md)** | Strategy-1 lifecycle, position state machine, entry/option-selection/order/monitor/exit, risk, crash recovery, error handling |
> | **[SUMMARY_API.md](./SUMMARY_API.md)** | Broker abstraction, Kite auth/token/orders/mapper, the three websockets, the market-data collector, config files, logging |
>
> **Convention in these docs:** anything marked *(inference)* is my reasoned deduction,
> not an explicit statement in code. Everything else was read directly from the source.

---

## 1. Project Overview

- **Name:** `algo_platform` (package `algo`, under `src/algo/`).
- **Purpose:** a production-grade, real-money **algorithmic options-trading platform** for Indian markets (NSE/BSE), plus an independent **market-data collector**.
- **What it does today:** runs **Strategy-1**, a *non-directional ATM short straddle* — once per trading day per instrument, squared off intraday. At entry (~09:20 IST) it **sells** one ATM Call + one ATM Put, collects the combined premium, and exits on the first of {time cutoff, kill-switch, stop-loss, target}. Runs identically in **paper** (simulated fills) and **live** (real Kite orders) modes.
- **Problem it solves:** automates a premium-selling strategy with **broker-grade safety** — crash recovery, reconciliation against broker truth, exactly-once order semantics, and strict fail-fast/fail-loud behavior — so a bug or outage never silently produces a wrong money outcome.
- **Instruments (config-driven):** NIFTY, SENSEX (weekly expiry); BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEXBANK (monthly). Adding an instrument is a config change, no code.
- **Broker:** Zerodha **Kite** (live) via `kiteconnect`; a **SimulationBroker** and **PaperTradingBroker** back paper mode. The broker is an abstract seam (`BrokerBase`), so a future broker is a new adapter.

### High-level architecture (one paragraph)
Two **separate OS processes** with **separate databases**: (1) the **trading platform** (`start_paper`/`start_live`) — PostgreSQL, ORM + repositories, Alembic-managed; (2) the **market-data collector** (`start_collector`) — TimescaleDB, SQLAlchemy Core + `COPY`, `create_all`-managed. The trading process wires everything through a **composition root** (`DependencyContainer`), runs strategies via a generic `StrategyRunner`, fires time triggers via `PlatformScheduler`, and checks exits continuously via `MonitoringScheduler` + websocket ticks. Business logic depends only on **seams** (protocols/ABCs): `BrokerBase`, `InstrumentService`, `ExpiryService`, `SpotPriceProvider`, `TickStream`, `TradingCalendar`.

```
                 configs/*.yaml  +  .env (secrets)
                          │  validated at boot
                          ▼
  start_paper/live → DependencyContainer → Application → PlatformScheduler + MonitoringScheduler
                          │                                   │
                          ▼                                   ▼
                    StrategyRunner(one per instance) ──► Strategy1 (StrikeSelector/EntryLogic/Monitor/ExitLogic)
                          │                                   │
             Repositories → PostgreSQL           BrokerBase → Kite / Simulation / Paper
                                                        │
   start_collector → CollectorService → CollectorTickStream → TickWriter → TimescaleDB (option_ticks)
```

### Current development status
- **Working & tested:** the entire trading path (Strategy-1 entry→monitor→exit), risk/kill-switch, crash recovery, reconciliation, reporting (Excel + `trade_history`), the collector (with a self-healing websocket watchdog), holiday awareness, and a startup migration guard. **843 tests pass, 1 skipped** (an opt-in Postgres migration round-trip). No syntax errors; every non-test module imports cleanly.
- **Stubbed / not implemented (explicitly):** `backtesting/*`, `database/seed/*`, multi-account (`accounts/*` beyond single-account), `portfolio/*` aggregation, `common/decorators.py` retry decorator, `market_data/candle_builder.py`, `market_data/option_chain_builder.py`, `execution/fill_manager.py`. **None of these are imported by any live path** — they are inert placeholders. Also **not implemented:** half-day/Muhurat sessions, a live alert channel (only a recording sink), and `asyncio`/`ThreadPoolExecutor`/`multiprocessing`.

### Major design principles
1. **Config-driven, nothing hardcoded.** Instruments, strategy params, holidays, risk limits — all YAML; a bad value fails at **boot**, not at 09:20.
2. **Fail fast at boot, fail loud on ambiguity.** Migration guard, broker health check, and money-path integrity checks (`MissingFillPriceError`) refuse to proceed rather than guess.
3. **Database is the source of truth for state; the broker is the source of truth for execution.** Recovery reads the DB; reconciliation asks the broker.
4. **Exactly-once & crash-safe.** Durable `order_intents` before every broker call; optimistic locking; a non-blocking exit lock; idempotent recovery.
5. **Freeze, don't crash.** A strategy hook exception freezes *that instance*; the process and other instances keep running.
6. **Seams everywhere.** Protocols/ABCs isolate the strategy from any concrete broker/data source, enabling paper↔live parity and full unit-testing with fakes.
7. **Two isolated subsystems.** The collector can never affect trading (separate process, separate DB).

---

## 2. Folder structure → see **[SUMMARY_ARCHITECTURE.md](./SUMMARY_ARCHITECTURE.md) §2**
## 3. Architecture (startup/shutdown/runtime/flows) → **[SUMMARY_ARCHITECTURE.md](./SUMMARY_ARCHITECTURE.md) §3–5**
## 4–6. Components / Classes / Functions → **[SUMMARY_ARCHITECTURE.md](./SUMMARY_ARCHITECTURE.md) §4** and **[SUMMARY_STRATEGY.md](./SUMMARY_STRATEGY.md)**
## 7. Configuration → **[SUMMARY_API.md](./SUMMARY_API.md) §Config** (summary table below)
## 8. Database → **[SUMMARY_DATABASE.md](./SUMMARY_DATABASE.md)**
## 9. APIs (broker/websocket) → **[SUMMARY_API.md](./SUMMARY_API.md)**
## 10. Trading logic → **[SUMMARY_STRATEGY.md](./SUMMARY_STRATEGY.md)**
## 11. Error handling → **[SUMMARY_STRATEGY.md](./SUMMARY_STRATEGY.md) §Error handling** + summary below
## 12. Logging → below

---

## 7. Configuration (quick reference — full detail in SUMMARY_API.md)

All configs live in `configs/`. Secrets live in `.env` (never in YAML).

| File | Read by | Key values |
|---|---|---|
| `.env` | `database.py`, `generate_token.py`, token store | `DATABASE_URL`, `MARKET_DATA_DATABASE_URL`, `KITE_API_KEY/SECRET/ACCESS_TOKEN`, optional `CONFIG_DIR`, `DB_AUTO_MIGRATE`, `I_UNDERSTAND_THIS_TRADES_REAL_MONEY` |
| `configs/app.yaml` | `DependencyContainer` (`AppConfig`) | `instances:` (which `{strategy_id, instrument, account}` run), `report_output_dir` |
| `configs/brokers.yaml` | container | `active_broker` (KITE/SIMULATION); secrets referenced by **env-var name**, never literal |
| `configs/accounts.yaml` | container `_ensure_accounts` | account definitions |
| `configs/risk.yaml` | `RiskCore` | `margin_per_lot_by_instrument`, `daily_loss_limit_by_account`, `max_daily_entries_per_account` |
| `configs/database.yaml` | `database.py` | pool settings (size, timeouts, `statement_timeout`) |
| `configs/instruments/<name>.yaml` | `ConfigInstrumentService` (`InstrumentConfig`) | `exchange`, `strike_interval`, `lot_size`, `tick_size`, `spot_exchange`, `spot_symbol`, `expiry_weekday`, `expiry_cadence`, `underlying_symbol` |
| `configs/strategies/strategy_1/<name>.yaml` | `ParameterLoader` (`Strategy1Config`) | `entry_time`, `hard_cutoff_time`, `target_pct`, `sl_pct`, `lots`, `product_type`, `skip_on_expiry_day`, `monitoring_interval_seconds`, `polling_interval_seconds`, `retry{…}` |
| `configs/holidays.yaml` | `HolidayService` | `NSE:` / `BSE:` ISO date lists |
| `configs/collector.yaml` | `CollectorConfig` (collector process only) | `underlyings`, `strikes_each_side`, `tick_mode`, `recompute_interval_seconds`, `writer{batch_size,flush_interval_ms,queue_max,overflow_policy,use_copy,db_retry_*}`, `reconnect{grace_seconds,restart_backoff_seconds,heartbeat_*}`, `market_hours{…}`, `metrics{interval_seconds}`, `timescale{…}` |
| `configs/market_data.yaml` | trading market-data layer | trading-feed tuning |

**Cross-field validators:** `entry_time < hard_cutoff_time`; `polling_interval_seconds ≤ monitoring_interval_seconds`; `0 < target_pct/sl_pct < 1`; `heartbeat_critical ≥ heartbeat_warning`; lot/strike/tick `> 0`.

---

## 12. Logging (full detail in SUMMARY_API.md)

- Configured once by `src/algo/logging/logger.py::configure_logging(level, alert_dispatcher)` — a single timestamped stderr `StreamHandler`, format `"%(asctime)s %(levelname)-8s %(name)s: %(message)s"`, plus an `AlertingHandler` on the `algo` logger that forwards **CRITICAL+** to an `AlertDispatcher` (currently `RecordingAlertDispatcher` — a **recording sink, not a live pager**).
- Every module uses `logging.getLogger("algo.<area>")` (e.g. `algo.collector`, `algo.expiry`, `algo.migration_guard`, `algo.risk`).
- **Level conventions:** INFO = normal lifecycle; WARNING = degraded-but-handled; ERROR = handled failure with traceback; **CRITICAL = money/integrity/permanent-failure events** (partial entry, BROKER/DB DIVERGENCE, watchdog restart, schema out-of-date, naked exposure, `on_noreconnect`) — these are the alert stream; DEBUG = observability internals.
- **No file/rotation handler is configured in code** — logs go to stderr; capture with your process supervisor (systemd/tmux/redirect).
- **Debugging workflow:** grep CRITICAL first → check the position's `state` and `position_state_transitions` → trace `positions → trades → orders → order_intents` → check `reconciliation_breaks`.

---

## 13. Deployment

- **Local setup:**
  1. Python 3.11+, create venv, `pip install -e .[dev]` (deps in `pyproject.toml`).
  2. Provision **two** databases: trading (Postgres, `DATABASE_URL`) and market-data (Postgres+TimescaleDB, `MARKET_DATA_DATABASE_URL`).
  3. Put secrets in `.env`. Run `alembic upgrade head` (trading DB). The collector auto-creates its schema on start.
  4. Daily: `python scripts/generate_token.py` (mint the Kite access token → `.env`).
  5. Run from the **`algo_platform/` root** (configs are resolved relative to CWD, or `CONFIG_DIR`).
- **Run commands:** `python start_paper.py` (paper), `python start_live.py` (requires `I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes`), `python -m algo.start_collector` (collector).
- **Production:** **This project currently does not implement Docker, systemd units, or a tmux/supervisor config** — there are no Dockerfiles or compose files. *(Inference)* the intended deployment is: run each entrypoint under a process supervisor (tmux/systemd/nohup), redirect stderr to a log file, and rely on the built-in crash-recovery/reconciliation on restart. The startup migration guard makes a stale-schema deploy fail fast.
- **Startup order matters (per process):** trading process authenticates the broker and runs reconciliation before arming any strategy; the migration guard runs before the container is even built.

---

## 14. Dependencies (from `pyproject.toml`, all pinned)

| Package | Why | Where used | Alternatives |
|---|---|---|---|
| `pydantic==2.11.7`, `pydantic-settings` | config validation at boot | every `*Config` | dataclasses+manual validation |
| `PyYAML` | read YAML configs | config loaders | tomli/json |
| `python-dotenv` | load `.env` | `database.py`, token store | os.environ only |
| `SQLAlchemy==2.0.41` | ORM + Core, engine/pool, optimistic locking | `database/*`, collector `db.py` | raw psycopg2 |
| `alembic` | schema migrations | `database/migrations/*`, `migration_guard` | manual SQL |
| `psycopg2-binary` | Postgres driver (**client-side param binding** — relied on by collector DDL) | engine | psycopg3 (would break `INTERVAL :ci` — see decisions) |
| `pandas`, `numpy` | present (collector/analytics helpers) | limited | — |
| `requests` | transitive/broker HTTP | broker | — |
| `websockets` | present | — | — |
| `kiteconnect==5.0.1` | Kite REST + `KiteTicker` websocket | `brokers/kite/*`, collector `full_tick_stream` | broker-specific |
| `pyotp` | present (TOTP capability) | not used for auto-login (login is manual) | — |
| `tenacity` | present but **not used in hot paths** (retries are hand-rolled) | — | — |
| `python-dateutil`, `pytz` | IST timezone, date math | time service, reports | zoneinfo |
| `openpyxl` | write Excel reports | `reporting/trade_report.py` | csv |
| `pytest`, `pytest-cov` (dev) | tests | `tests/` | — |

**Note:** `kiteconnect` pulls in **Twisted** (the `KiteTicker` websocket runs on a Twisted reactor thread).

---

## 15. Development Workflow

- **Run tests:** `pytest src/algo/tests -q` (SQLite in-memory + fakes; no network, no real broker, no Timescale needed). ~68s, 843 passing.
- **Run one area:** `pytest src/algo/tests/strategy_engine/strategies/strategy_1 -q`.
- **Opt-in Postgres migration test:** set `MIGRATION_TEST_DATABASE_URL` to a disposable Postgres and run `pytest src/algo/tests/database/test_migration_guard.py`.
- **Debug:** grep CRITICAL logs; use the DB tables as the audit trail (`position_state_transitions` tells you who changed state and why).
- **Add a new instrument (config only, no code, no migration):**
  1. `configs/instruments/<name>.yaml` (exchange, strike_interval, lot_size, tick_size, spot_exchange, spot_symbol, expiry_weekday, expiry_cadence).
  2. `configs/strategies/strategy_1/<name>.yaml` (params).
  3. Add to `configs/app.yaml::instances`.
  4. Add `margin_per_lot_by_instrument[<NAME>]` to `configs/risk.yaml`.
  5. (Collector) add to `configs/collector.yaml::underlyings`. (Holidays already shared.)
- **Add a new strategy (medium code change):**
  1. New package `src/algo/strategy_engine/strategies/strategy_2/` mirroring `strategy_1/` (a `strategy.py` with `@register_strategy("strategy_2")` and `config_schema()`, plus its own config/logic).
  2. `configs/strategies/strategy_2/<instrument>.yaml`.
  3. **Add one import line** in `src/algo/dependency_container.py` (near the existing `import algo.strategy_engine.strategies.strategy_1.strategy` on ~line 57) so the `@register_strategy` decorator runs.
  4. Add `instances` with `strategy_id: strategy_2` to `app.yaml`. Switching which strategy runs is then a config edit.
  5. **Migration** only if the new strategy persists new per-position fields or a different leg structure (the `Position` model is straddle-shaped).
- **Add a new broker:** implement `BrokerBase` in `src/algo/brokers/<name>/` (mirror `kite/`: broker, mapper, auth, websocket), wire in `brokers.yaml` + `build_seams()`. Strategy code is unchanged.

---

## 16. Common Problems (troubleshooting)

| Symptom | Cause | Fix |
|---|---|---|
| `Database schema is not up to date … Run: alembic upgrade head` (exit 1) | DB behind Alembic head | `alembic upgrade head`, then restart |
| `psycopg2.errors.UndefinedColumn` mid-trade | (pre-guard) schema drift | Now prevented by the migration guard; run `alembic upgrade head` |
| `RuntimeError: DATABASE_URL is not set` | missing env | set `DATABASE_URL` in `.env` |
| `refusing to start live trading …` | live confirmation missing | `export I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes` |
| `BrokerAuthenticationError` / profile() fails at boot | expired/absent Kite token | `python scripts/generate_token.py` |
| Instance shows FROZEN, stops trading | a strategy hook raised → fault isolation | inspect the CRITICAL that preceded it + the position `state`; clear `strategy_instances.status` to ACTIVE after resolving |
| Entry skipped silently | already-entered / expiry-day / risk block | check `EntryResult` outcome in logs (`SKIPPED_*`, `BLOCKED_BY_RISK`) |
| Orders rejected on a weekday | it's an **exchange holiday** not in `holidays.yaml` (system treats unknown dates as trading days) | add the date to `configs/holidays.yaml`, restart |
| Collector: `Connected: NO`, 0 ticks | websocket down; watchdog should rebuild | check watchdog CRITICALs; verify Kite token; the watchdog builds a fresh `KiteTicker` after `grace_seconds` |
| Collector writes nothing but `Connected: YES` | frozen reactor | heartbeat CRITICAL → watchdog restart; check `dropped_ticks` |
| `HolidayService … FileNotFoundError` at boot | `configs/holidays.yaml` deleted | restore the file (production requires it) |
| Config validation error at boot | bad YAML value | the pydantic error names the exact field |

---

## 17. Important Decisions (do NOT change without understanding)

1. **Two databases, two processes.** The collector must never share state or a DB with trading. Merging them would let a data-pipeline incident touch trading data.
2. **Repositories flush, never commit.** The **caller** owns the transaction boundary (`unit_of_work`), so multiple repo calls become one atomic commit (Position + intents together). Committing inside a repo would break crash-safety.
3. **`expire_on_commit=False`** on the session factory — code reads object attributes after the session closed; changing this reintroduces `DetachedInstanceError`.
4. **Optimistic locking (`version`) + non-blocking `ExitLogic._exit_lock` + runner RLock** = the three-layer "exactly-one-exit" guarantee. Removing any layer reopens double-exit / naked-leg races.
5. **`order_intents` written before every broker call.** This is what makes "sent an order, crashed before the ack" recoverable. Never place an order without a durable intent.
6. **Entry never blind-retries a placement; a timeout on a mutating call is AMBIGUOUS.** Retrying could double-fire. See `brokers/exceptions.py::BrokerTimeoutError`.
7. **Money-path fail-loud:** a COMPLETE order with no fill price raises `MissingFillPriceError` — never record a 0. (Fixed this session; do not re-introduce `or Decimal("0")` on execution prices.)
8. **psycopg2 client-side parameter binding** is relied on by the collector's `init_timescale.py` (`INTERVAL :ci` becomes `INTERVAL '1 day'`). Switching to psycopg3/asyncpg (server-side binding) would break the Timescale DDL.
9. **The migration guard** aborts boot if the DB ≠ Alembic head. This is deliberate; auto-migrate is opt-in via `DB_AUTO_MIGRATE`, off by default.
10. **Holidays are a static YAML list** with no auto-calculation. It **fails open** (unknown years treated as trading days) — the yearly update is a hard maintenance requirement.
11. **The state machine is the single gate for legal transitions**; `ERROR→CLOSED` is restricted to `MANUAL`/`RECOVERY` actors. Don't bypass it.

---

## 18. Future Improvements / Known Technical Debt

- 🔴 **Security (highest priority, not a code bug):** `.env` currently holds **real secrets in plaintext with no `.gitignore`.** Rotate the Kite API secret/access token and DB password; add `.gitignore` (`.env`, `reports/`, `*.xlsx`) before this touches git.
- **Half-day / Muhurat sessions** are not modeled (only full holidays + weekends). Would need a date→session-hours concept.
- **Live alerting** is a recording sink only — no webhook/Slack/pager.
- **Retry decorator** (`common/decorators.py`) is a TODO; retries are hand-rolled (works fine, just not centralized).
- **Backtesting, multi-account, portfolio aggregation** are stubs.
- **The `Position` model is straddle-shaped** (single `strike`, CE/PE legs) — a 4-leg (iron condor) or two-expiry (calendar) strategy would need schema flexibility.
- **Daily-loss breach blocks new entries but does not force-close open positions** — confirm this is the intended policy.
- **No Docker/systemd** deployment artifacts.

---

## 19. AI CONTEXT — read this before modifying any file

**If you are an AI about to edit this repo, internalize these first:**

- **Philosophy:** this trades real money. Prefer **refusing/failing loudly** over guessing. Never let observability/reporting break a trade. Never introduce a silent default in a money path.
- **Seams, not concretes.** Strategy/risk/service code depends on **protocols/ABCs** (`BrokerBase`, `InstrumentService`, `ExpiryService`, `SpotPriceProvider`, `TickStream`, `TradingCalendar`). Add behavior behind a seam; don't import a concrete broker into strategy code.
- **Transaction ownership:** repositories call `flush()` **never `commit()`**; the caller wraps work in `with unit_of_work(session_factory) as session:`. A `Session` is **not thread-safe** — one per unit of work; never share across threads.
- **State changes go through `PositionStateMachine.transition`** (validates the legal graph, writes an audit row, bumps `version`). Never write `position.state = …` directly.
- **Before any broker order:** there must be a durable `OrderIntent` (`SUBMITTED_UNCONFIRMED`) so recovery can reconstruct. Entry places CE then PE; PE only after CE fills; auto-unwind CE if PE fails.
- **Exit is guarded by `_exit_lock.acquire(blocking=False)`** (skip if held) + optimistic locking. Don't remove either.
- **Config is validated at boot** (pydantic, `frozen=True`). New knobs go in the relevant `*Config` with bounds; **no Python-level defaults for values that should come from YAML** (e.g. `skip_on_expiry_day`).
- **Enums** are stored as strings (`Enum(native_enum=False)` + CHECK) so adding a member is metadata-only. Member name == value.
- **Naming conventions:** `snake_case` functions/vars, `PascalCase` classes, `_leading_underscore` for internals/private methods, `configs/` YAML lowercase filenames, instrument identity **UPPERCASE** (`"NIFTY"`) — this exact casing is a lookup key in `risk.yaml`/`app.yaml`; do not change it.
- **Invariants / hidden assumptions:**
  - One `Position` per `(strategy_instance_id, trade_date)` (unique constraint) — one straddle per instrument per day; Strategy-1 is **intraday** (no overnight positions).
  - `quantity = lots * lot_size`; option fill prices are always `> 0`.
  - Instrument identity is UPPERCASE; the expiry service maps `NFO→NSE`, `BFO→BSE` for holidays.
  - The collector's `option_ticks`/`collector_instruments` are **SQLAlchemy Core Tables, not ORM models**; written via `COPY`.
  - `psycopg2` client-side binding is assumed by the collector DDL.
- **Safety rules:** don't blind-retry mutating broker calls; don't force-close on a daily-loss breach (policy); don't run with `python -O` (there are `assert` invariants in non-test code); the socket thread must **never** touch the DB (collector enqueues only).
- **Performance:** the collector's socket thread only enqueues (O(1)); the writer thread batches via `COPY`. Metrics counters are intentionally unlocked (approximate). The trading exit decision (`evaluate_exit`) is a tiny pure function on the hot path.
- **Security:** secrets only in `.env`, referenced by env-var **name** in YAML; the token flow never stores a password/TOTP; broker product codes stay abstract (`INTRADAY/NORMAL`) — the mapper translates.
- **Testing:** the suite uses SQLite + fakes and `Base.metadata.create_all()` (which **bypasses Alembic**). If you change an ORM model, **write an Alembic migration too** — the migration guard will refuse to boot otherwise, and the opt-in Postgres test exists to catch model/migration drift.

---

## 20. File Index → see **[SUMMARY_ARCHITECTURE.md](./SUMMARY_ARCHITECTURE.md) §File Index** for the full table.

Top-level orientation:

| Path | Purpose |
|---|---|
| `src/algo/start_paper.py` / `start_live.py` / `start_collector.py` | process entrypoints |
| `src/algo/app.py` | `Application` — OS-process lifecycle (signals, block, shutdown) |
| `src/algo/dependency_container.py` | composition root (DI); wires everything |
| `src/algo/strategy_engine/strategies/strategy_1/` | **Strategy-1** (the whole strategy) |
| `src/algo/database/` | engine, session, models, repositories, migrations, migration guard |
| `src/algo/brokers/` | `BrokerBase` + Kite/Simulation/Paper adapters |
| `src/algo/market_data_collector/` | the collector subsystem |
| `src/algo/services/` | seams: instruments, expiry, holidays, reconciliation, recorder |
| `src/algo/risk/` | risk core, kill switch, daily-loss latch |
| `src/algo/reporting/trade_report.py` | Excel report |
| `configs/` | all YAML config | 

---

## 21. Glossary

- **Straddle (short):** selling an ATM Call + ATM Put together; profits if the underlying stays near the strike (premium decays).
- **ATM:** at-the-money — the strike nearest spot (`round(spot / strike_interval)`).
- **Combined premium:** CE price + PE price; Strategy-1's target/stop are levels on this single number.
- **Target / Stop-loss:** `target = entry × (1 − target_pct)` (premium fell = profit); `stoploss = entry × (1 + sl_pct)` (premium rose = loss).
- **0DTE:** zero days to expiry (entering on the expiry day); `skip_on_expiry_day` can skip it.
- **Weekly / Monthly cadence:** how an instrument's options expire (config `expiry_cadence`).
- **Position (state machine):** `IDLE → ENTRY_PENDING → OPEN → EXIT_PENDING → CLOSED`; `ERROR` from any non-terminal.
- **InstanceStatus:** `ACTIVE / FROZEN / DISABLED` — the persistent strategy instance's operational status.
- **RunnerStatus:** `RUNNING / FROZEN / STOPPED` — the in-memory `StrategyRunner`'s status.
- **Order intent:** a durable "about to place this order" row (`order_intents`) enabling crash recovery.
- **Reconciliation:** repairing in-doubt DB state against broker truth on startup.
- **Optimistic locking:** a `version` column; a concurrent writer that loses raises `ConcurrentModificationError`.
- **Kill switch / emergency exit / freeze:** risk control flags (`risk_control_flags`) that make `is_halted` true.
- **Seam:** a protocol/ABC boundary (`BrokerBase`, `TickStream`, …) enabling substitution and testing.
- **Collector:** the separate market-data process writing FULL ticks to TimescaleDB.
- **Watchdog:** the collector controller-loop mechanism that rebuilds a dead/frozen websocket.
- **NFO/BFO/NSE/BSE:** F&O segments (NFO=NSE derivatives, BFO=BSE derivatives) and cash segments.
- **IST:** India Standard Time (all market times are IST).

---

## 22. Change Log Summary (milestones)

*(Inference where dates aren't in code; based on migration chain, docstrings, and this session's work.)*

1. **Initial platform build** — DB schema (`3634d974da39`), broker abstraction, Strategy-1 engine, schedulers, risk, reconciliation, paper/live entrypoints.
2. **Trade-history analytics** — `trade_history` table + recorder (`c45ec4abd494`).
3. **Excel reporting** — per-day `.xlsx` exporter.
4. **Options market-data collector** — separate TimescaleDB subsystem (FULL ticks, ATM window, batch COPY writer, market-hours automation).
5. **[This session] Collector self-healing watchdog** — reconnect grace/backoff, `on_noreconnect` handling, brand-new-`KiteTicker` rebuild, heartbeat, honest metrics (`Subscribed: 0 (Disconnected)`), config `reconnect{…}`.
6. **[This session] `pnl_per_share` column** — additive analytics on Excel + `trade_history` (migration `a1f2c3d4e5f6`).
7. **[This session] Applied the `pnl_per_share` migration** to the real DB (it was created but unapplied → `UndefinedColumn` incident).
8. **[This session] Money-path fail-loud fix** — replaced silent `or Decimal("0")` on entry/exit fill prices with `MissingFillPriceError` (entry raises → freeze; exit returns AMBIGUOUS / finalize raises → reconciliation).
9. **[This session] Exchange holiday awareness** — real `HolidayService` + `HolidayAwareTradingCalendar`, `configs/holidays.yaml`, exchange-aware expiry shift; wired into scheduler/collector/expiry.
10. **[This session] Startup migration guard** — `database/migration_guard.py`; `start_paper`/`start_live` abort if the DB schema ≠ Alembic head (strict by default, opt-in auto-migrate).

**Current state:** 843 tests passing, 1 skipped; all modules compile and import; no known functional bugs. Outstanding non-code item: rotate `.env` secrets + add `.gitignore`.
