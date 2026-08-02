# SUMMARY_DATABASE — Persistence Architecture

> Companion to [SUMMARY.md](./SUMMARY.md). Covers both databases, every table,
> the ORM models, the repository pattern, sessions/transactions, optimistic
> locking, and migrations.

---

## 1. Two databases

| | **Trading DB** | **Collector DB** |
|---|---|---|
| Env var | `DATABASE_URL` | `MARKET_DATA_DATABASE_URL` |
| Tech | PostgreSQL | PostgreSQL + **TimescaleDB** extension |
| Access | SQLAlchemy **ORM** + repositories | SQLAlchemy **Core** `Table` + `COPY` |
| Schema mgmt | **Alembic** migrations (guarded at boot) | `init_timescale.initialize_schema` (`create_all` + Timescale DDL) — idempotent every start |
| Process | trading (`start_paper`/`start_live`) | collector (`start_collector`) |
| Why separate | a data-pipeline incident must never touch trading data; different tuning/retention | — |

- **Why PostgreSQL:** ACID, real constraints (`UNIQUE`/`CHECK`/FK), `SELECT … FOR UPDATE`, SAVEPOINTs — all used by the crash-safety design.
- **Why TimescaleDB (collector only):** hypertable + compression + continuous aggregates for high-volume time-series ticks. It is a Postgres extension.
- **Why ORM for trading, Core for collector:** trading rows are rich objects with state machines (ORM fits); ticks are flat high-volume rows best served by `COPY` (Core fits).

---

## 2. Engine, Session, Transaction (mechanics)

**`src/algo/database/database.py`**
- `load_database_settings()` → reads `DATABASE_URL` (env/.env; **raises `RuntimeError` if unset**) + `configs/database.yaml` → `DatabaseSettings(url, pool=DatabasePoolSettings)`.
- `build_engine(settings)` → `create_engine(url, poolclass=QueuePool, pool_size, max_overflow, pool_timeout, pool_recycle, pool_pre_ping=True, echo, connect_args={connect_timeout, application_name, options='-c statement_timeout=…'})`. **Pure function** (tests build their own engine).
- `get_engine()` → process-wide singleton via **double-checked locking** (`_engine_lock`).

**`src/algo/database/session.py`**
- `build_session_factory(engine)` → `sessionmaker(bind=engine, autoflush=True, expire_on_commit=False)`.
  - **`expire_on_commit=False`** is deliberate: callers read attributes (e.g. `Position.id`) after the `with` block closed the Session; the default would raise `DetachedInstanceError`.
- `get_session_factory()` → singleton (double-checked, `_session_factory_lock`).
- **`unit_of_work(session_factory)`** (`@contextmanager`): `session = factory(); try: yield; commit(); except: rollback; raise; finally: close`. **The single transaction pattern.** DI'd factories (strategy/risk/services) delegate here via a thin `_unit_of_work`.
- `session_scope()` — same, using the process-wide singleton.
- **Deliberately NOT using `scoped_session`/`threading.local`** — repositories take an explicit `Session` via constructor; a thread-local "current session" would be a second inconsistent path. A `Session` is **not thread-safe** → one per unit of work, never shared across threads.

**`src/algo/database/repositories/base.py::BaseRepository[ModelT]`** — the shared machinery:
- `get_by_id(id)` / `get_by_id_or_raise(id)` (`NotFoundError`).
- **`get_by_id_for_update(id)`** → `select(...).where(id==...).with_for_update()` — a **row lock** (used before a state transition, since the kill switch can touch the same row).
- `add(obj)` → add + flush (assigns PK).
- **`flush()`** → `session.flush()`, translating SQLAlchemy `StaleDataError` (optimistic-lock version mismatch) → **`ConcurrentModificationError`**.
- **`_get_or_create(lookup, factory)`** → look up; if absent, insert inside a **`begin_nested()` SAVEPOINT**; on `IntegrityError` re-query. *Why SAVEPOINT:* Postgres aborts the entire transaction on any statement error, so a duplicate-insert race must be contained in a nested transaction.
- **Contract:** every method **flushes, never commits** — the caller owns the transaction boundary, so several repo calls in one `unit_of_work` become **one atomic commit**.

### Transaction flow (strategy enters)
```
with unit_of_work(session_factory) as session:              # commit/rollback/close
    pos,created = PositionRepository(session).get_or_create(ENTRY_PENDING,…)   # SAVEPOINT
    TradeRepo/OrderIntentRepo(session).create(...)          # flush → PKs
# exit → session.commit()  → one atomic SQL transaction → PostgreSQL
Python objects → ORM add/flush → Session → INSERT/UPDATE → COMMIT (or ROLLBACK on error)
UPDATE bumps `version`; concurrent loser → StaleDataError → ConcurrentModificationError
```

---

## 3. Trading DB schema (tables)

Models in `src/algo/database/models/`. `base.py` defines `Base` + `TimestampMixin` (`created_at`, `updated_at`). `enum_column.py::enum_column(EnumType)` = `Enum(..., native_enum=False, create_constraint=True)` (stored as string + CHECK; adding a member is metadata-only).

### `strategy_instances` (`strategy_instance.py::StrategyInstance`)
- **Purpose:** persistent identity of one running `(strategy_id, instrument, account)`.
- **Columns:** `id`, `strategy_id`, `instrument`, `account_id`(FK), `exchange`, `status`(`InstanceStatus`: ACTIVE/FROZEN/DISABLED), timestamps.
- **Written by:** `InstanceFactory` (create), `StrategyRunner._freeze_instance` (freeze). **Read by:** factory, recovery, reporting.

### `positions` (`position.py::Position`) — the central row
- **Purpose:** one daily straddle cycle.
- **Columns (key):** `id`, `strategy_instance_id`(FK), `trade_date`, `state`(`PositionState`), `strike`, `strike_interval`, `expiry_date`, `lots`, `lot_size`, `quantity`, `entry_spot_ltp`, `combined_entry_premium`, `combined_exit_premium`, `target_premium`, `stoploss_premium`, `target_pct`, `sl_pct`, `realized_pnl`, `exit_reason`(`ExitReason`), `entry_signal_time`, `entry_completed_at`, `exit_signal_time`, `exit_completed_at`, **`version`** (optimistic lock), timestamps.
- **Constraints:** `UNIQUE(strategy_instance_id, trade_date)` (one position/day), `CHECK lot_size>0`, `CHECK quantity = lots*lot_size`, FK→strategy_instances.
- **`__mapper_args__ = {"version_id_col": version}`** → SQLAlchemy optimistic locking.
- **Written by:** entry (`get_or_create` ENTRY_PENDING; `_finalize_open` sets premium/thresholds→OPEN), exit (EXIT_PENDING, realized_pnl→CLOSED), recovery/reconciliation/kill-switch. **Read by:** monitor, exit, reporting, risk, recovery.

### `trades` (`trade.py::Trade`) — per leg
- **Purpose:** one option leg (CE or PE) of a position.
- **Columns:** `id`, `position_id`(FK), `option_type`(CE/PE), `exchange`, `trading_symbol`, `strike`, `quantity`, `entry_price`, `exit_price`, `entry_time`, `exit_time`, `realized_pnl`, `status`(`TradeLegStatus`: PENDING/OPEN/CLOSED/UNWOUND/ERROR).
- **Written by:** entry (create+fill), exit (exit price/pnl/CLOSED), auto-unwind (UNWOUND). **Read by:** exit finalize, recorder, report.

### `orders` (`order.py::Order`)
- **Purpose:** one broker order (entry or exit leg placement).
- **Columns:** `id`, `trade_id`(FK), `intent_id`(FK), `purpose`(ENTRY/EXIT), `transaction_type`(BUY/SELL), `order_type`(MARKET), `quantity`, `broker_order_id`, `status`(`OrderStatus`), `average_price`, `broker_tag`, `retry_count`, **`version`**, timestamps.
- **Written by:** entry/exit (create+ack+reconcile), `OrderUpdateProcessor` (push updates). **Read by:** fill confirmation, reconciliation.

### `order_intents` (`order_intent.py::OrderIntent`)
- **Purpose:** durable "about to place this order" record — the crux of crash recovery.
- **Columns:** `id`, `trade_id`(FK), `status`(`IntentStatus`: PENDING/SUBMITTED_UNCONFIRMED/PLACED/CONFIRMED/FAILED/CANCELLED), `broker_tag`(unique idempotency key), timestamps.
- **Written by:** entry/exit **before every broker call** (`mark_broker_call_started`→SUBMITTED_UNCONFIRMED, `mark_placed`, `mark_failed`). **Read by:** recovery, reconciliation.

### `position_state_transitions` (`position_state_transition.py`)
- **Purpose:** audit of every state change.
- **Columns:** `id`, `position_id`(FK), `from_state`, `to_state`, `actor`(`StateTransitionActor`: STRATEGY/RISK_MANAGER/KILL_SWITCH/RECOVERY/MANUAL), `reason`, `created_at`.
- **Written by:** `PositionRepository.transition_state` (via `PositionStateMachine.transition`). **Read by:** audit, `_infer_pending_exit_reason`.

### `trade_history` (`trade_history.py::TradeHistory`)
- **Purpose:** one denormalized analytics row per closed trade (append-only).
- **Columns (selection):** `id`, `position_id`(FK, **UNIQUE** → idempotent), `strategy_instance_id`, `account_id`, `trade_date`, `strategy_id`, `instrument`, `account_name`, `mode`(PAPER/LIVE), `exchange`, `strike`, `expiry_date`, `call_symbol`, `put_symbol`, `entry_time`, `exit_time`, `entry_signal_time`, `exit_signal_time`, `holding_seconds`, `holding_minutes`, `lots`, `lot_size`, `quantity`, `combined_entry_premium`, `combined_exit_premium`, `target_premium`, `stoploss_premium`, `target_pct`, `sl_pct`, `call/put_entry/exit_price`, `entry_spot_ltp`, `exit_spot_ltp`(nullable, not yet persisted), `exit_reason`, `realized_pnl`, **`pnl_per_share`** (=`realized_pnl/lot_size`, added this session), `profit_percent`, `max_profit_seen`/`max_loss_seen`(nullable), `day_of_week`, `month`, `broker_order_ids`(JSONB), `extra`(JSONB).
- **Indexes:** `trade_date`, `instrument`, `strategy_id`, `exit_reason`, `expiry_date`, composite `(strategy_id, instrument, trade_date)`.
- **Written by:** `TradeHistoryRecorder` after CLOSED. **Read by:** analytics/backtesting (external).

### `accounts`, `daily_risk_state`, `risk_control_flags`, `reconciliation_breaks`
- `accounts` — broker account identity (`broker`, `display_name`, …).
- `daily_risk_state` — per-account daily realized/unrealized P&L + `loss_limit` snapshot + `breached` latch.
- `risk_control_flags` — kill-switch/emergency-exit/freeze flags with scope (GLOBAL/ACCOUNT/STRATEGY_INSTANCE), `active`, `activated_by`, `activated_at`.
- `reconciliation_breaks` — detected DB↔broker discrepancies (`break_type`, `subject`, `resolution`).

### Collector DB tables (`market_data_collector/db.py`, SQLAlchemy Core `Table`)
- **`option_ticks`** (hypertable) — `time`, `instrument_token`, `last_price`, `last_traded_qty`, `avg_traded_price`, `volume`, `oi`, `oi_day_high`, `oi_day_low`, `ohlc_open/high/low/close`, `total_buy_qty`, `total_sell_qty`, `depth`(JSONB). Index `(instrument_token, time DESC)`; compression `segmentby=instrument_token, orderby=time DESC`; continuous aggregates (1m/5m). `TICK_COLUMNS` tuple + `MarketTick` dataclass (`as_row()`).
- **`collector_instruments`** (dimension) — `instrument_token`(PK), `underlying`, `exchange`, `expiry_date`, `strike`, `option_type`, `tradingsymbol`, `first_seen`.

---

## 4. Repositories

All in `src/algo/database/repositories/` (constructed with a `Session`):

| Repository | Tables | Notable methods |
|---|---|---|
| `PositionRepository` | positions, trades, position_state_transitions | `get_or_create`, `get_by_instance_and_date`, `get_by_id_for_update`, `transition_state`, `list_trades_for_position`, `realized_pnl_for_account_on_date`, `list_non_terminal` |
| `OrderRepository` | orders | `create`, `mark_acknowledged`, `get_by_id_or_raise`, `list_for_trade`, `increment_retry_count` |
| `OrderIntentRepository` | order_intents | `create`, `mark_broker_call_started`, `mark_placed`, `mark_failed`, `get_by_id_or_raise` |
| `TradeHistoryRepository` | trade_history | `get_by_position_id`, `add_if_absent` (idempotent) |
| `StrategyInstanceRepository` | strategy_instances | `get_or_create`, `get_by_id_or_raise`, `set_status` |
| `AccountRepository` | accounts | `get_or_create`, `get_by_id` |
| `DailyRiskStateRepository` | daily_risk_state | `get_portfolio_row`, `get_or_create_portfolio_row`, `update_pnl`, `mark_breached` |
| `ReconciliationBreakRepository` | reconciliation_breaks | `record`, `list_for_position` |
| `RiskControlFlagRepository` | risk_control_flags | flag CRUD |

`repositories/exceptions.py` → `ConcurrentModificationError`, `NotFoundError`.

---

## 5. Migrations

- **Alembic** at `alembic.ini` (`script_location = src/algo/database/migrations`, `prepend_sys_path = src`, `sqlalchemy.url` blank — resolved by `env.py` from `DATABASE_URL` or `-x db_url=`).
- **`env.py`** — `target_metadata = Base.metadata`; online mode builds an engine from `DATABASE_URL`. (It only runs *inside* Alembic; importing it standalone fails — that's normal.)
- **Chain (`versions/`):** `3634d974da39` (initial) → `c45ec4abd494` (add trade_history) → **`a1f2c3d4e5f6` (add pnl_per_share, HEAD)**.
- **Workflow:** change a model → `alembic revision --autogenerate -m "..."` → review → `alembic upgrade head`.
- **Startup guard (`migration_guard.py`):** `guard_database_schema()` compares the DB's `alembic_version` (via `MigrationContext.get_current_heads`) to the code head (`ScriptDirectory.get_heads`). **Behind → CRITICAL + `SchemaOutOfDateError` → main returns 1.** Missing `alembic_version` (fresh DB) → also fails. Auto-migrate is opt-in via `DB_AUTO_MIGRATE=true` (runs `alembic upgrade head` then re-verifies) — **off by default**.
- **CI drift note:** the test suite builds schema with `Base.metadata.create_all()`, which **bypasses Alembic** — so a model change without a migration is invisible to unit tests. The opt-in Postgres round-trip test (`tests/database/test_migration_guard.py`, `MIGRATION_TEST_DATABASE_URL`) closes that gap by running `alembic upgrade head` and asserting the ORM round-trips.
- **Collector DB is NOT Alembic-managed** — `init_timescale` `create_all`s every start, so it cannot drift.

### Typical queries (examples of intent, not literal SQL)
- "today's position for this instance": `PositionRepository.get_by_instance_and_date(instance_id, today)`.
- "lock a position before transition": `get_by_id_for_update(position_id)`.
- "account's realized P&L today": `realized_pnl_for_account_on_date(account_id, today)`.
- "record analytics once": `TradeHistoryRepository.add_if_absent(row)` (UNIQUE position_id).

---

## 6. SQLAlchemy concepts used (where)
- Engine/QueuePool/`pool_pre_ping` — `database.py`.
- Session/`expire_on_commit=False`/`autoflush` — `session.py`.
- `mapped_column`, `Mapped[...]` — `models/*`.
- Optimistic locking `version_id_col` — `positions`, `orders`.
- `select().with_for_update()` — `base.py::get_by_id_for_update`.
- `flush()` vs `commit()` — repos flush; `unit_of_work` commits.
- `begin_nested()` SAVEPOINT — `_get_or_create`.
- `Enum(native_enum=False)` — `enum_column.py`.
- **Lazy loading avoided post-commit** — reporting/exit extract values inside the open session (e.g. `_LegRow`) to avoid `DetachedInstanceError`.
- **Core (not ORM)** — collector `option_ticks`/`collector_instruments` via `Table` + `COPY`.
