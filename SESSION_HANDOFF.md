# Session Handoff Document — algo_platform / Strategy-1

**Scope of this session:** project skeleton, full database architecture (schema, crash-recovery design, SQLAlchemy models, repository layer, Alembic migration, engine/session infrastructure), and a critical review of the whole database layer. No strategy logic, broker integration, or risk engine code has been written yet.

**Status:** Database layer approved through Task 7 (review). Nothing beyond `database/` and the project skeleton has been implemented.

---

## 1. Architecture decisions

These are the load-bearing calls made this session, in the order they were locked in. Each was either explicitly confirmed by the user or made as a clearly-flagged default where confirmation was never obtained (see §4 Assumptions for which is which).

| # | Decision | Where decided |
|---|---|---|
| 1 | Postgres as the target database from day one (not SQLite) | Confirmed via AskUserQuestion |
| 2 | `Trade` normalized as one row per leg (CE/PE), not flat columns on `Position` | Confirmed via AskUserQuestion |
| 3 | Integer autoincrement primary keys (not UUID) | Confirmed via AskUserQuestion |
| 4 | `StrategyInstance` is a persistent singleton per `(strategy_id, instrument, account_id)`, created once, reused forever | Confirmed via AskUserQuestion |
| 5 | Money fields as `Numeric`/`Decimal`, never `Float` | Stated assumption, unchallenged |
| 6 | Timestamps stored timezone-aware in UTC; `trade_date` as a plain `Date` (IST calendar date) | Stated assumption, unchallenged |
| 7 | Enums stored as validated strings (`native_enum=False` + CHECK constraint), not native Postgres `ENUM` types — avoids `ALTER TYPE` migration pain | Stated design decision |
| 8 | Sync SQLAlchemy 2.0, not async — websocket runs in its own thread, DB access is sync | Stated assumption, unchallenged |
| 9 | Config values (`lots`, `lot_size`, `target_pct`, `sl_pct`, `strike_interval`) snapshotted onto `Position` at entry time, never re-derived from current YAML later | Stated design decision |
| 10 | `order_intents` table, written *before* any broker call, is the core crash-recovery mechanism (closes the duplicate-entry gap identified in the engineering review) | Task 2 architecture review |
| 11 | Idempotency enforced via DB unique constraints + insert-and-catch, never read-then-write (closes a TOCTOU race) | Task 2 architecture review |
| 12 | Partial-entry failure (one leg fills, other rejects) resolved via **auto-unwind** (buy back the filled leg immediately, then ERROR) | My recommended default — **never explicitly confirmed**, see §4 |
| 13 | `daily_risk_state` supports **both** portfolio-level and per-instrument loss-limit grains simultaneously via a nullable `instrument` column + two partial unique indexes, rather than picking one | My own resolution of an open question — **never explicitly confirmed**, see §4 |
| 14 | `reconciliation_breaks` table + `SUBMITTED_UNCONFIRMED` intent status added for ambiguous-ack handling (network timeout during order placement) | Task 2.1 crash-recovery scenario analysis |
| 15 | Repository layer: one repository per **aggregate**, not per table — `Trade` and `PositionStateTransition` are methods on `PositionRepository`, not separate repositories | Task 4 |
| 16 | Repositories `flush()`, never `commit()` — the caller (via an injected `Session`) owns the transaction boundary | Task 4 |
| 17 | Idempotent creation (`get_or_create`) uses a SAVEPOINT (`session.begin_nested()`), not read-then-write, so a duplicate-insert race never poisons the caller's outer Postgres transaction | Task 4 |
| 18 | `StaleDataError` (optimistic-lock conflict) translated to `ConcurrentModificationError` at the repository boundary — callers never see raw SQLAlchemy exceptions | Task 4 |
| 19 | No `scoped_session` — explicit `Session` injection throughout, consistent with the repository layer's constructor-injection design | Task 6 |
| 20 | `expire_on_commit=False` on the sessionmaker — avoids `DetachedInstanceError` when code reads an object's attributes after its `session_scope()` block has closed | Task 6 |
| 21 | `DATABASE_URL` read from environment only, never YAML, never hardcoded; `configs/database.yaml` holds only non-secret pool tuning, validated via Pydantic | Tasks 5 & 6 |
| 22 | `QueuePool` explicitly specified; `pool_pre_ping=True` treated as load-bearing (not generic best practice) — a silently-dead connection at the moment a fill is being persisted is exactly the failure this platform can't tolerate | Task 6 |
| 23 | Alembic migrations live inside the package (`src/algo/database/migrations/`), matching the already-approved project tree, not Alembic's own top-level default | Task 5 |

## 2. Files created

```
algo_platform/
├── alembic.ini
├── pyproject.toml
├── .env                                  (placeholders only, no real secrets)
├── configs/
│   ├── app.yaml, brokers.yaml, accounts.yaml, risk.yaml,
│   │   holidays.yaml, market_data.yaml                    (TODO stubs — not yet built)
│   ├── database.yaml                                      (real content — pool tuning)
│   ├── instruments/{nifty,sensex}.yaml                     (TODO stubs)
│   └── strategies/strategy_1/{nifty,sensex}.yaml           (TODO stubs)
├── scripts/*.py                                            (6 files, all TODO-stub entry points)
└── src/algo/
    ├── app.py, scheduler.py, dependency_container.py        (TODO stubs)
    ├── common/{constants,enums,exceptions,decorators,utilities}.py
    │       — enums.py is REAL and load-bearing (16 enums used throughout the DB layer)
    │       — the other four are still TODO stubs
    ├── database/
    │   ├── database.py                    — REAL: Engine construction, pool config, Pydantic settings
    │   ├── session.py                      — REAL: sessionmaker, session_scope() transaction management
    │   ├── models/                         — REAL: all 10 ORM models + base.py + enum_column.py
    │   │   (account, strategy_instance, position, trade, order, order_intent,
    │   │    position_state_transition, daily_risk_state, risk_control_flag,
    │   │    reconciliation_break)
    │   ├── repositories/                   — REAL: 8 repositories + base.py + exceptions.py
    │   ├── migrations/                     — REAL: env.py, script.py.mako, one migration
    │   │   └── versions/3634d974da39_initial_schema.py  (create/drop all 10 tables)
    │   └── seed/                           — empty stub, not yet built
    ├── market_data/, strategy_engine/, execution/, risk/, portfolio/,
    │   accounts/, brokers/, services/, reporting/, monitoring/,
    │   backtesting/, logging/, tests/       — ALL still TODO-stub files from the
    │                                          Task 1 skeleton pass, no real logic anywhere
    └── SESSION_HANDOFF.md                  (this file)
```

Everything under `database/` (models, repositories, migrations, database.py, session.py) is real, implemented, and verified. Everything else in the tree is a docstring-and-TODO-only stub created in the initial skeleton pass and never touched since.

## 3. Design rationale (the "why" behind the biggest calls)

- **Normalized `Trade` per leg, not flat CE/PE columns on `Position`**: extends cleanly to a future multi-leg strategy (e.g. an iron condor) without a schema change, and lets one leg have a richer order/retry history than its sibling without contorting the parent row.
- **`order_intents` written before any broker call**: this is what makes crash recovery possible at all. Without a durable "about to do this" record, a crash between a successful broker fill and the DB write recording it is unrecoverable — the DB looks empty, the entry job re-fires, and the system doubles a naked-ish straddle. The whole crash-recovery design (Task 2.1) hinges on this one decision.
- **SAVEPOINT-based `get_or_create`, not read-then-write**: Postgres aborts an *entire* transaction on any statement error, including a caught `IntegrityError` from a duplicate-insert race. Without a SAVEPOINT wrapping the risky insert, the caller's outer transaction would be poisoned even though the "conflict" is an entirely expected, handled condition (e.g. a scheduler misfire).
- **Repository boundaries drawn at the aggregate, not the table**: `Trade` and `PositionStateTransition` have no meaningful lifecycle independent of their parent `Position` — folding them into `PositionRepository` matches how they're actually used (always loaded/written together) rather than mechanically mapping one repository per table.
- **`daily_risk_state`'s dual grain via nullable `instrument`**: resolves the "portfolio vs per-instrument loss limit" question without forcing a premature choice — both grains coexist, enforced by two separate partial unique indexes (a plain 3-column unique constraint would *not* prevent two portfolio-level rows, since SQL treats every `NULL` as distinct from every other `NULL`).
- **`pool_pre_ping=True` and the Postgres-specific `connect_args`**: chosen and tuned specifically for this system's stakes, not lifted from a generic template — a dead connection discovered only when trying to persist a fill is precisely the failure this platform is built to avoid.

## 4. Assumptions made without explicit confirmation

These are the places where I made a call and flagged it, rather than getting an explicit yes — worth a deliberate second look before they become expensive to change:

1. **Auto-unwind partial-entry policy** (Scenario 4/7 in the crash-recovery review) — implemented as my recommended default (`TradeLegStatus.UNWOUND` exists in the enum), but the alternative (freeze-and-alert, leaving the naked leg live) was never explicitly ruled out by you.
2. **`daily_risk_state` "both grains" resolution** — I resolved an open multiple-choice question with a design that supports both rather than picking one; this is a reasonable engineering answer to an unanswered question, not something you selected among options.
3. **Which tiers to build now** — I built Tier 1 (the original 5 tables) + Tier 2 (`order_intents`, `position_state_transitions`, `daily_risk_state`, `risk_control_flags`) + `reconciliation_breaks`, and deferred `instrument_master` and `event_log`. This was my recommendation, accepted by proceeding, not a separate explicit confirmation.
4. **Kill-switch architecture** (independent async watcher vs. inline check in `monitor.py`) — I proposed the independent-watcher interpretation to resolve an apparent contradiction in the original spec, but this was never confirmed via the interrupted AskUserQuestion call.
5. **0DTE / expiry-day entry policy** (skip vs. trade normally) — recommended default was "skip," never confirmed.
6. **Single-writer process assumption** — the whole optimistic-locking/recovery design assumes one platform process per account; no advisory lock or multi-process coordination exists.
7. **psycopg2 as the driver** (not psycopg3 or asyncpg) — matches `pyproject.toml`, consistent with the sync-SQLAlchemy decision, never separately debated.

## 5. Outstanding TODOs

From the Task 7 review, concrete and verified against source (not speculative):

- **Missing indexes**: `orders.broker_tag` (used by `get_by_broker_tag()`, currently a full table scan), `positions.needs_reconciliation` (no partial index), `strategy_instances.status = 'ACTIVE'` (only `FROZEN` has a partial index — asymmetric with the scheduler's more-frequent arm-guard query).
- **Missing constraints**: no both-or-neither pairing enforced between related columns on several tables — `orders` (`status='COMPLETE'` vs `average_price`, `filled_at` vs `placed_at`), `reconciliation_breaks` (zero CHECK constraints at all — `resolution` vs `resolved_at`/`resolved_by`), `risk_control_flags` (`cleared_at` vs `cleared_by`), `daily_risk_state` (`breached` vs `breached_at`), `trades` (`entry_price` vs `entry_time`, `exit_price` vs `exit_time`).
- **`Account` has no `version` column** — inconsistent with every other table's optimistic-lock coverage.
- **`database/migrations/env.py` duplicates `DATABASE_URL` resolution logic** that now also exists in `database/database.py` (env.py was built one task before database.py existed) — should be reconciled to one source of truth.
- **No `idle_in_transaction_session_timeout` or dedicated `lock_timeout`** configured at the session level.
- **No retry-on-transient-error** at the session/commit layer for transient connectivity blips.
- **No caching** for the kill-switch's hot-path `list_active()` read.
- **No pagination** on any repository `list_*` method.
- **`database/seed/` is still empty** — nothing bootstraps the first `Account`/`StrategyInstance` row yet; needed before `start_paper.py` can run.
- **No connection-level observability** (no logging hook on new connections, slow checkouts, or pool exhaustion).
- **Regulatory question never answered**: whether SEBI's algo-trading framework (empanelled algo IDs, order tagging) applies to this deployment — could affect whether `Order` needs an `algo_id` field.
- **Alerting channel never decided** (email/Telegram/Slack/SMS/PagerDuty) — `monitoring/alert_dispatcher.py` can't be meaningfully built without this.
- **Product type (MIS vs NRML)** and its interaction with Kite's own auto-square-off timing, relative to the platform's own hard cutoff — never confirmed.

## 6. Known limitations

- **No live Postgres instance is available in this development environment.** Every verification this session was done through a combination of: (a) direct SQLAlchemy dialect compilation against the Postgres dialect (no connection needed), (b) live execution against SQLite with compile shims (for JSONB/BigInteger, which SQLite doesn't natively support) to prove mechanical correctness, and (c) Alembic's offline `--sql` mode (also no connection needed) to see the exact DDL Postgres would run. Nothing has been run against a real Postgres server. A smoke test against real Postgres (even local Docker) is recommended before paper trading.
- **The Alembic migration has only ever been exercised "from scratch."** Upgrading from empty to full schema and back down again is proven; a schema change against a table that already holds real rows (a fundamentally riskier class of migration) has not been attempted or even simulated.
- **The crash-recovery design (Task 2.1) is schema-supported but functionally untested.** The tables and constraints that make recovery *possible* exist and are verified in isolation; the actual recovery *workflow* (detect in-doubt state on startup, query the broker, repair the DB) has no implementation yet, because `entry_logic.py` and `reporting/reconciliation.py` don't exist.
- **Every module outside `database/` is a docstring-and-TODO stub.** `brokers/`, `strategy_engine/`, `execution/`, `risk/`, `market_data/`, `services/`, `monitoring/` — none of it has real logic. The database layer is solid, but it is currently a foundation with nothing built on top of it yet.
- **Single-process assumption throughout.** Optimistic locking and the recovery design both assume exactly one platform process touches a given account's data at a time; there is no advisory lock or leader-election mechanism if that assumption is ever violated.

## 7. Next recommended implementation step

**Step 2 of the original delivery plan: `brokers/broker_base.py` + `brokers/simulation/`.**

This is already queued (task tracker shows it as the next unblocked item) and matches the delivery order agreed at the very start of this session: database models → **broker interface + fake broker** → strategy_1 logic tested against the simulation broker → configs → risk core → real Kite integration → paper trading script. Building the simulation broker next, before any strategy logic, means `strategy_engine/strategies/strategy_1/` can be built and tested end-to-end (including the crash-recovery paths this session designed so carefully) without ever touching a real broker connection or real capital.

Two things worth deciding *before* or *during* that step, since they'll shape `broker_base.py`'s interface directly: the partial-entry policy (§4.1) and the product-type/auto-square-off question (§5) — both affect what the broker interface needs to expose (e.g., whether `place_order` needs an explicit unwind/cancel path baked in, whether product type is a parameter or fixed).
