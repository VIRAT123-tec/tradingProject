# SUMMARY_ARCHITECTURE — Folders, Files, Execution Flow, Threading

> Companion to [SUMMARY.md](./SUMMARY.md). Covers folder structure, per-file
> responsibilities, startup/shutdown/runtime flows, the threading model, the
> dependency graph, data/event flow, and the complete file index.

---

## 2. Folder Structure

```
algo_platform/                        # RUN FROM HERE (configs resolve relative to CWD)
├── alembic.ini                       # Alembic config: script_location=src/algo/database/migrations, url from DATABASE_URL
├── pyproject.toml                    # pinned deps + pytest config (testpaths=src/algo/tests)
├── .env                              # SECRETS (DATABASE_URL, MARKET_DATA_DATABASE_URL, KITE_*) — must be gitignored
├── README.md, SESSION_HANDOFF.md
├── SUMMARY*.md                       # this documentation set
├── configs/                          # ALL behaviour is config-driven
│   ├── app.yaml                      # instances to run + report_output_dir
│   ├── brokers.yaml                  # active_broker; secrets by env-var NAME
│   ├── accounts.yaml, risk.yaml, database.yaml, market_data.yaml, collector.yaml
│   ├── holidays.yaml                 # NSE/BSE holiday dates
│   ├── instruments/{nifty,sensex,banknifty,finnifty,midcpnifty,sensexbank}.yaml
│   └── strategies/strategy_1/{...}.yaml   # per-instrument strategy params
├── reports/                          # generated trades_<DD-MM-YYYY>.xlsx (business data — gitignore)
├── scripts/                          # generate_token.py, sync_instruments.py, reconcile_accounts.py, ...
└── src/algo/
    ├── start_paper.py, start_live.py, start_collector.py   # ENTRYPOINTS
    ├── app.py                        # Application (process lifecycle)
    ├── dependency_container.py       # composition root (DI)
    ├── instance_admin.py             # ops helper (freeze/unfreeze instances)
    ├── common/                       # enums.py, utilities.py, exceptions.py(stub), constants.py(stub), decorators.py(stub)
    ├── database/                     # engine/session/models/repositories/migrations/migration_guard
    ├── brokers/                      # broker_base.py + kite/ + simulation/ + paper_trading_broker.py + exceptions.py + rate_limiter.py
    ├── market_data/                  # trading-feed seam + services (TickStream, MarketDataService, ...)
    ├── market_data_collector/        # the SEPARATE collector subsystem
    ├── scheduler/                    # platform_scheduler.py, trading_calendar.py
    ├── strategy_engine/              # runner, registry, factory, context, base, strategy_scheduler, strategies/strategy_1/
    ├── services/                     # live_seams, holiday_service, expiry_service, reconciliation_engine, order_update_processor, trade_history_recorder, time_service
    ├── risk/                         # risk_core.py, kill_switch.py, daily_loss_limit.py, emergency_exit.py
    ├── reporting/                    # trade_report.py (Excel), pnl_manager.py(stub)
    ├── logging/                      # logger.py, alerting.py, strategy_logger.py
    ├── execution/                    # fill_manager.py(stub)
    ├── portfolio/, accounts/, monitoring/, backtesting/   # mostly STUBS (not wired)
    └── tests/                        # 843 tests, SQLite + fakes
```

### Why each folder exists / what belongs / what must NOT go there

| Folder | Why | Belongs | Never put here | Depends on |
|---|---|---|---|---|
| `configs/` | externalize all tunables | YAML only | secrets, code | — |
| `common/` | shared vocabulary + tiny utils | enums, pure helpers | domain logic (goes in services/) | nothing |
| `database/` | the ONLY persistence layer | models, repositories, migrations, engine/session | business logic, broker calls | common/ |
| `database/models/` | ORM table definitions | one file per table | queries, business rules | base.py, enum_column |
| `database/repositories/` | typed persistence ops | one file per aggregate; flush-not-commit | `commit()`, business decisions | models/ |
| `brokers/` | broker abstraction | `BrokerBase` + adapters + mapper + exceptions | strategy logic | broker_base, exceptions |
| `market_data/` | trading price-feed seam | TickStream impls, MarketDataService, subscription/routing | order placement | brokers/ |
| `market_data_collector/` | independent data pipeline | collector service, socket, writer, ATM window, timescale init | anything the trading path imports | its own db.py, config |
| `scheduler/` | wall-clock trigger firing | PlatformScheduler, TradingCalendar | strategy decisions | strategy_runner (type only) |
| `strategy_engine/` | generic runner + strategies | runner/registry/factory/context + strategies/* | broker/db specifics (use context) | services, database, brokers via context |
| `strategy_engine/strategies/strategy_1/` | Strategy-1 itself | entry/exit/monitor/strike/premium/state/config | generic engine code | context seams |
| `services/` | cross-cutting seams | instruments, expiry, holidays, reconciliation, recorder | strategy-specific logic | database, brokers, common |
| `risk/` | pre-trade gates + kill switch | risk core, flags, daily loss | order placement | database, common |
| `reporting/` | post-trade output | Excel exporter | trading decisions | database (read-only) |
| `logging/` | log config + alert sink + strategy log formatting | logger, alerting, strategy_logger | business logic | — |

### Important files (purpose · imported by · depends on)

- **`start_paper.py` / `start_live.py`** — build the seams dict, run the **migration guard**, construct `DependencyContainer`, run `Application`. Imported by: nothing (entrypoints). Depend on: `dependency_container`, `app`, `migration_guard`, `services.live_seams`, `services.holiday_service`, brokers.
- **`start_collector.py`** — build the collector (config, market-data engine, timescale init, ATM manager, tick stream, writer, `CollectorService`) and run forever. Depends on: `market_data_collector/*`, `services.live_seams`, `services.holiday_service`, `brokers.kite`.
- **`app.py::Application`** — process lifecycle: `run/start/wait_for_shutdown/stop`, signal handlers, broker-mode cross-check, clean partial-startup teardown. Used by: entrypoints. Depends on: `DependencyContainer` (type).
- **`dependency_container.py::DependencyContainer`** — the composition root. `__init__` = pure construction (no I/O); `start()` = ordered I/O (auth → accounts → runners → reconcile → market data → schedulers). Also imports `strategy_1.strategy` for its `@register_strategy` side effect (~line 57). Used by: entrypoints. Depends on: nearly everything.
- **`strategy_engine/instance_factory.py::InstanceFactory`** — `build_runner(strategy_id, instrument, account_id, exchange)` → resolves the `StrategyInstance` row, loads config via `ParameterLoader`, builds a `StrategyContext`, looks up the class (`StrategyRegistry.get`), constructs the `Strategy`, wraps in a `StrategyRunner`. Used by: container. Depends on: registry, parameter_loader, repositories.
- **`strategy_engine/strategy_registry.py`** — `@register_strategy("id")` decorator + `default_registry` (id→class map). Used by: strategy modules + factory.
- **`strategy_engine/strategy_runner.py::StrategyRunner`** — the generic per-instance wrapper: `RunnerStatus`, per-instance **RLock**, `dispatch_time_trigger/dispatch_tick/dispatch_monitor_cycle`, `_run_isolated` (fault isolation → FROZEN), `_freeze_instance`. Used by: schedulers, container.
- **`strategy_engine/strategy_context.py`** — `StrategyContext` (injected bundle: broker, risk, time, market_data, session_factory, instrument/expiry/spot services, logger, identity, config), `StrategyIdentity`, and the seam Protocols (`Tick`, `TimeProvider`, `RiskGateway`, `MarketDataGateway`, `SpotPriceProvider`, `TradeExporter`, `TradeHistoryRecorder`).
- **`strategy_engine/strategy_base.py::Strategy`** — the ABC every strategy implements (`scheduled_triggers`, `on_time_trigger`, `on_market_tick`, `on_monitor_cycle`, `recover`, `on_shutdown`, `config_schema`, `initialize`, `health`).
- **`scheduler/platform_scheduler.py::PlatformScheduler`** — wall-clock trigger firing; background daemon thread; at-most-once/day; trading-day gating; concurrent dispatch when triggers coincide.
- **`strategy_engine/strategy_scheduler.py::MonitoringScheduler`** — the ~2s exit heartbeat; background daemon thread → `dispatch_monitor_cycle`.
- See **[SUMMARY_STRATEGY.md](./SUMMARY_STRATEGY.md)** for the strategy_1 files, **[SUMMARY_DATABASE.md](./SUMMARY_DATABASE.md)** for database/*, **[SUMMARY_API.md](./SUMMARY_API.md)** for brokers/* and collector/*.

---

## 3. Architecture — Execution Flow

### 3.1 Startup sequence (trading: `start_paper`/`start_live`)
```
main()
  ├─ configure_logging(INFO, RecordingAlertDispatcher)
  ├─ guard_database_schema()                 # ABORT (exit 1) if DB schema != Alembic head
  ├─ [live only] _check_live_trading_confirmed()   # env I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes
  ├─ DependencyContainer(**build_seams())    # DI, pure construction, NO I/O
  └─ Application(container, expected_broker).run()
        └─ start() → container.start():
             1. broker.authenticate() + health_check()   # fail fast if broker unreachable
             2. _ensure_accounts()
             3. build_runner() per app.yaml instance      # construct StrategyRunners (no start)
             4. reconciliation_engine.reconcile()          # repair in-doubt DB vs broker truth
             5. register_order_update_callback + connect_websocket   # order postbacks
             6. market_data connect + register_consumer(dispatch_tick)   # price ticks
             7. scheduler.register(runner) → runner.start() → Strategy.recover()   # MAIN THREAD
             8. monitoring_scheduler.register(runner)
             9. scheduler.start(); monitoring_scheduler.start()   # spawn background loops
        └─ wait_for_shutdown()   # main thread blocks on _shutdown_event
```

### 3.2 Startup sequence (collector: `start_collector`)
```
main() → build_collector()
   ├─ load_collector_config(); build_market_data_engine(); initialize_schema()   # create_all + Timescale DDL, idempotent
   ├─ ConfigInstrumentService; HolidayService.from_config(); HolidayAwareTradingCalendar; ConfigExpiryService
   ├─ KiteMarketReader; AtmWindowManager; TickWriter; CollectorTickStream; CollectorMetrics; MarketHoursController
   └─ CollectorService.run_forever()
        ├─ TickWriter.start()   → spawn [collector-writer] thread
        └─ start()              → spawn [collector-controller] thread → _run loop
             CONNECT phase → CollectorTickStream.start() → spawn [KiteTicker reactor] thread
```

### 3.3 Runtime lifecycle (trading, per trading day)
```
09:20 PlatformScheduler fires "entry" → StrategyRunner.dispatch_time_trigger → Strategy1.on_time_trigger
      → EntryLogic.enter (checks → strike select → durable record → sell CE, sell PE → OPEN → monitor.attach)
intraday: price ticks → dispatch_tick → PositionMonitor.on_tick → evaluate_exit
          every ~2s: MonitoringScheduler → dispatch_monitor_cycle → poll_and_check → evaluate_exit
          15:30 cutoff trigger → poll_and_check
      → first exit trigger {TIMEOUT|KILL_SWITCH|STOPLOSS|TARGET} → ExitLogic.exit
          → EXIT_PENDING → buy back both legs → CLOSED → trade_history + Excel report
```

### 3.4 Shutdown sequence
```
SIGINT/SIGTERM → Application._handle → request_shutdown (sets _shutdown_event)
   → wait_for_shutdown returns → Application.stop → container.stop()
        PlatformScheduler.stop (Event + join)  → each StrategyRunner.stop → Strategy1.on_shutdown → monitor.stop (unsubscribe only)
        MonitoringScheduler.stop
        market data / broker websockets close
   Open positions are LEFT INTACT (intraday; recovered next start). Daemon threads cannot block exit.
```
Collector shutdown: `CollectorService.stop` sets `_stop`, joins controller (15s) + writer (30s, final drain), closes the socket.

### 3.5 Dependency graph (who depends on what)
```
entrypoints → dependency_container → { instance_factory, schedulers, risk, reconciliation, broker, services }
instance_factory → strategy_registry + parameter_loader + repositories
Strategy1 → context seams (broker, risk, time, market_data, session_factory, instrument/expiry/spot services)
EntryLogic/ExitLogic → repositories + state_machine + broker + combined_premium + strike_selector
schedulers → StrategyRunner → Strategy1
everything persistent → repositories → models → SQLAlchemy → PostgreSQL
```

### 3.6 Data & event flow
```
DATA (trading):  broker LTP ticks → MarketDataService → dispatch_tick → monitor → (exit decision)
DATA (writes):   entry/exit → repositories → unit_of_work → PostgreSQL
DATA (collector):Kite FULL ticks → CollectorTickStream → TickWriter deque → COPY → TimescaleDB
EVENTS:          time triggers (scheduler), order postbacks (KiteOrderUpdateStream → OrderUpdateProcessor),
                 risk flags (kill switch → is_halted), shutdown signals (Event)
CROSS-PROCESS:   none shared except each process's own DB; collector and trading never share state
```

---

## 4. Component Documentation (module map)

| Module | Purpose | Public classes / functions | Side effects |
|---|---|---|---|
| `common/enums.py` | shared enum vocabulary | `PositionState, ExitReason, OrderStatus, TradeLegStatus, IntentStatus, InstanceStatus, StateTransitionActor, Exchange, OptionType, OrderPurpose, TransactionType, OrderType, ProductType, RiskFlagType, RiskFlagScope, ReconciliationBreakType/Resolution, BrokerName` | none (pure) |
| `common/utilities.py` | tiny pure helpers | `pnl_per_share(total_pnl, lot_size)` | none |
| `database/database.py` | engine/settings | `load_database_settings, build_engine, get_engine, dispose_engine` | builds pool |
| `database/session.py` | session/tx | `build_session_factory, get_session_factory, unit_of_work, session_scope` | DB tx |
| `database/migration_guard.py` | boot schema check | `guard_database_schema, verify_database_is_current, SchemaOutOfDateError, alembic_head_revisions` | reads DB revision |
| `database/repositories/base.py` | shared persistence | `BaseRepository` (get_by_id, get_by_id_for_update, add, flush, _get_or_create) | flush |
| `scheduler/platform_scheduler.py` | time triggers | `PlatformScheduler, SchedulerConfig` | spawns thread, dispatches |
| `strategy_engine/strategy_scheduler.py` | exit heartbeat | `MonitoringScheduler, MonitoringSchedulerConfig` | spawns thread |
| `strategy_engine/strategy_runner.py` | per-instance wrapper | `StrategyRunner, RunnerStatus` | freezes instance |
| `services/live_seams.py` | instrument/expiry/spot | `ConfigInstrumentService, ConfigExpiryService, BrokerSpotPriceProvider, InstrumentConfig, KiteInstrumentTokenMap, build_kite_tick_stream` | reads config, broker |
| `services/holiday_service.py` | holiday calendar | `HolidayService, HolidayAwareTradingCalendar, load_holidays` | reads holidays.yaml |
| `services/reconciliation_engine.py` | startup repair | `ReconciliationEngine, ReconciliationReport` | reads broker, writes DB breaks |
| `services/order_update_processor.py` | order postback → DB | `OrderUpdateProcessor`, `reconcile_order_terminal` | writes orders |
| `services/trade_history_recorder.py` | analytics row | `TradeHistoryRecorder` | writes trade_history |
| `risk/risk_core.py` | pre-trade gates | `RiskCore` (`is_halted`, `approve_entry`, daily-loss check) | reads/writes daily_risk_state |
| `risk/kill_switch.py` | control flags producer | `KillSwitch` | writes risk_control_flags |
| `reporting/trade_report.py` | Excel | `TradeReportExporter` | writes .xlsx |
| `logging/logger.py` | log config | `configure_logging` | sets handlers |

---

## Threading model (summary — full detail was analyzed separately)

**Trading process:** Main + `PlatformScheduler` (daemon) + `MonitoringScheduler` (daemon) + one `KiteTicker` reactor thread (Twisted; price + order feeds share it) + (paper) `SimulationBroker` matching-engine thread + ephemeral dispatch threads.
**Collector process:** Main + `collector-controller` (daemon) + `collector-writer` (daemon) + `KiteTicker` reactor (daemon).

- **Locks:** `threading.Lock` almost everywhere (singletons, registries, subscription sets, deque, callback lists); `RLock` only in `StrategyRunner` (reentrant dispatch); non-blocking `Lock` in `ExitLogic._exit_lock`.
- **Events:** stop/flush/dead/shutdown (`Event.wait(timeout)` paces loops and enables instant shutdown).
- **Queue:** a bounded `collections.deque` in `TickWriter` (NOT `queue.Queue`).
- **Not used:** `asyncio`, `ThreadPoolExecutor`/`concurrent.futures`, `multiprocessing`, `Semaphore`, `Condition`, `Timer`, `threading.local`.
- **Rule:** the socket thread never touches the DB (collector enqueues only); callbacks are never invoked while holding a lock; `Session` is one-per-thread.

---

## File Index (complete)

| File | Purpose | Depends on | Used by |
|---|---|---|---|
| `start_paper.py` | paper entrypoint | container, app, migration_guard, live_seams, holiday_service, paper broker | run manually |
| `start_live.py` | live entrypoint | container, app, migration_guard, live_seams, holiday_service, kite | run manually |
| `start_collector.py` | collector entrypoint | market_data_collector/*, live_seams, holiday_service, kite | run manually |
| `app.py` | process lifecycle | container(type) | entrypoints |
| `dependency_container.py` | composition root | ~everything | entrypoints |
| `instance_admin.py` | ops (freeze/unfreeze) | repositories | operators |
| `common/enums.py` | enums | — | ~everything |
| `common/utilities.py` | `pnl_per_share` | decimal | recorder, report, strategy_logger |
| `database/database.py` | engine | dotenv, sqlalchemy, database.yaml | session, container |
| `database/session.py` | session/tx | database.py | repositories callers |
| `database/migration_guard.py` | schema guard | alembic, database.py | entrypoints |
| `database/models/*.py` | ORM tables | base, enum_column, enums | repositories |
| `database/repositories/*.py` | persistence ops | models, base | services, strategy, risk |
| `scheduler/platform_scheduler.py` | time triggers | strategy_runner, trading_calendar | container |
| `scheduler/trading_calendar.py` | TradingCalendar Protocol + WeekdayTradingCalendar | — | scheduler, expiry(historical) |
| `strategy_engine/strategy_runner.py` | runner | strategy_base, repositories | schedulers, container |
| `strategy_engine/strategy_scheduler.py` | monitoring heartbeat | strategy_runner | container |
| `strategy_engine/instance_factory.py` | build runners | registry, parameter_loader, repositories | container |
| `strategy_engine/strategy_registry.py` | id→class | strategy_base | strategy modules, factory |
| `strategy_engine/strategy_context.py` | context + seams | enums, brokers(type) | strategy, factory |
| `strategy_engine/parameter_loader.py` | load strategy YAML | pydantic, yaml | factory |
| `strategy_engine/strategies/strategy_1/*` | Strategy-1 | context seams, repositories, state_machine | runner (via registry) |
| `services/live_seams.py` | instrument/expiry/spot | instruments YAML, broker, holiday_service | entrypoints, container, strategy |
| `services/holiday_service.py` | holidays | holidays.yaml, enums | entrypoints, expiry |
| `services/reconciliation_engine.py` | startup repair | broker, repositories | container |
| `services/order_update_processor.py` | postback→DB | broker, order repo | broker websocket, entry/exit |
| `services/trade_history_recorder.py` | analytics | repositories, utilities | exit logic |
| `risk/risk_core.py` | gates | repositories, risk.yaml | strategy entry/exit |
| `risk/kill_switch.py` | flags | repositories | ops, risk |
| `reporting/trade_report.py` | Excel | openpyxl, repositories | exit logic |
| `logging/logger.py`, `alerting.py`, `strategy_logger.py` | logs | — | everywhere |
| `brokers/broker_base.py` | ABC | enums | all brokers, strategy(type) |
| `brokers/kite/*` | Kite adapter | kiteconnect | live |
| `brokers/simulation/*`, `paper_trading_broker.py` | paper | — | paper |
| `brokers/rate_limiter.py` | throttle | — | kite broker |
| `market_data/*` | trading price feed | brokers | strategy(via context) |
| `market_data_collector/*` | collector | kite, timescale | start_collector |
