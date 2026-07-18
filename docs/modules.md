# What's in Each Folder (Module Guide)

A plain tour of the code under `src/algo/`, so you know what each part is for
and — importantly — **what is finished versus what is still a placeholder.**

Three words you'll see below:

- **Done** — built, tested, working.
- **Interface only** — the "shape" (a Python `Protocol`) exists, but the real
  working version hasn't been written yet.
- **Placeholder** — just a file with a note saying "to be built." Not working.

> Heads up: early in the project, lots of empty placeholder files were created
> to sketch out the plan. Whole folders (`accounts/`, `backtesting/`,
> `execution/`, `logging/`, `monitoring/`, `portfolio/`, `reporting/`, and most
> of `risk/`) are still placeholders. They are future plans, not shipped work.

---

## The "on switch" and assembly

- `start_paper.py` — start in paper mode (fake broker). **Done**, but see note.*
- `start_live.py` — start in live mode (real broker). **Done**, but see note.*
- `app.py` — the process wrapper: handles Ctrl-C and clean shutdown. **Done.**
- `dependency_container.py` — reads all config and builds every piece. **Done.**

\* Both entrypoints work, but they deliberately refuse to fully start yet
because a few required helpers (see "Services" below) don't exist. This is
on purpose, not a bug.

## The strategy framework (`strategy_engine/`)

The reusable machinery any strategy would use.

- `strategy_base.py`, `strategy_context.py`, `strategy_runner.py`,
  `instance_factory.py`, `parameter_loader.py`, `strategy_registry.py` — **Done.**
- `strategy_scheduler.py` — **Done.** The `MonitoringScheduler` heartbeat that
  drives the during-the-day stop-loss/target checks. This resolved the former
  top gap ("C1").

## The straddle strategy (`strategy_engine/strategies/strategy_1/`)

The actual trading logic, split into small tested pieces — **all Done:**

- `strategy.py` — the coordinator that ties the pieces together.
- `strike_selector.py` — picks the at-the-money strike and the call/put contracts.
- `entry_logic.py` — places the entry safely (the crash-proof sequence).
- `exit_logic.py` — decides and places the exit.
- `monitor.py` — watches the open position.
- `combined_premium.py` — the profit/loss math.
- `state_machine.py` — the rules for what state a position can move to.
- `config.py` — the strategy's settings and their validation.

## The broker (`brokers/`)

- `broker_base.py` — the common interface every broker must follow. **Done.**
- `exceptions.py` — the different kinds of broker errors. **Done.**
- `rate_limiter.py` — stops the platform from calling the broker too fast. **Done.**
- `simulation/` — the fake broker for paper/testing. **Done.**
- `kite/kite_broker.py`, `kite/mapper.py`, `kite/kite_auth.py` — the real Kite
  broker. **Done.**
- `kite/websocket.py` — the live order-update feed. **Done, but not switched
  on** (blocker "H3"): fills are currently detected by asking the broker
  repeatedly instead.

## The price feed (`market_data/`)

- `market_data_service.py`, `market_cache.py`, `subscription_manager.py`,
  `tick_router.py`, `websocket_manager.py` — **Done.**
- `candle_builder.py`, `option_chain_builder.py`, and similar — **Placeholder.**

## Safety / risk (`risk/`)

- `risk_core.py` — the pre-trade safety checks and the "are we halted?" check.
  **Done.**
- Everything else here (`kill_switch.py`, `emergency_exit.py`,
  `daily_loss_limit.py`, `margin_monitor.py`, …) — **Placeholder.** These were
  meant to *trigger* the kill switch and enforce the loss limit. Because they're
  missing, the kill switch and daily loss limit can be *read* but never
  *activated* (blockers "H1" and "H2").

## Shared services (`services/`)

- `time_service.py` — the one place the current time is read. **Done.**
- `reconciliation_engine.py` — the startup crash-cleanup. **Done.**
- `instrument_service.py`, `expiry_service.py`, `pricing_service.py` —
  **Interface only.** The real versions (which need live instrument data) don't
  exist yet. This is why the entrypoints can't fully start (blocker "H4").
- `holiday_service.py`, `strike_service.py` — **Placeholder.**

## The daily timer (`scheduler/`)

- `platform_scheduler.py` — fires the 09:20 and 15:15 timers. **Done.**
- `trading_calendar.py` — knows weekends aren't trading days. **Done** (but
  doesn't yet know about holidays — it treats every weekday as a trading day).

## Storage (`database/`)

The database models, the code that reads/writes them, and the schema
migration — **all Done.** This was the first thing built and is the most
battle-tested part of the platform.

## Shared basics (`common/`)

- `enums.py` — the shared named values (position states, order statuses, etc.).
  **Done.**
- `constants.py`, `decorators.py`, `exceptions.py`, `utilities.py` —
  **Placeholder.**
