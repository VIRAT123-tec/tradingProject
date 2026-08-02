# SUMMARY_STRATEGY — Strategy-1 (Short Straddle) Deep Reference

> Companion to [SUMMARY.md](./SUMMARY.md). Covers Strategy-1's lifecycle, the
> position state machine, entry/option-selection/order/monitor/exit, risk, crash
> recovery, error handling, and every config value it reads.

Package: **`src/algo/strategy_engine/strategies/strategy_1/`**

| File | Class(es) | Role |
|---|---|---|
| `strategy.py` | `Strategy1` | orchestrator: routes triggers/ticks/monitor-cycles; `recover`; `config_schema`; builds sub-components |
| `config.py` | `Strategy1Config`, `RetrySettings` | pydantic validation only |
| `strike_selector.py` | `StrikeSelector`, `StraddleStrikeSelection`, `StrikeSelectionError`; `compute_atm_strike` | ATM + contract resolution + verification |
| `combined_premium.py` | `CombinedPremiumTracker`, `PremiumThresholds`, `PremiumSnapshot`; `compute_combined_premium`, `compute_thresholds` | premium/target/SL math + live tracking |
| `entry_logic.py` | `EntryLogic`, `EntryOutcome`, `EntryResult` | pre-checks, durable record, order placement, auto-unwind |
| `exit_logic.py` | `ExitLogic`, `ExitOutcome`, `ExitResult`; `evaluate_exit`, `compute_leg_realized_pnl` | exit decision + execution |
| `monitor.py` | `PositionMonitor` | live monitoring (ticks + heartbeat) |
| `state_machine.py` | `PositionStateMachine`, `IllegalStateTransitionError` | legal transition gate |
| `exceptions.py` | `MissingFillPriceError` | money-path fail-loud |

---

## 1. Objective & lifecycle

**Non-directional ATM short straddle**, one cycle/day/instrument, intraday. Registered as `@register_strategy("strategy_1")` on `Strategy1` (`strategy.py:70`).

```
09:20 entry → sell ATM CE + ATM PE → collect combined premium
   profit if combined premium falls (target = entry×(1−target_pct))
   loss   if combined premium rises (stoploss = entry×(1+sl_pct))
exit on first of: 15:30 cutoff (TIMEOUT) | kill-switch (KILL_SWITCH) | stoploss | target
```

`Strategy1.__init__` builds `StrikeSelector`, `EntryLogic`, `ExitLogic`, `PositionMonitor` from the injected `StrategyContext` (unless a test injects fakes). It reads `context.config` (must be `Strategy1Config`), `retry` settings, and passes `polling_interval_seconds` to the monitor as the stale-tick threshold.

`scheduled_triggers()` declares two: `TimeTrigger("entry", entry_time, SKIP)` and `TimeTrigger("cutoff", hard_cutoff_time, FIRE_ON_STARTUP)`. `on_time_trigger` routes `entry`→`_handle_entry_trigger`→`EntryLogic.enter`; `cutoff`→`PositionMonitor.poll_and_check`. `on_market_tick`→`monitor.on_tick`; `on_monitor_cycle`→`monitor.poll_and_check`; `on_shutdown`→`monitor.stop`.

---

## 2. Position State Machine (`state_machine.py`)

```
IDLE ─────► ENTRY_PENDING ─────► OPEN ─────► EXIT_PENDING ─────► CLOSED (terminal)
  │              │                 │               │               ▲
  └────────────► ERROR ◄───────────┴───────────────┘               │ (MANUAL/RECOVERY only)
       (ERROR reachable from every non-terminal state) ────────────┘
```
`_ALLOWED_TRANSITIONS`:
- IDLE → {ENTRY_PENDING, ERROR}
- ENTRY_PENDING → {OPEN, CLOSED, ERROR}
- OPEN → {EXIT_PENDING, ERROR}
- EXIT_PENDING → {CLOSED, ERROR}
- ERROR → {CLOSED}  *(actor-restricted: only `MANUAL` or `RECOVERY`)*
- CLOSED → {} (terminal)

`PositionStateMachine.transition(position, to_state, actor, reason)`:
- same-state → idempotent no-op (returns `None`, no audit row) → this is what lets recovery re-apply safely.
- else `assert_transition_allowed` (raises `IllegalStateTransitionError`) → `PositionRepository.transition_state` (writes state + audit row + bumps `version`).
- `mark_error(position, actor, reason)` → convenience for `→ERROR` (reason required).
- `ConcurrentModificationError` is allowed to propagate (retry is the caller's decision).

**Three "state" concepts** (don't conflate): `PositionState` (above); `InstanceStatus` (ACTIVE/FROZEN/DISABLED on `strategy_instances`); `RunnerStatus` (RUNNING/FROZEN/STOPPED, in-memory). A position `ERROR` also freezes the instance (`StrategyRunner._freeze_instance`).

---

## 3. Entry (`entry_logic.py::EntryLogic.enter`)

Ordered pre-checks (each failure aborts **before** any broker call; returned as `EntryResult`, not raised):
1. **Already entered today?** `_find_existing_position` → `SKIPPED_ALREADY_EXISTS`.
2. **Expiry-day skip?** `_should_skip_expiry_day`: `config.skip_on_expiry_day` and `ExpiryService.get_current_weekly_expiry(instrument, trade_date) == trade_date` → `SKIPPED_EXPIRY_DAY`.
3. **Risk?** `_check_risk`: `context.risk.is_halted(identity)` (flags) and `approve_entry(identity, quantity=lots)` → `BLOCKED_BY_RISK`.
4. *(Trading-day/holiday/market-timing are enforced upstream by `PlatformScheduler`, not re-checked here.)*
5. **Resolve straddle:** `_resolve_straddle` → spot LTP + `StrikeSelector.select`.
6. **Durable record:** `_create_durable_record` → `Position(ENTRY_PENDING)` + 2 `Trade` + 2 `OrderIntent`, committed **before** ordering.
7. **Place + finalize:** `_place_and_finalize`.

**Order placement (`_place_and_finalize`):**
- `_place_leg(CE, qty)`: mark intent SUBMITTED_UNCONFIRMED (commit) → `broker.place_order(SELL, MARKET, product_type, tag)` → `_confirm_and_record` polls `broker.get_order` (`_poll_until_terminal`, up to `fill_confirmation_attempts`×`fill_confirmation_delay`) → reconcile order row → read fill price.
  - **Money guard:** COMPLETE with `average_price is None or <=0` → **raise `MissingFillPriceError`** (freeze → ENTRY_PENDING → recovery). Never a 0 fill.
  - Broker exceptions: `OrderRejected`/`InvalidOrderRequest`→FAILED; `Connection`/`RateLimit`→FAILED ("not sent"); `Timeout`→AMBIGUOUS (never resend).
- CE FILLED → `_place_leg(PE)`.
- **Partial handling:** CE FAILED → no PE, ERROR. CE AMBIGUOUS → ERROR + `AMBIGUOUS_ACK` break (never place PE). CE FILLED but PE fails/ambiguous → **auto-unwind CE** (`_auto_unwind` buys CE back) + `PARTIAL_ENTRY`/`AMBIGUOUS_ACK` break + ERROR; unwind failure → CRITICAL "NAKED EXPOSURE".
- Both FILLED → `_finalize_open`: `CombinedPremiumTracker.record_entry(ce_fill, pe_fill)` → `compute_thresholds` → persist `combined_entry_premium`, `target_premium`, `stoploss_premium` → `transition(ENTRY_PENDING→OPEN)` → `PositionMonitor.attach`.

**Premium/threshold math (`combined_premium.py`):**
- `compute_combined_premium(ce, pe) = ce + pe`.
- `compute_thresholds(entry, target_pct, sl_pct)` → `target = entry×(1−target_pct)`, `stoploss = entry×(1+sl_pct)`.

---

## 4. Option selection (`strike_selector.py::StrikeSelector.select`)
- **Spot:** `SpotPriceProvider.get_spot_ltp(instrument)` → `broker.get_ltp` on the cash segment (`spot_exchange`/`spot_symbol`); `LookupError` if unpriced.
- **ATM:** `compute_atm_strike(spot, strike_interval)` = `round(spot/interval, HALF_UP) × interval`.
- **Expiry:** `ConfigExpiryService.get_current_weekly_expiry` (weekly nearest-weekday or monthly last-weekday, holiday-shifted — see SUMMARY_API/holidays).
- **Contracts:** broker resolves CE/PE for `(underlying, expiry, strike)`; `_validate_resolved_contract` asserts strike/expiry/option_type/exchange match, else `StrikeSelectionError`.
- Output: `StraddleStrikeSelection(atm_strike, expiry, strike_interval, spot_ltp, call, put)`.

---

## 5. Monitoring (`monitor.py::PositionMonitor`)
- **Two entry points, one decision:** `on_tick(tick)` (push, from `dispatch_tick`) and `poll_and_check()` (pull, from `MonitoringScheduler` heartbeat + cutoff trigger). Both call `_evaluate(combined_premium)` → `ExitLogic.evaluate`.
- **Prices:** `CombinedPremiumTracker.on_tick` updates the ticking leg. `_is_market_data_stale()` (no tick within `polling_interval_seconds` or feed disconnected) → `_poll_prices` via `broker.get_ltp` before evaluating.
- `current_snapshot` = CE+PE (seeded with fill prices at entry → available immediately).
- Cutoff & kill-switch fire **without a price**.
- `_trigger_exit(reason)` → `ExitLogic.exit(reason)`.

---

## 6. Exit (`exit_logic.py`)

**Decision — `evaluate_exit` (pure), priority high→low:**
```
now_ist_time >= hard_cutoff_time → TIMEOUT       (price-independent)
halted                           → KILL_SWITCH   (price-independent)
combined_premium is None         → no exit
combined_premium >= stoploss     → STOPLOSS
combined_premium <= target       → TARGET
else                             → stay in
```
`ExitReason` values: TARGET, STOPLOSS, TIMEOUT, KILL_SWITCH, MANUAL, ERROR. **No separate "emergency exit" reason** — emergency-exit is a risk *flag* → `is_halted` → KILL_SWITCH branch. `MANUAL` is operator/reconciliation only.

**Execution — `exit(reason)`:**
- Acquire `_exit_lock.acquire(blocking=False)` first → if held, return `SKIPPED_EXIT_IN_PROGRESS` (exactly-one-exit). `try/finally` releases it.
- `_prepare_exit`: validate state (OPEN or resume EXIT_PENDING), write exit intents, `transition(OPEN→EXIT_PENDING)`.
- Close each leg (`_close_leg`→`_confirm_close`/`_adopt_existing_order`) via `broker.place_order(BUY, MARKET)`. Idempotent via broker tags (`find_order_by_tag`). COMPLETE-with-no-price close → `_close_fill_without_price` → returns **AMBIGUOUS** (never records a 0).
- **All CLOSED** → `_finalize_closed`: guard each leg's `exit_price`/`realized_pnl` not None (else `MissingFillPriceError` → caught → reconciliation break + PERSISTENCE_DIVERGENCE, leaves EXIT_PENDING); `combined_exit = Σ exit_price`; `realized = Σ compute_leg_realized_pnl = Σ (entry−exit)×qty`; `transition(EXIT_PENDING→CLOSED)`.
- **Any leg not closed** → CRITICAL + reconciliation break + `_fail_to_error` (ERROR + freeze); never unwind.
- After CLOSED → `_export_closed_trades` (Excel) + `_record_trade_history` (analytics), both exception-isolated.

**Exactly-once exit — three layers:** (1) `StrategyRunner` per-instance RLock serializes dispatches; (2) `ExitLogic._exit_lock` (non-blocking) covers non-dispatch callers (recovery) and any future parallelism; (3) DB optimistic locking (`version`) is the final cross-thread/process guard (independent kill switch).

---

## 7. Risk (`risk/`)
- `RiskCore.is_halted(identity)` — true if any active kill-switch/emergency-exit/freeze flag applies (checked each monitor cycle → KILL_SWITCH exit).
- `RiskCore.approve_entry(identity, quantity)` — margin + `max_daily_entries_per_account` + daily-loss-limit latch.
- **Daily loss limit** (`risk_core.py`): `total_pnl = realized + unrealized`; `crossed = total_pnl <= -effective_limit`; breach is **latched** (`daily_risk_state.breached`) and survives restart. **Blocks new entries; does NOT force-close open positions** (per-position SL protects those). The decision is read-only (no lock race); the persist is best-effort (tolerates `ConcurrentModificationError`).
- `KillSwitch` (`kill_switch.py`) — the producer that writes `risk_control_flags`; scope GLOBAL/ACCOUNT/STRATEGY_INSTANCE.

---

## 8. Crash recovery (`Strategy1.recover`)

Runs at `runner.start()` (on the **main thread**, before the scheduler loops begin, after startup reconciliation). Reads today's `Position` and acts by state:
- none / IDLE → `initialize()` (fresh start).
- **OPEN** → `PositionMonitor.attach()` (resume monitoring).
- **EXIT_PENDING** → re-run `ExitLogic.exit` with the reason inferred from `position_state_transitions` (`_infer_pending_exit_reason`); idempotent via `_exit_lock` + broker tags.
- **ENTRY_PENDING** → `ERROR` + freeze (no supported auto-resume of an interrupted entry).
- CLOSED / ERROR → nothing.

**Startup reconciliation** (`ReconciliationEngine.reconcile`, before recovery) repairs in-doubt `order_intents`/`orders`/`positions` against broker truth (read-only broker calls), records `reconciliation_breaks`; **never auto-trades**; if the broker is unreachable, logs CRITICAL and proceeds unrepaired.

---

## 9. Error handling (Strategy-1)
- **Entry failure** → `EntryResult` outcome (SKIPPED/BLOCKED/REJECTED/PARTIAL_ENTRY_ERROR/AMBIGUOUS_ERROR); ERROR outcomes → `_fail_to_error` (ERROR + break + freeze). Unexpected exception (incl. `MissingFillPriceError`) → propagates → `StrategyRunner` freeze.
- **Exit failure** → break + `_fail_to_error`; `_exit_lock` guarantees single exit.
- **Broker failure** → classified via `brokers/exceptions.py` (Retryable/NonRetryable). Timeout on a mutating call = AMBIGUOUS (never resend).
- **Freeze** → `StrategyRunner._run_isolated` catches any hook exception → `RunnerStatus.FROZEN` + `InstanceStatus.FROZEN`; process survives; other instances unaffected.
- **DB race** → `ConcurrentModificationError` (loser); `order_update_processor` rolls back and defers to the committed row.

---

## 10. Config values Strategy-1 reads (`config.py::Strategy1Config`)
From `configs/strategies/strategy_1/<instrument>.yaml`, all **required, no Python defaults**:
- `entry_time`, `hard_cutoff_time` (IST times; entry < cutoff enforced).
- `target_pct`, `sl_pct` (Decimal, `0 < x < 1`).
- `lots` (>0; order qty = `lots × lot_size`).
- `product_type` (INTRADAY/NORMAL).
- `skip_on_expiry_day` (bool; no default).
- `monitoring_interval_seconds`, `polling_interval_seconds` (polling ≤ monitoring).
- `retry: {order_timeout_seconds(null=broker default), fill_confirmation_attempts, fill_confirmation_delay_seconds, close_retry_attempts, close_retry_delay_seconds}` — `close_retry_*` apply to exit only.

From `configs/instruments/<instrument>.yaml` (via `ConfigInstrumentService`): `exchange`, `strike_interval`, `lot_size`, `tick_size`, `spot_exchange`, `spot_symbol`, `expiry_weekday`, `expiry_cadence`, `underlying_symbol`.

---

## Sequence diagram (entry → close)
```
Scheduler ── entry ──► Strategy1.on_time_trigger ──► EntryLogic.enter
   EntryLogic ──► RiskCore.approve_entry
   EntryLogic ──► SpotPriceProvider.get_spot_ltp ──► broker.get_ltp
   EntryLogic ──► StrikeSelector.select ──► broker (resolve CE/PE) ──► verify
   EntryLogic ──► PositionRepository.get_or_create (ENTRY_PENDING) + intents
   EntryLogic ──► broker.place_order(SELL CE) ──► broker.get_order (confirm)
   EntryLogic ──► broker.place_order(SELL PE) ──► broker.get_order (confirm)
   EntryLogic ──► compute_thresholds ──► StateMachine(ENTRY_PENDING→OPEN) ──► Monitor.attach
   ...ticks/heartbeat... Monitor._evaluate ──► ExitLogic.evaluate_exit
   ExitLogic.exit ──► StateMachine(OPEN→EXIT_PENDING) ──► broker.place_order(BUY ×2)
   ExitLogic._finalize_closed ──► realized_pnl ──► StateMachine(EXIT_PENDING→CLOSED)
   ExitLogic ──► TradeReportExporter + TradeHistoryRecorder
```
