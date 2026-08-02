# SUMMARY_API — Brokers, Websockets, Collector, Config, Logging

> Companion to [SUMMARY.md](./SUMMARY.md). Covers the broker abstraction, Kite
> integration (auth/token/orders/mapper), the three websockets, the market-data
> collector, the config files in depth, and logging.

---

## 1. Broker abstraction (`src/algo/brokers/`)

**`broker_base.py::BrokerBase(abc.ABC)`** — the seam every strategy/risk/service call goes through. Abstract methods:
`authenticate(timeout)`, `health_check(timeout)→HealthStatus`, `place_order(request, timeout)→placement`, `modify_order`, `cancel_order`, `get_order(broker_order_id, timeout)→BrokerOrder`, `get_orders`, `find_order_by_tag(tag, timeout)`, `get_positions`, `get_quote`, `get_ltp(instruments, timeout)→dict`, `connect_websocket`, `register_order_update_callback`.

DTOs (Pydantic `BaseModel`): `PlaceOrderRequest`, `BrokerOrder` (`broker_order_id, status(OrderStatus), transaction_type, average_price: Decimal|None, …`), `BrokerPosition`, `BrokerQuote`, `BrokerInstrument`, `HealthStatus`.

**Concrete implementations:**
- `brokers/kite/kite_broker.py::KiteBroker(BrokerBase)` — wraps `kiteconnect.KiteConnect`.
- `brokers/simulation/simulation_broker.py::SimulationBroker` — a full in-memory matching engine (own thread unless `SimulationConfig(synchronous=True)`), instrument catalog, static/live price source.
- `brokers/paper_trading_broker.py::PaperTradingBroker` — uses the real Kite client for **read-only** contract/price resolution + `SimulationBroker` for **execution**.

**`brokers/exceptions.py`** — the vocabulary all callers depend on (Retryable vs NonRetryable split):
- `BrokerError` → `RetryableBrokerError` {`BrokerConnectionError`, `BrokerTimeoutError`, `BrokerRateLimitExceededError`} and `NonRetryableBrokerError` {`BrokerAuthenticationError`, `OrderRejectedError`, `OrderNotFoundError`, `OrderNotModifiableError`, `OrderNotCancellableError`, `InstrumentNotFoundError`, `InvalidOrderRequestError`}.
- **Key rule:** `BrokerTimeoutError` on a **mutating** call (place/modify/cancel) = UNKNOWN outcome → never blind-retry; resolve via `find_order_by_tag`/`get_order` (the `SUBMITTED_UNCONFIRMED` window). On a read-only call it's safe to retry.

**`brokers/rate_limiter.py::RateLimiter`** — token buckets under a `Lock`; `acquire(deadline)` waits for a slot or raises `BrokerRateLimitExceededError` (never-sent → safe to retry).

### KiteBroker method → kiteconnect mapping (`kite_broker.py`)
| BrokerBase method | kiteconnect call | mapper |
|---|---|---|
| `authenticate` | `client.profile()` (verifies token) | — |
| `health_check` | `_read(client.profile)` | → `HealthStatus` |
| `place_order` | `client.place_order(**kwargs)` | `mapper.to_kite_order_params` → `order_id` |
| `modify_order`/`cancel_order` | `client.modify_order`/`cancel_order` | — |
| `get_order` | `client.order_history(id)` | `mapper.to_broker_order` |
| `get_orders` | `client.orders()` | `to_broker_order` |
| `get_positions` | `client.positions()` | `to_broker_position` |
| `get_ltp` | `client.ltp(*keys)` | → Decimal map |
| `get_quote` | `client.quote(*keys)` | `to_broker_quote` |
| instruments dump | `client.instruments(exchange)` | `to_broker_instrument` |
| `connect_websocket` | `KiteOrderUpdateStream` | — |

All reads go through `KiteBroker._read(...)` (timeout + rate-limit + broker-exception translation). `mapper._price_or_none` maps Kite's `0` → `None` (this is what lets the money-path guard detect "COMPLETE but no price").

---

## 2. Authentication & token (`brokers/kite/` + `scripts/generate_token.py`)

- **Token minting (daily, manual):** `python scripts/generate_token.py`:
  - `_build_token_manager()`: read `KITE_API_KEY`/`KITE_API_SECRET`, build `KiteConnect`, `EnvFileTokenStore`, `KiteSession`, `TokenManager`.
  - If a valid token exists for today (`check_existing()` = local same-day check + live `profile()`), skip unless `--force`.
  - Else `ensure_valid_token(on_login_required=_prompt_for_request_token)`: open Kite's hosted login (`webbrowser.open`), paste back the `request_token`, exchange via `KiteConnect.generate_session`, validate with `profile()`, store to `.env` (`EnvFileTokenStore`). **Prints only the last 4 chars.** Never collects password/TOTP.
- **Token storage:** `brokers/kite/token_store.py::EnvFileTokenStore` — reads/writes `.env` via `python-dotenv` (`KITE_ACCESS_TOKEN`, `KITE_ACCESS_TOKEN_GENERATED_AT`).
- **Token reuse:** at process start, `KiteBroker` = `KiteConnect(api_key)` + `set_access_token(store.get_access_token())`; `authenticate()` verifies via `profile()` inside `container.start` (fail fast).
- **`token_manager.py`, `kite_auth.py`** — the flow orchestration; **never automate password/TOTP** (login is interactive, in the browser).

---

## 3. The three websockets

| # | Class / file | Feed | Mode | Consumer |
|---|---|---|---|---|
| 1 | `brokers/kite/market_ticker.py::KiteTickStream` | trading **price** ticks | LTP | `MarketDataService` → `dispatch_tick` → `PositionMonitor.on_tick` |
| 2 | `brokers/kite/websocket.py::KiteOrderUpdateStream` | trading **order updates** | postbacks | `OrderUpdateProcessor.process` → `orders` table |
| 3 | `market_data_collector/full_tick_stream.py::CollectorTickStream` | collector **FULL** ticks | FULL | `TickWriter` → TimescaleDB |

All three wrap `kiteconnect.KiteTicker`, connect via `ticker.connect(threaded=True)` (Twisted reactor thread — **one reactor per process**), and expose the same callbacks: `on_connect/on_ticks/on_close/on_error/on_reconnect` (collector adds `on_noreconnect`; order stream uses `on_order_update`).

- **KiteTickStream** — `subscribe(instruments)` maps `InstrumentIdentifier`→token (under `_lock`); `_handle_connect` re-subscribes the tracked set + `set_mode`; `_to_tick` maps raw → `Tick(last_price)`.
- **KiteOrderUpdateStream** — `register_callback` appends under `_callbacks_lock`; `_on_order_update` maps the postback → `BrokerOrder`, copies the callback list, **releases the lock**, then invokes each callback (a callback may re-enter the broker — never hold the lock across it).
- **CollectorTickStream** — see collector section; adds `_dead` Event, `is_dead()`, `restart()` (destroy + brand-new `KiteTicker`, preserve subscriptions), per-tick `try/except` drop.

### Why websocket (vs REST polling)
Ticks are pushed sub-second, event-driven, high-frequency. REST polling adds per-request latency, burns rate limits, and misses intra-poll moves. **Disadvantages:** stateful (can drop → reconnect/watchdog), delivered on a background thread (→ thread-safety), no gap replay. The trading feed keeps a **polling fallback** (`PositionMonitor.poll_and_check` → `broker.get_ltp`) for resilience; **the collector has no polling fallback** — its resilience is reconnect + watchdog.

---

## 4. Market-data collector (`src/algo/market_data_collector/`)

**Separate process, separate DB (TimescaleDB).** Orchestrated by `collector_service.py::CollectorService`.

| File | Role |
|---|---|
| `config.py` | `CollectorConfig` (+ `WriterConfig`, `ReconnectConfig`, `MarketHoursConfig`, `MetricsConfig`, `TimescaleConfig`) |
| `db.py` | `build_market_data_engine`, `option_ticks`/`collector_instruments` Core tables, `MarketTick` dataclass, `TICK_COLUMNS` |
| `init_timescale.py` | `initialize_schema` — create tables + hypertable + compression + continuous aggregates (idempotent; degrades to plain table if Timescale absent) |
| `instrument_chain.py` | `KiteMarketReader` — `spot_ltp`, `option_chain` (Kite dump), lock-guarded caches |
| `atm_window.py` | `AtmWindowManager.recompute` — ATM ± N strikes × {CE,PE}, edge-diff vs current window → `(to_subscribe, to_unsubscribe, new_instruments)` |
| `full_tick_stream.py` | `CollectorTickStream` — the FULL websocket (watchdog-managed) |
| `tick_writer.py` | `TickWriter` — bounded deque + batch COPY writer thread |
| `market_hours.py` | `MarketHoursController` — session phases (CONNECT/COLLECT/FREEZE/FLUSH/CLOSED), holiday-aware |
| `metrics.py` | `CollectorMetrics` — status block (Connected, Subscribed, Ticks, Rows, Queue, Restart Count, Since Last Tick, …) |
| `collector_service.py` | orchestrator: controller thread (`_run`), `_watchdog`, `_recompute_all`, phase transitions |

### Threading & tick flow
```
[KiteTicker reactor] on_ticks → MarketTick → TickWriter.enqueue  (O(1), never blocks, _lock)
                                                   ▼ bounded deque
[collector-writer] _run: _flush.wait(interval)/full-batch → _take_batch(batch_size) → COPY option_ticks → Timescale
[collector-controller] _run: MarketHoursController.phase → CONNECT/COLLECT/FREEZE/… ; _recompute_all ; _watchdog ; metrics
```

### Watchdog & reconnect (`collector_service._watchdog` + `full_tick_stream`)
- `KiteTicker` auto-reconnects (default-on, ~50 tries, exp backoff).
- The controller-loop watchdog: if `is_connected()` stays false past `reconnect.grace_seconds`, **or** `is_dead()` (`on_noreconnect` fired), it calls `CollectorTickStream.restart` → **destroy the dead `KiteTicker`, build a brand-new one** (factory re-reads the token = fresh auth), preserving `_subscribed` → resubscribe on connect. Heartbeat (`heartbeat_warning/critical_seconds`) catches a connected-but-silent reactor. Restart is backoff-throttled (`restart_backoff_seconds`).

### Writer (queue) details
- Bounded `deque` under `_lock`; `enqueue` overflow → `drop_oldest`(popleft)/`drop_newest`(skip) + `dropped_ticks`; full batch → `_flush.set()`.
- `_write_batch` → `_copy_batch` (Postgres: `COPY option_ticks (...) FROM STDIN WITH (FORMAT csv, NULL '')` via `copy_expert`, depth JSON-encoded) or `_insert_batch` (`executemany`, SQLite/`use_copy=false`).
- `_write_with_retry` → capped exponential backoff (`db_retry_base→max`); during shutdown, gives up after 3 and counts loss.

---

## 5. Configuration files (in depth)

*(Every value: meaning · allowed · default · impact · reader. Full list; defaults are pydantic defaults where present, else "required".)*

### `configs/strategies/strategy_1/<instrument>.yaml` → `Strategy1Config`
| Key | Meaning | Allowed | Default | Impact | Reader |
|---|---|---|---|---|---|
| `entry_time` | IST entry trigger time | HH:MM:SS | required | when to enter | scheduler/entry |
| `hard_cutoff_time` | IST forced-exit time | HH:MM:SS, > entry | required | latest exit | exit_logic |
| `target_pct` | fractional target | 0<x<1 | required | target = entry×(1−x) | entry/exit |
| `sl_pct` | fractional stoploss | 0<x<1 | required | stoploss = entry×(1+x) | entry/exit |
| `lots` | lots/leg | >0 | required | qty = lots×lot_size | entry |
| `product_type` | broker product | INTRADAY/NORMAL | required | margin/settlement | orders |
| `skip_on_expiry_day` | skip 0DTE | bool | required | avoids expiry-day entry | entry |
| `monitoring_interval_seconds` | heartbeat cadence | >0 | required | max exit latency w/o ticks | monitoring scheduler |
| `polling_interval_seconds` | stale-feed cadence + staleness threshold | >0, ≤ monitoring | required | fallback poll | monitor |
| `retry.order_timeout_seconds` | per broker-call timeout | >0 or null | required (null ok) | call timeout | entry/exit |
| `retry.fill_confirmation_attempts` | max get_order polls | >0 | required | ambiguity threshold | entry/exit |
| `retry.fill_confirmation_delay_seconds` | poll delay | >0 | required | poll spacing | entry/exit |
| `retry.close_retry_attempts` | exit resend attempts | >0 | required | never-sent resend | exit |
| `retry.close_retry_delay_seconds` | close retry delay | >0 | required | resend spacing | exit |

### `configs/instruments/<instrument>.yaml` → `InstrumentConfig`
`exchange`(NFO/BFO), `strike_interval`(>0), `lot_size`(>0), `tick_size`(>0), `spot_exchange`(NSE/BSE), `spot_symbol`(e.g. "NIFTY 50"), `expiry_weekday`(0=Mon..6=Sun), `expiry_cadence`(weekly[default]/monthly), `underlying_symbol`(optional; e.g. SENSEXBANK→BANKEX).

### `configs/collector.yaml` → `CollectorConfig`
`db_url_env`, `underlyings`(list), `strikes_each_side`(>0), `tick_mode`(FULL/QUOTE/LTP), `recompute_interval_seconds`(>0), `writer{batch_size,flush_interval_ms,queue_max,overflow_policy(drop_oldest/drop_newest),use_copy,db_retry_base_seconds,db_retry_max_seconds}`, `reconnect{grace_seconds=60,restart_backoff_seconds=5,heartbeat_warning_seconds=20,heartbeat_critical_seconds=60}` (critical≥warning), `market_hours{connect,start,stop_subscribe,flush,disconnect,idle_poll_seconds}`, `metrics{interval_seconds=60}`, `timescale{chunk_interval,compression_after,retention,continuous_aggregates}`.

### `configs/holidays.yaml` → `HolidayService`
`holidays: {NSE:[ISO dates], BSE:[ISO dates]}`. Service maps NFO→NSE, BFO→BSE. **Static list, no auto-calc; fails open on unknown years** (see SUMMARY.md decisions). `is_trading_day(day,exchange)` = weekday AND not in list.

### `configs/app.yaml`, `brokers.yaml`, `risk.yaml`, `accounts.yaml`, `database.yaml`, `market_data.yaml`
See SUMMARY.md §7 table. `risk.yaml`: `margin_per_lot_by_instrument`(exact-match dict, UPPERCASE keys), `daily_loss_limit_by_account`, `max_daily_entries_per_account`.

### `.env` (secrets)
`DATABASE_URL`, `MARKET_DATA_DATABASE_URL`, `KITE_API_KEY`, `KITE_API_SECRET`, `KITE_ACCESS_TOKEN`, `KITE_ACCESS_TOKEN_GENERATED_AT`, optional `CONFIG_DIR`, `DB_AUTO_MIGRATE`, `I_UNDERSTAND_THIS_TRADES_REAL_MONEY`, `MIGRATION_TEST_DATABASE_URL`(tests).

---

## 6. Expiry resolution (`services/live_seams.py::ConfigExpiryService`)

`get_current_weekly_expiry(instrument, as_of)`:
1. Read `expiry_weekday`, `expiry_cadence`, `exchange` from the instrument config.
2. **Weekly:** `days_ahead = (weekday − as_of.weekday()) % 7; expiry = as_of + days_ahead` (same-day if today is the expiry weekday).
   **Monthly:** last `expiry_weekday` of the month (`_last_weekday_of_month`); roll to next month if passed (`_monthly_expiry_on_or_after`, Dec→Jan handled).
3. **Holiday shift:** `while not HolidayService.is_trading_day(expiry, exchange) and guard<14: expiry -= 1` (backward to previous trading day); logs if shifted.

Per instrument: NIFTY(NFO→NSE, Tue, weekly), SENSEX(BFO→BSE, Thu, weekly), BANKNIFTY/FINNIFTY/MIDCPNIFTY(NFO→NSE, Tue, monthly), SENSEXBANK(BFO→BSE, Thu, monthly). The result is stored on `Position.expiry_date` and **verified against the broker's resolved contract** (`StrikeSelector._validate_resolved_contract`).

---

## 7. Logging (detail)

- `logging/logger.py::configure_logging(level, alert_dispatcher, log_format, stream_handler)` — resets root handlers, one timestamped stderr `StreamHandler`, sets `algo` logger level, attaches `AlertingHandler` (forwards CRITICAL+ to the dispatcher). Idempotent.
- `logging/alerting.py` — `AlertDispatcher` protocol + `AlertingHandler` + `RecordingAlertDispatcher` (**records, does not send** — no live webhook/pager: *not implemented*).
- `logging/strategy_logger.py::PositionMonitorLogger` — the operator-facing periodic "monitoring" block and the "POSITION CLOSED" summary (Total P&L, P&L Per Share, etc.); purely observational, exception-isolated.
- Loggers: `algo.collector`, `algo.collector.writer`, `algo.collector.init`, `algo.expiry`, `algo.holidays`, `algo.migration_guard`, `algo.risk`, `algo.scheduler`, `algo.app`, `algo.trade_history`, `algo.reporting`, `algo.start_*`.
- **CRITICAL messages** (the alert stream): partial entry / naked exposure, BROKER/DB DIVERGENCE, watchdog restart, `on_noreconnect`, schema out of date, DB unreachable during shutdown, dispatch raised (should have been contained).

---

## 8. Error/retry summary (cross-reference)
- Retries are **hand-rolled** (no generic decorator; `common/decorators.py` is a TODO). Bounded budgets from config: fill-confirmation, exit-close resend (never-sent only), writer DB backoff, rate-limiter waits, websocket reconnect + watchdog.
- **No `asyncio`.** Concurrency is thread-based (kiteconnect uses Twisted internally). See SUMMARY_ARCHITECTURE §Threading.
