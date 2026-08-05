# Setup & Handover

Practical guide to deploy and run this platform on a fresh machine / new AWS,
GitHub, and Kite account. Follow top to bottom.

All commands assume you are in the `algo_platform/` folder (the one with
`pyproject.toml`).

> Note on entrypoints: the real programs are Python modules (`python -m algo.*`).
> Some files under `scripts/` (`start_live.py`, `start_paper.py`,
> `sync_instruments.py`) are stubs — do **not** run them. Use the commands in
> this doc.

---

## 1. Project Requirements

- **Python:** 3.11 or newer (`python --version`).
- **OS:** Linux (Ubuntu 22.04/24.04 for AWS) or Windows/macOS for dev. Built and tested on all three.
- **Required software:**
  - Git
  - PostgreSQL 13+ (this platform does not use SQLite in production)
  - `tmux` (to keep the process running after you disconnect)
- **Required services:**
  - A PostgreSQL database (local on the box, or AWS RDS)
- **Required accounts:**
  - Zerodha **Kite Connect** developer app (gives you API key + secret). Costs ₹2000/month.
  - An AWS account (for the EC2 box)
  - A GitHub account (only if you host your own copy of the code)

---

## 2. First Time Setup

### Clone
```
git clone <your-repo-url>
cd "trading project/algo_platform"
```

### Virtual environment
Linux/macOS:
```
python3 -m venv venv
source venv/bin/activate
```
Windows (PowerShell):
```
python -m venv venv
.\venv\Scripts\Activate.ps1
```
Prompt should now show `(venv)`.

### Install dependencies
```
pip install --upgrade pip
pip install -e ".[dev]"
```
(`[dev]` adds pytest. All runtime deps are pinned in `pyproject.toml`.)

### Environment variables
Create a `.env` file in the project root:
```
DATABASE_URL=postgresql+psycopg2://algo_user:YOUR_PASSWORD@localhost:5432/algo
KITE_API_KEY=your_api_key
KITE_API_SECRET=your_api_secret
KITE_ACCESS_TOKEN=
# Optional:
# CONFIG_DIR=configs
# MARKET_DATA_DATABASE_URL=postgresql+psycopg2://...   # only for the data collector
```
- Leave `KITE_ACCESS_TOKEN` blank — `generate_token.py` fills it every morning (Section 5).
- The process reads `.env` automatically (via `load_dotenv()` in the DB loader). **Do not `export` the Kite variables in your shell** — an exported value overrides `.env` and blocks the daily token update.

### Database
See Section 6.

### Instrument dump
Nothing to run. The platform downloads the Kite instrument master automatically the first time it needs it and **refreshes it once per trading day**. A valid Kite login (Section 5) is all that's required.

### Verify the install
```
alembic upgrade head        # create tables (Section 6)
pytest src/algo/tests -q     # should be all green
```

---

## 3. AWS Setup

The platform runs **no web server** — it only makes outbound calls to Kite and Postgres. So the box needs no inbound app ports.

### EC2 instance
- **AMI:** Ubuntu Server 22.04 or 24.04 LTS (x86_64).
- **Type:** the project does not mandate one. Recommended minimum **t3.small** (2 vCPU / 2 GB); **t3.medium** (4 GB) is comfortable, especially if Postgres runs on the same box. Workload is light (6 threaded strategy instances).
- **Storage:** 20 GB gp3 is plenty (logs + the daily Excel reports + Postgres if local).

### Security Group
- **Inbound:** SSH (TCP **22**) from your IP only. Nothing else.
- **Outbound:** allow all (default). The platform needs outbound **443** (Kite REST + websocket) and **5432** (Postgres, only if using remote/RDS).

### SSH in
```
ssh -i your-key.pem ubuntu@<ec2-public-ip>
```

### Install prerequisites
```
sudo apt update
sudo apt install -y python3-venv python3-pip git tmux postgresql postgresql-contrib
```
(Skip `postgresql` here if you use RDS.)

### Upload the project
Preferred — clone from GitHub:
```
git clone <your-repo-url>
cd "trading project/algo_platform"
```
Or copy from your laptop:
```
scp -i your-key.pem -r ./algo_platform ubuntu@<ec2-public-ip>:~/
```

### venv + requirements + DB + run
```
python3 -m venv venv && source venv/bin/activate
pip install --upgrade pip && pip install -e ".[dev]"
# create .env (Section 2), create DB + run migrations (Section 6)
alembic upgrade head
```
Then run it inside tmux — see Section 8.

---

## 4. GitHub Change (use your own repo)

From inside `algo_platform/`:
```
git remote -v                                   # see the current 'origin'
git remote remove origin                        # remove the old one
git remote add origin git@github.com:<you>/<your-repo>.git
git push -u origin main                         # push (use your branch name if not 'main')
git remote -v                                   # verify origin now points to you
```
Before pushing: make sure `.env` is **not** committed. Add it to `.gitignore` first (see Project Improvement).

---

## 5. Kite Setup (switch to another person's Kite account)

Kite issues a **new access token every day**; API key/secret are permanent per app.

### One-time (new account)
1. Create a Kite Connect app at the Kite developer console. Note its **API key** and **API secret**.
2. Put them in `.env`:
   ```
   KITE_API_KEY=...
   KITE_API_SECRET=...
   ```
3. Update the broker client id if you track it: `configs/accounts.yaml` → `broker_client_id`.

### Every trading morning (daily login)
```
python scripts/generate_token.py
```
- It opens the official Kite login page (prints the URL if it can't open a browser — on a headless EC2, open the URL on your laptop, log in, then paste the redirect URL back into the SSH prompt).
- On success it writes `KITE_ACCESS_TOKEN` into `.env`.
- Add `--force` to re-login even if today's token still looks valid.

### Instrument dump
Automatic — see Section 2. No manual step.

### Restart required?
- If the platform is **not** running yet: just start it (Section 8); it reads the fresh token.
- If it is **already** running: stop it and start again. The token is read at startup only; it is **not** hot-reloaded.

### Files that change per account
- `.env` — `KITE_API_KEY`, `KITE_API_SECRET`, `KITE_ACCESS_TOKEN`.
- `configs/accounts.yaml` — `broker_client_id`, `display_name`.

---

## 6. Database Setup

### Requirements
- PostgreSQL 13+. One database, one user.
- The connection string (with credentials) comes from `DATABASE_URL` in `.env`.
- `configs/database.yaml` holds only non-secret pool tuning (safe to leave as-is).

### Create the database + user
```sql
CREATE DATABASE algo;
CREATE USER algo_user WITH PASSWORD 'choose-a-password';
GRANT ALL PRIVILEGES ON DATABASE algo TO algo_user;
```
Then set `DATABASE_URL` in `.env` to match (Section 2).

### Migrations
```
alembic upgrade head
```
This creates every table. The trading process also **checks** the schema at startup and refuses to start if it is behind (it does not auto-migrate) — so run this after every code update that adds a migration.

### Verify
```
psql "$DATABASE_URL" -c "\dt"      # should list positions, orders, accounts, etc.
```

---

## 7. Configuration Files

All under `configs/`.

- **`brokers.yaml`** — which broker + rate limits.
  - Edit when going live: set `active_broker: KITE` (committed default is `SIMULATION`). Live mode refuses to start unless this is `KITE`.
  - Rate-limit values are placeholders — confirm against Kite's published limits before live.
- **`app.yaml`** — which (strategy, instrument, account) combos run.
  - Edit to add/remove instruments or change the account name. `log_level` lives here too.
- **`accounts.yaml`** — account name, broker, `broker_client_id`, display name.
  - Edit when switching Kite account.
- **`risk.yaml`** — market hours, daily loss limit, per-lot margin estimates, max entries/day.
  - Edit before live: these are placeholders and must be signed off. `max_daily_entries_per_account` must be ≥ number of instruments.
- **`configs/instruments/*.yaml`** — per-index static specs: exchange, strike interval, lot size, spot symbol, **expiry weekday**, expiry cadence (weekly/monthly).
  - Edit when an exchange changes lot size / strike interval / expiry rules. **The expiry weekday and cadence are the most failure-prone values** — verify against the live dump before each series (see Common Problems).
- **`configs/strategies/strategy_1/*.yaml`** — per-instrument strategy params: `entry_time`, `hard_cutoff_time`, `target_pct`, `sl_pct`, `lots`, `product_type`, `skip_on_expiry_day`.
  - Edit to change entry/exit times, sizing, or targets. **Set `hard_cutoff_time` before the broker's intraday auto-square-off** (see Before Live Checklist).
- **`configs/holidays.yaml`** — NSE/BSE trading holidays.
  - Edit each year and verify movable-festival dates against the official circular.
- **`configs/market_data.yaml`** — websocket freshness + poll fallback tuning. Rarely changed.
- **`configs/collector.yaml`** — optional market-data collector only. Ignore unless you run the collector.

---

## 8. Daily Startup

On the EC2 box, each trading morning:

```
cd "trading project/algo_platform"
source venv/bin/activate

# 1. Fresh Kite token (live only)
python scripts/generate_token.py

# 2. Start the trading engine in tmux, tee logs to a file
mkdir -p logs
tmux new -s trading
export I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes
python -m algo.start_live 2>&1 | tee -a logs/live_$(date +%F).log
# detach from tmux: Ctrl-b then d
```

Paper mode (no real orders, for testing):
```
python -m algo.start_paper 2>&1 | tee -a logs/paper_$(date +%F).log
```

Optional — the research data collector (separate process, needs `MARKET_DATA_DATABASE_URL` + a TimescaleDB, and `python -m algo.start_collector --init-only` once to create its schema):
```
tmux new -s collector
python -m algo.start_collector 2>&1 | tee -a logs/collector_$(date +%F).log
```

### Health checks (after startup)
- Logs show, in order: `starting platform` → `reconciliation complete` → `strategy instance ... is RUNNING` (one per instrument) → `platform started with N strategy instance(s)`.
- `python -m algo.killswitch status` → should print "No active control flags."
- `python -m algo.instance_admin list` → every instance should be `ACTIVE` (not `FROZEN`).
- Instrument sync is not required — the dump refreshes itself once per day.

---

## 9. Daily Shutdown

```
tmux attach -s trading
# press Ctrl-C  (or: kill the process with SIGTERM)
```
- Wait for `platform stopped` in the log, then `tmux kill-session -t trading`.
- Shutdown is graceful (timers, strategies, broker, DB pool all close cleanly).
- **It does NOT close open positions.** An open straddle survives shutdown and is resumed on the next start. To actually flatten, use the kill switch (Section 14) or the broker directly.

---

## 10. Log Files

- **Where:** the platform logs to **stdout/stderr only** — there is no built-in log file. You get a file only because Section 8 pipes through `tee` into `logs/`. Always start with `tee`, or logs are lost on tmux/session death.
- **Which to check first:** today's `logs/live_YYYY-MM-DD.log`.
- **What to grep:**
  - `grep CRITICAL logs/live_*.log` — always investigate.
  - `grep -E "froze|FROZEN" logs/*.log` — an instrument parked for manual review.
  - `grep "reconciliation break" logs/*.log` — a state the platform couldn't safely resolve.
  - `grep "entry rejected" logs/*.log` — a risk check blocked entry (often normal).

---

## 11. Common Problems

**`DATABASE_URL is not set`**
- Symptom: process refuses to start.
- Reason: `.env` missing/not in project root, or the variable name is wrong.
- Fix: create `.env` in `algo_platform/` with a valid `DATABASE_URL`.

**`no Kite access token` / auth fails at startup**
- Symptom: startup fails on broker auth.
- Reason: no token minted today, or you `export`ed `KITE_ACCESS_TOKEN` in the shell (which blocks `.env` from updating it — `load_dotenv` never overrides an existing env var).
- Fix: run `python scripts/generate_token.py`; make sure the Kite vars are only in `.env`, not exported.

**Live mode refuses: "expected active_broker=KITE but brokers.yaml selects SIMULATION"**
- Fix: set `active_broker: KITE` in `configs/brokers.yaml`.

**Live mode refuses: confirmation not set**
- Fix: `export I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes` before `python -m algo.start_live`.

**An instrument freezes before entry (e.g. SENSEX not trading)**
- Symptom: log shows `... is NOT listed on the exchange` (or `InstrumentNotFoundError`), then the instance is `FROZEN`.
- Reason: the computed expiry (from `configs/instruments/<name>.yaml`'s `expiry_weekday`/`expiry_cadence`) doesn't match what the exchange actually lists — the exchange changed the weekly day, or moved the index to monthly-only.
- Fix: check the live listed expiries, correct `expiry_weekday` (or set `expiry_cadence: monthly`) in that instrument's YAML, restart, then unfreeze the instance (Section 14).

**`alembic upgrade head` won't connect**
- Reason: wrong `DATABASE_URL`, Postgres not running, or user lacks access.
- Fix: test with `psql "$DATABASE_URL"` first.

**Schema-out-of-date on startup**
- Reason: code has a newer migration than the DB.
- Fix: run `alembic upgrade head`.

---

## 12. Updating the Project

```
tmux attach -s trading      # stop the running process first (Ctrl-C)
git pull
source venv/bin/activate
pip install -e ".[dev]"      # in case dependencies changed
alembic upgrade head          # in case a migration was added
pytest src/algo/tests -q      # confirm green before trading
```
Then start again (Section 8). Never `git pull` into a running trading process — stop it first.

---

## 13. Before Live Trading Checklist

Verify every trading day:

- [ ] EC2 instance running, SSH works
- [ ] `venv` activated
- [ ] `.env` has a valid `DATABASE_URL` and the correct Kite API key/secret
- [ ] `python scripts/generate_token.py` run **today** (fresh access token in `.env`)
- [ ] `configs/brokers.yaml` → `active_broker: KITE`
- [ ] `I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes` exported
- [ ] `configs/instruments/*.yaml` expiry weekday/cadence verified against the current listed contracts
- [ ] `hard_cutoff_time` in `configs/strategies/strategy_1/*.yaml` is **before** Kite's intraday (MIS) auto-square-off — not equal to market close
- [ ] `risk.yaml` limits (margin per lot, daily loss limit) confirmed for real sizing
- [ ] `alembic upgrade head` applied (schema current)
- [ ] `python -m algo.killswitch status` → no active flags
- [ ] `python -m algo.instance_admin list` → all instances `ACTIVE`
- [ ] Startup logs reached `platform started with N strategy instance(s)`

---

## 14. Emergency Recovery

**Halt a running platform now (flatten open positions safely):**
```
python -m algo.killswitch engage --reason "manual halt" --by yourname
python -m algo.killswitch status         # confirm it's engaged
# ... later, to resume:
python -m algo.killswitch disengage --by yourname
```
Engaging sets a durable flag the running process picks up within ~2s; it blocks new entries and closes open positions via the normal exit path. Survives restarts.

**Process crashed — just restart it:**
```
python -m algo.start_live 2>&1 | tee -a logs/live_$(date +%F).log
```
On start it runs reconciliation (compares DB vs broker) and each instrument recovers from the DB — open positions resume monitoring, interrupted exits finish. **Read the startup logs** to see what reconciliation did.

**Unfreeze an instrument (only after you understand why it froze):**
```
python -m algo.instance_admin list
python -m algo.instance_admin unfreeze --instance-id <N> --reason "confirmed & resolved: <what you fixed>" --by yourname
```

**When NOT to restart / unfreeze blindly:**
- If positions are open and you don't know the real broker state — check the broker first. Restart is safe (recovery reconciles), but never clear a freeze without knowing what caused it. A freeze means "stop and look," not "retry."

---

## 15. Files Worth Knowing

- `src/algo/start_live.py` / `start_paper.py` → real entrypoints (`python -m algo.start_live` / `start_paper`)
- `src/algo/dependency_container.py` → wires the whole platform together at startup
- `src/algo/strategy_engine/strategies/strategy_1/` → the short-straddle strategy (entry, exit, monitor, strike/expiry selection)
- `src/algo/services/live_seams.py` → instrument/expiry/spot config-backed services (+ expiry validation)
- `src/algo/brokers/kite/` → Kite broker, order/tick websockets, auth/token, mapping
- `src/algo/services/reconciliation_engine.py` → crash-recovery reconciliation against broker truth
- `src/algo/risk/` → risk checks, kill switch, daily loss limit
- `src/algo/database/` → models, repositories, migrations (`alembic`)
- `configs/` → all runtime configuration (Section 7)
- `scripts/generate_token.py` → daily Kite login
- `src/algo/killswitch.py` → kill-switch CLI (`python -m algo.killswitch`)
- `src/algo/instance_admin.py` → unfreeze CLI (`python -m algo.instance_admin`)
- `docs/` → older design/ops notes (some predate the current code — trust this file for setup)

---

## Changing Broker (Kite → Dhan / XTS / Kotak Neo / Fyers / AliceBlue / etc.)

### 1. Current broker architecture
- **Interface:** `src/algo/brokers/broker_base.py` — abstract `BrokerBase`. Every method the platform uses (orders, positions, LTP, quote, instrument lookup, websocket lifecycle) is declared here. Strategy/risk/execution code depends only on this.
- **Implementation:** `src/algo/brokers/kite/` implements it for Kite: `kite_broker.py` (the broker), `mapper.py` (request/response/error mapping), `kite_auth.py` (session/token), `websocket.py` (order-update push), `market_ticker.py` (market-tick push).
- **Exceptions:** concrete brokers translate their SDK errors into `src/algo/brokers/exceptions.py`. Callers catch only these types, never a broker SDK's own exceptions.
- **Rate limiting:** every broker is wrapped in `RateLimitedBroker` (`rate_limiter.py`) — broker-agnostic.
- **Dependency injection:** `DependencyContainer` builds the broker (`_build_broker` → `_build_kite_broker`), wraps it in `RateLimitedBroker`, and hands it to `InstanceFactory`, which puts it on every `StrategyContext`.
- **Where Strategy1 gets the broker:** off `self.context.broker` (a `BrokerBase`). It never imports Kite.
- **Why strategy code doesn't change:** the interface + DI fully isolate the concrete broker. Swap the implementation behind `BrokerBase` and everything above it is untouched.

### 2. Files to replace / change / create
```
src/algo/brokers/kite/                → copy to brokers/<newbroker>/ and reimplement:
    kite_broker.py                    → new <NewBroker>(BrokerBase)
    mapper.py                         → new request/response/EXCEPTION mapping
    kite_auth.py                      → new auth (only if the broker needs a session/token)
    websocket.py                      → new order-update stream
    market_ticker.py                  → new market-tick stream (TickStream)
    token_store.py / token_manager.py → only if the broker uses a daily token

src/algo/brokers/broker_base.py           → No change required.
src/algo/brokers/exceptions.py            → No change required (reuse the vocabulary).
src/algo/brokers/rate_limiter.py          → No change required (wraps any BrokerBase).
src/algo/market_data/websocket_manager.py → No change required (TickStream/LtpPoller seams).

src/algo/common/enums.py            → add a BrokerName value (e.g. DHAN).
src/algo/dependency_container.py    → add <NewBroker>Settings, an elif in _build_broker, and _build_<newbroker>_broker().
src/algo/services/live_seams.py     → replace build_kite_tick_stream + KiteInstrumentTokenMap for the new broker.
src/algo/start_live.py              → build_seams(): wire the new tick_stream (+ token store).
src/algo/start_paper.py             → build_seams(): same, plus the paper data broker below.
src/algo/services/paper_trading_seams.py → build the paper-mode data broker from the new broker (or keep Kite for data only).
scripts/generate_token.py           → replace/adjust only if the new broker has a daily interactive login.

configs/brokers.yaml                → add active_broker: <NEW> + a config block.
configs/accounts.yaml               → set broker: <NEW> for the account.
.env                                → new broker's API credentials.
```

### 3. New broker implementation
Implement every abstract method on `BrokerBase` (the platform calls all of them):
- Lifecycle: `authenticate`, `is_authenticated`, `close`, `health_check`
- Orders: `place_order`, `modify_order`, `cancel_order`, `get_order`, `get_orders`, `find_order_by_tag`
- Portfolio: `get_positions`, `get_holdings`, `get_margins`
- Market data (pull): `get_quote`, `get_ltp` (must be **batched** — one call for many instruments)
- Instruments: `get_instrument`, `find_option_contract`
- Order-update websocket: `connect_websocket`, `disconnect_websocket`, `is_websocket_connected`, `register_order_update_callback`
- `broker_name` property
- `list_option_expiries` — optional (default returns `None` → expiry validation skipped). Implement it to keep the "expiry not listed" safety check working.

Non-negotiable mapping rules (copy the shape from `kite/mapper.py`):
- Translate SDK errors into `brokers/exceptions.py`. **Critical split:** a mutating call (place/modify/cancel) that times out → `BrokerTimeoutError` (outcome UNKNOWN, never auto-retried); a "never left" error → `BrokerConnectionError`; a definitive rejection → `OrderRejectedError` / `InvalidOrderRequestError`. Getting this wrong causes duplicate orders.
- `place_order` returns only `PlaceOrderResult(broker_order_id)` — status is unknown synchronously.
- `get_order` returns `BrokerOrder`; report partial fills as `status=OPEN` with `0 < filled_quantity < quantity`; `average_price` must be **None until filled** (map the broker's "0 before fill" to None — the money path rejects a 0 fill price).
- `place_order` must pass the caller's `tag` (idempotency key, ≤20 chars) through unchanged so `find_order_by_tag` can reconcile after an ambiguous timeout.

### 4. Dependency injection changes (`dependency_container.py`)
1. Add `<NewBroker>Settings(BaseModel)` mirroring `KiteBrokerSettings` (env-var names + timeouts).
2. Add it to `BrokersConfig`.
3. In `_build_broker`, add `elif config.active_broker is BrokerName.<NEW>: inner = self._build_<newbroker>_broker(config.<new>)`.
4. Write `_build_<newbroker>_broker()` — construct the SDK client, auth/session, order-update stream, and return the `BrokerBase` (mirror `_build_kite_broker`, ~lines 640–681). Everything after (the `RateLimitedBroker` wrap) is unchanged.
- Note: the KITE branch requires an injected `AccessTokenStore`. If the new broker has no daily token, drop that requirement in your branch.

### 5. Config changes
- `configs/brokers.yaml`: set `active_broker: <NEW>`, add a `<new>:` block (env-var names + timeouts). Keep `rate_limits:` (set to the new broker's real limits).
- `configs/accounts.yaml`: `broker: <NEW>`, correct `broker_client_id`.
- `.env`: the new broker's key/secret/token vars (names must match the `<new>:` block).
- Nothing else — instruments/strategies/risk/holidays are broker-agnostic (but review symbols, see §6).

### 6. Instrument mapping
- Today: `kite_broker.find_option_contract` / `get_instrument` / `list_option_expiries` scan the Kite instrument dump; `mapper.to_broker_instrument` shapes a row; `live_seams.KiteInstrumentTokenMap` maps symbol ↔ numeric token for the tick websocket.
- New broker: reimplement those three lookups against the new broker's instrument master, and reimplement the symbol↔token map for its ticks.
- Symbol/segment formats differ per broker. Map the `Exchange` enum (NFO/BFO/NSE/BSE) to the new broker's segment codes in your mapper, and reconcile contract naming with `configs/instruments/*.yaml` (`spot_symbol`, `underlying_symbol`, `exchange`).
- `src/algo/market_data/instrument_mapper.py` is **not** used in the live path — ignore it.

### 7. Websocket (market ticks)
Replace `brokers/kite/market_ticker.py` with a class satisfying the `TickStream` Protocol (`market_data/websocket_manager.py`): `set_handlers`, `start`, `stop`, `is_connected`, `subscribe`/`unsubscribe` (by `InstrumentIdentifier`).
- Convert each raw tick into the platform `Tick(instrument, last_price, timestamp)`.
- Handle reconnect internally and call `on_reconnect` so the service re-subscribes.
- Map `InstrumentIdentifier` → broker token/symbol for subscribe.
- Wire it in `start_live.py` / `start_paper.py` `build_seams()` (replace `build_kite_tick_stream`).
- The pull fallback (`LtpPoller`) is satisfied by the broker's own `get_ltp` — no extra work.

### 8. Order updates
- Replace `brokers/kite/websocket.py` (`KiteOrderUpdateStream`) with the new broker's push feed: map each push into a platform `BrokerOrder` and fan it out to registered callbacks.
- The **consumer side is unchanged**: the container calls `broker.register_order_update_callback(order_update_processor.process)`, and `OrderUpdateProcessor` works on `BrokerOrder` DTOs. No change to the processor, entry/exit, or reconciliation.
- If the new broker has no order-update push, implement the lifecycle methods as no-ops — fills are still caught by the polling path.

### 9. Data collector
- **No change required** to swap the *trading* broker. `src/algo/market_data_collector/` is a separate process with its **own** Kite websocket (`full_tick_stream.py`) and does not use the trading broker at all.
- It is itself Kite-coupled. Only if you also want the collector to record data from the new broker do you touch `market_data_collector/full_tick_stream.py` (+ its token map) — a separate task, unrelated to trading.

### 10. Strategy code
- `Strategy1` — **No change required.** Uses `context.broker` (BrokerBase) only.
- `EntryLogic` — **No change required.** Calls `place_order` / `get_order` / `find_order_by_tag` on the interface.
- `ExitLogic` — **No change required.**
- `PositionMonitor` — **No change required.** Uses `context.market_data` + `spot_price_provider`.
- `StrategyRunner` — **No change required.**
- Schedulers (platform + monitoring) — **No change required.**
- `RiskCore` — **No change required.** Uses `broker.get_margins` / `get_orders` via the interface.
- State machine — **No change required.** Pure DB logic, no broker.
- `reconciliation_engine` — **No change required.** Uses `broker.get_orders` / `get_positions` via the interface.
- `StrikeSelector` — **No change required.** Calls `broker.find_option_contract`.

The only new-broker dependency in strategy code is the **exception contract** (§3): raise the right `brokers/exceptions.py` types and the strategy behaves identically.

### 11. Migration checklist
```
□ Add BrokerName.<NEW> in common/enums.py
□ Create src/algo/brokers/<newbroker>/ package
□ Implement <NewBroker>(BrokerBase) — all abstract methods
□ Implement the mapper (requests, responses, EXCEPTION split)
□ Implement instrument resolution (find_option_contract / get_instrument / list_option_expiries)
□ Implement the market-tick TickStream + symbol↔token map
□ Implement the order-update websocket (BrokerOrder mapping)
□ Implement auth (session/token) if the broker needs it
□ Add <NewBroker>Settings + BrokersConfig + _build_<newbroker>_broker in dependency_container
□ Wire tick_stream (+ paper data broker) in start_live.py / start_paper.py build_seams
□ Update configs/brokers.yaml (active_broker + block + rate_limits) and accounts.yaml
□ Add API credentials to .env
□ pytest src/algo/tests -q  (broker-agnostic tests must stay green)
□ Test paper mode end-to-end (entry → monitor → SL/target → exit)
□ Test order-update push (a fill arrives via register_order_update_callback)
□ Test crash recovery (restart mid-cycle → reconciliation repairs)
□ Test live with 1 lot, watched closely
```

### 12. Estimated work
- **Simple broker swap** (REST orders + websocket + instrument dump, Kite-like — e.g. Dhan/Fyers): ~2–4 days for the broker package + mapper + two websockets, plus 1–2 days testing. Touches only `brokers/<new>/`, the container, `live_seams`, the two `build_seams`, and configs.
- **Completely new broker** (unusual auth like XTS session login, no order-update push, or a very different instrument master): ~1–2 weeks, mostly auth + instrument mapping + the exception split.
- **High-risk files:**
  - `<newbroker>/mapper.py` — the timeout-ambiguity / exception split (duplicate-order risk).
  - `<newbroker>/<broker>.py` `place_order` / `get_order` — tag pass-through, partial-fill + 0-price handling.
  - `dependency_container._build_<newbroker>_broker` — wiring / auth.
- **Testing priority (in order):** exception mapping → order placement + fill confirmation → order-update push idempotency → crash recovery/reconciliation → live with minimal size.

---

## Project Improvement

These are things this documentation cannot fully cover because the project does
not currently define them:

- **Instrument sync script is a stub.** `scripts/sync_instruments.py` is not implemented. There is no manual "sync the dump" command; the dump is fetched automatically at runtime. To inspect listed expiries/contracts today you must query Kite directly.
- **No external alerting.** Alerts are recorded in-memory and re-logged only; nothing is sent to Slack/email/webhook (the `.env` `ALERT_CHANNEL_WEBHOOK_URL` is unused). Until a dispatcher is added, watching the logs is the only notification.
- **No durable/rotated log file.** The platform logs to stderr only. The `tee` in Section 8 is a manual workaround; consider adding a rotating file handler or a `systemd`/journald unit.
- **No process manager / Docker / CI provided.** No `systemd` unit, `Dockerfile`, or deployment scripts ship with the project. Running under `tmux` + `tee` is the documented approach; a `systemd` service is recommended for auto-restart.
- **Secrets are committed in `.env`.** The repo currently tracks a real `.env`. Before handover: rotate all Kite and DB credentials, `git rm --cached .env`, and add `.env` to `.gitignore`.
- **`hard_cutoff_time` equals market close (15:30) with an INTRADAY product.** Confirm and move it before the broker's MIS auto-square-off, or the forced exit can collide with the broker squaring off first.
