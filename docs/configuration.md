# Configuration Guide

Everything you can tune lives in two places:

1. **Secrets** (passwords, API keys, the database address) go in **environment
   variables** — never in the config files.
2. **Everything else** goes in the **`configs/*.yaml`** files.

The platform reads and checks all config the moment it starts. If anything is
wrong or missing, it stops immediately with a clear error — it will *not* start
half-configured and fail later when a trade is due.

> **The one rule that matters most:** real secrets never go in the YAML files.
> The broker config stores the *names* of environment variables (like
> `KITE_API_KEY`), and the actual values live in the environment.

---

## 1. Secrets (environment variables)

For local development these go in a `.env` file at the project root. In
production, your deployment tool should inject them.

| Variable | Needed when | What it is |
|---|---|---|
| `DATABASE_URL` | always | The full address of the Postgres database, including username and password. Example: `postgresql+psycopg2://user:pass@localhost:5432/algo`. The platform won't start without it. |
| `CONFIG_DIR` | optional | Where the config files live. Defaults to `configs`. |
| `KITE_API_KEY` | live only | Your Zerodha Kite API key. |
| `KITE_API_SECRET` | live only | Your Zerodha Kite API secret. |
| `KITE_ACCESS_TOKEN` | live only | Today's login token (has to be refreshed daily). |
| `I_UNDERSTAND_THIS_TRADES_REAL_MONEY` | live only | Must be set to exactly `yes` or live mode refuses to start. A deliberate "are you sure?" gate. |

There's a `.env` template in the project root. **Never put real passwords or
keys in a file you commit to git.**

---

## 2. The config files, one by one

All of these are in the `configs/` folder.

### `app.yaml` — what to run

This is where you list which strategies run on which instruments.

```yaml
environment: "development"   # just a label for logs
log_level: "INFO"            # how chatty the logs are
instances:
  - strategy_id: strategy_1
    instrument: NIFTY        # must be UPPERCASE (see warning below)
    account: primary         # must match a name in accounts.yaml
  - strategy_id: strategy_1
    instrument: SENSEX
    account: primary
```

> ⚠️ **Write the instrument name in UPPERCASE** (`NIFTY`, `SENSEX`). The platform
> uses this exact text to look up things like the margin in `risk.yaml`, and the
> lookup is case-sensitive. If you write `nifty` here, the margin check won't
> find its setting. (You'll get an error at startup if the `account` name is
> wrong, so that one is safe.)

### `brokers.yaml` — which broker, and its settings

The single most important line here is `active_broker`. It's either
`SIMULATION` (paper) or `KITE` (live).

```yaml
active_broker: SIMULATION
rate_limits:                 # how fast we're allowed to call the broker
  ORDER_MUTATION:    {max_calls: 10, per_seconds: 1.0}
  ORDER_READ:        {max_calls: 10, per_seconds: 1.0}
  PORTFOLIO_READ:    {max_calls: 10, per_seconds: 1.0}
  MARKET_DATA:       {max_calls: 10, per_seconds: 1.0}
  INSTRUMENT_LOOKUP: {max_calls: 3,  per_seconds: 1.0}
  GENERAL:           {max_calls: 10, per_seconds: 1.0}
simulation:
  synchronous: false
  initial_cash: "10000000"
  # ...failure/latency knobs for the fake broker...
kite:
  api_key_env_var: "KITE_API_KEY"       # NAME of the env var, not the key itself
  api_secret_env_var: "KITE_API_SECRET"
  access_token_env_var: "KITE_ACCESS_TOKEN"
  read_retry_attempts: 3
  read_retry_delay_seconds: 0.5
  quote_batch_size: 200
```

Good to know:
- You must fill in **both** the `simulation` and `kite` sections, even though
  only one is active. That way switching brokers is a one-line change.
- You must list **all six** rate-limit categories. Leaving one out is treated as
  an error, not "no limit."
- The rate-limit numbers are **placeholders** — check Zerodha's real limits
  before going live.
- The safe default is `SIMULATION`.

### `accounts.yaml` — the trading account(s)

```yaml
accounts:
  - name: primary
    broker: SIMULATION
    broker_client_id: null              # null for paper; your Kite ID for live
    display_name: "Paper trading (simulation)"
```

The `name` (here, `primary`) is what `app.yaml` points at.

### `risk.yaml` — the safety limits

```yaml
market_open_time: "09:15:00"
market_close_time: "15:30:00"
max_daily_entries_per_account: 2
legs_per_entry: 2
margin_per_lot_by_instrument:
  NIFTY: "50000"                        # UPPERCASE names again
  SENSEX: "70000"
daily_loss_limit_by_account: "25000"
```

> The margin and loss-limit numbers are **placeholders** — put in real values
> before running. Also note: **the daily loss limit doesn't actually stop
> trading yet** (see `deployment.md`, item "H1"). It's read but never enforced,
> because the piece that tracks the day's running loss isn't built.

### `strategies/strategy_1/nifty.yaml` and `sensex.yaml` — the strategy dials

```yaml
entry_time: "09:20:00"        # when to enter
hard_cutoff_time: "15:15:00"  # when to force-close
target_pct: "0.10"            # take profit at 10% premium drop
sl_pct: "0.10"                # stop loss at 10% premium rise
lots: 1
product_type: INTRADAY
skip_on_expiry_day: true
monitoring_interval_seconds: 5
polling_interval_seconds: 2
retry:
  order_timeout_seconds: null
  fill_confirmation_attempts: 20
  fill_confirmation_delay_seconds: 0.25
  close_retry_attempts: 3
  close_retry_delay_seconds: 0.5
```

> The profit target, stop-loss, and lot size are **placeholders** — confirm real
> values first. The stop-loss and profit target are now checked continuously
> during the day by the monitoring heartbeat (roughly every 2 seconds), not only
> at the cut-off. `monitoring_interval_seconds` is the degraded-feed staleness
> threshold; the heartbeat cadence itself is set in the container.

### `database.yaml` — database connection tuning

Non-secret settings for the database connection pool (how many connections to
keep open, timeouts, etc.). Every setting has a sensible default, so you can
usually leave this file alone. The database *address and password* come from
`DATABASE_URL`, not here.

### `market_data.yaml` — price feed tuning

```yaml
freshness_seconds: 5.0        # a cached price older than this triggers a re-fetch
poll_timeout_seconds: null
```

---

## 3. Config files that aren't filled in yet

These exist but are empty placeholders. They're needed before live trading:

- `configs/instruments/nifty.yaml`, `sensex.yaml` — the instrument details
  (lot size, strike gap, expiry day). Needed for live trading (blocker "H4").
- `configs/holidays.yaml` — the market holiday calendar. Until this is filled,
  the platform assumes every weekday is a trading day.

---

## 4. How to check your config is valid

The simplest check today is to run the test suite, which loads the real config
files:

```
pytest src/algo/tests -q
```

If your config has a mistake, the container-related tests will fail with a clear
message pointing at the problem.
