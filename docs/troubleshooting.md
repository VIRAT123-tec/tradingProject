# Troubleshooting Guide

Common problems, what causes them, and what to do. Organised by "what you're
seeing."

---

## Startup problems

### "DATABASE_URL is not set"
**Cause:** the platform can't find the database address.
**Fix:** make sure `DATABASE_URL` is set — in your `.env` file (project root) for
local dev, or in the environment for production. Check the spelling is exactly
`DATABASE_URL`, and that your virtual environment is activated.

### It stops with "not implemented" / `NotImplementedError` in `build_seams`
**Cause:** this is expected right now. The platform deliberately refuses to fully
start because some required helpers (instrument data, expiry dates, spot prices)
aren't built yet — blocker H4 in `deployment.md`.
**Fix:** nothing to fix; this is the current honest state. Those helpers need to
be implemented before the platform can launch.

### "refusing to start live trading" (live mode)
**Cause:** the safety gate. Live mode requires you to explicitly acknowledge real
money.
**Fix:** set `I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes` and try again. If you
didn't mean to run live, run `start_paper.py` instead.

### "expected active_broker=... but brokers.yaml selects ..."
**Cause:** you ran the live starter with a paper config (or vice versa).
**Fix:** make `brokers.yaml`'s `active_broker` match the starter you're using —
`KITE` for `start_live`, `SIMULATION` for `start_paper`.

### "brokers.yaml rate_limits is missing categories [...]"
**Cause:** you left one of the six rate-limit categories out of `brokers.yaml`.
**Fix:** add the missing category. All six must be present (the platform won't
guess "no limit").

### "app.yaml instances reference unknown account name(s) [...]"
**Cause:** an `instance` in `app.yaml` points at an `account` that doesn't exist
in `accounts.yaml`.
**Fix:** fix the typo, or add the account. The names must match exactly.

### Config validation errors at startup
**Cause:** a value in one of the `configs/*.yaml` files is invalid (wrong type,
out of range, entry time after cut-off time, etc.).
**Fix:** the error message names the file and field. This is the platform doing
its job — catching bad config at startup instead of at 09:20. Fix the value.

---

## Database problems

### `alembic upgrade head` won't connect
**Cause:** wrong `DATABASE_URL`, Postgres not running, or the user lacks access.
**Fix:** test the connection with `psql` using the same credentials. Confirm
Postgres is running and the database exists.

### "database is locked" (only in tests)
**Cause:** this is a SQLite-only quirk in the test setup, not a production issue
(production uses Postgres).
**Fix:** if you're writing new tests, follow the existing test setup in
`src/algo/tests/integration/conftest.py` (it uses WAL mode to avoid this).

### `ConcurrentModificationError`
**Cause:** two things tried to change the same database row at once, and the
optimistic-lock check caught it. This is a *safety feature working*, not a bug.
**Fix:** usually transient. If it happens repeatedly for the same position,
investigate what two code paths are fighting over that row.

---

## Trading-day problems

### An entry didn't happen at 09:20
**Cause:** most likely a safety check blocked it. Look for `entry rejected` in
the logs — the message says which check failed. Common, normal reasons:
- already have a position for that instrument today,
- outside trading hours,
- it's an expiry day and `skip_on_expiry_day` is on,
- the daily entry limit was reached,
- not enough margin.

**Fix:** if the reason is expected, nothing to do. If it's wrong (e.g. "not
enough margin" when there clearly is), check the relevant `risk.yaml` setting.

### The stop-loss / profit target didn't trigger when I expected
**Cause:** the intraday monitoring heartbeat *does* run now (about every 2
seconds), so the usual cause is that the combined premium simply didn't cross
the configured target/stop-loss threshold, or a leg's price wasn't available
(the monitor needs both legs priced to compute the combined premium).
**Fix:** check the actual leg prices vs the position's `target_premium` /
`stoploss_premium` in the database, and confirm the price feed (or polling
fallback) is returning both legs. If prices are missing, see the market-data
notes in `deployment.md`.

### An instrument is "frozen"
**Cause:** that instrument hit an unexpected error and was parked for safety. The
other instruments keep running.
**Fix:** see the "Handling a frozen instrument" section in `operations.md`. Short
version: read the `CRITICAL` log line to learn why, check the real position at
the broker, resolve manually, then clear the freeze in the database.

### Reconciliation recorded "breaks" on startup
**Cause:** the platform found leftover state from a previous run that it couldn't
safely resolve automatically (e.g. a partial entry, or an order it couldn't match
to broker truth).
**Fix:** look up the recorded break, compare the database against the broker's
actual state, and resolve the discrepancy. A break is a deliberate "a human
should look at this" flag.

### An order timed out and I don't know if it went through
**Cause:** this is the "ambiguous" case the platform is specifically designed
for. It will **not** blindly resend the order (it might already be live).
**Fix:** the platform leaves the order marked "unconfirmed" for reconciliation to
sort out, and (for an entry) freezes the instrument. Check the broker to see
whether the order actually landed, then resolve as above.

---

## "Is this broken, or is it just not built yet?"

A lot of "problems" are actually known, deliberate gaps. Before deep debugging,
check whether what you're hitting is one of the blockers in `deployment.md`
Section 1 (C1, H1–H4, M1–M3). If it's listed there, it's expected behaviour for
the current state of the project, not a fault to chase down.

---

## Getting more detail

- Turn up logging: set `log_level: "DEBUG"` in `app.yaml` (or `LOG_LEVEL=DEBUG`).
- The database itself is the best source of truth for "what state is this
  position in" — inspect the `positions`, `orders`, `order_intents`, and
  `reconciliation_breaks` tables directly.
- Run the test suite (`pytest src/algo/tests -q`) to confirm the code itself is
  healthy after any change.
