# Deployment Guide

How to run the platform — and, just as importantly, an honest account of what's
safe to run today and what isn't.

**Please read Section 1 before anything else.** It is the difference between a
paper experiment and an expensive mistake.

---

## 1. Production Readiness & Known Limitations (read this first)

The platform's individual pieces are well-built and heavily tested. But a few
important safety features are **built yet not fully connected**, so:

> ### 🚫 The platform is NOT ready to trade real money today.
> ### 🚫 It also can't fully launch in paper mode yet (a couple of required helpers aren't built).

Here is the complete, honest list from the production review, worst first. The
letter/number codes (C1, H1, …) are just labels so we can refer to each item.

### ✅ C1 (the former top blocker) — FIXED

**C1 — The automatic stop-loss and profit-target now run during the day.**
This was the big one, and it's resolved. A **monitoring heartbeat**
(`strategy_engine/strategy_scheduler.py`, previously a stub) now re-checks every
open position on a cadence (default every 2 seconds) and closes it the moment
the stop-loss or target is crossed — using a polling price read that works with
or without a live websocket. Live ticks are also routed to the strategy now, so
when a real market-data feed is connected the check is near-instant. Both paths
are proven by integration tests that close a position on stop-loss and on target
mid-morning, with no manual trigger, well before the cut-off.

The remaining items below are still open.

### ✅ H1–H4 (the former High-severity blockers) — FIXED

**H1 — Daily loss limit now enforces.** The check recomputes the account's
realized P&L from the closed-position rows on every entry attempt, writes it to
the daily risk row, and latches a sticky "breached" flag once the limit is
crossed — blocking all new entries for the rest of the day, persisted across
restarts. (Uses realized P&L; an unrealized-P&L extension is noted below.)

**H2 — There's a working kill switch.** A `KillSwitch` producer sets the
control flags the platform already reads, and an operator CLI
(`python -m algo.killswitch engage/disengage/status`) is the out-of-process
"big red button" that halts an already-running platform. Engaging it blocks new
entries and flattens open positions via the monitoring heartbeat; the state is a
durable flag, so it survives restarts. Idempotent (engaging twice ≠ two flags).

**H3 — Live fill notifications are connected.** The container connects the
broker's order-update feed and routes updates through an idempotent,
advance-only `OrderUpdateProcessor` that records fills, partial fills,
cancellations, and rejections — safely alongside the polling path (whichever
sees the update first wins; the other no-ops). Degrades gracefully to polling if
the websocket is unavailable.

**H4 — The live startup flow is completed.** The container now verifies broker
connectivity (and fails fast) *before* any strategy is armed. Real config-backed
seams exist (`ConfigInstrumentService`, `ConfigExpiryService`, broker-backed
`BrokerSpotPriceProvider`) plus a `PollingTickStream` (correct via broker
polling) and an opt-in low-latency `KiteTickStream`, so `start_paper`/
`start_live` `build_seams()` now return real objects instead of raising. Some
values still need confirmation before live use — see below.

### Still to confirm before a real live run

- **Instrument-config values** in `configs/instruments/*.yaml` — lot size,
  strike interval, and especially the **weekly-expiry weekday** — are sensible
  defaults but MUST be validated against current exchange rules (they change).
  Ideally replace the config-weekday expiry with an instrument-master/
  exchange-calendar source.
- **A valid `KITE_ACCESS_TOKEN`** must be minted by the daily login. The token
  store now reads it from the environment; `scripts/generate_token.py` (the
  interactive login) is still a stub, so today the token is provided manually.
- **The daily loss limit is realized-P&L based.** An open position sitting at a
  large unrealized loss does not yet count toward the limit (that blends into
  intraday emergency-exit territory) — a reasonable follow-up.

### ✅ Medium-severity items (M1–M3) — FIXED

**M1 — Instruments now enter concurrently.** When two instruments' triggers
coincide (Nifty + Sensex at 09:20), the scheduler dispatches them in parallel,
so the second no longer waits for the first's full fill-confirmation. The
daily-loss-limit check was made concurrency-safe to match (its decision is
computed read-only; the shared risk-row write is best-effort and tolerates a
lost race), so parallel same-account entries can't spuriously freeze.

**M2 — Live order calls are now time-bounded.** The Kite client is constructed
with a configurable socket timeout (`request_timeout_seconds` in `brokers.yaml`,
default 7s) that bounds *every* call including order placement — the SDK has no
per-call timeout, so this is the correct place to bound it. A hung request now
surfaces as the (mutation-ambiguous) timeout the platform already handles,
instead of blocking a strategy indefinitely.

**M3 — Critical events are now surfaced (alerting).** A logging handler forwards
every CRITICAL event (frozen instrument, reconciliation break, broker/DB
divergence) to a pluggable `AlertDispatcher`; the default records a bounded
history and re-emits on a dedicated `algo.alerts` stream. Logging is also set up
with a consistent structured format. A specific external channel
(webhook/email/Slack) is a drop-in dispatcher to add later. A process *watchdog*
was intentionally **not** built — it is a distinct new feature, out of scope for
a bug-fix pass.

### Minor notes

- A cancelled/no-fill entry still "uses up" that instrument's one attempt for
  the day (by design, but worth knowing).
- The tests use SQLite, which doesn't enforce row-locking the way Postgres does,
  so one concurrency safeguard is real in production but not exercised by tests.

### Bottom line

- **C1, all four High-severity blockers (H1–H4), and all Medium items (M1–M3)
  are fixed and tested.** Only Low-severity items remain.
- **Paper mode** starts end-to-end (proven by tests). A full paper *trade* still
  needs the simulation broker seeded with an option chain + prices.
- **Live mode** is now much closer: confirm the instrument-config values (esp.
  expiry weekday), supply a valid `KITE_ACCESS_TOKEN`, and wire a real external
  alert channel (a drop-in `AlertDispatcher`) before real capital. Remaining
  work is Low severity plus that operational wiring.
  Remaining work is Medium/Low severity, not blockers.

None of this is a rewrite — it's connecting and finishing pieces that are
already built to plug together.

---

## 2. How running the platform is meant to work

Even though it can't fully launch yet, here's the intended shape so the rest of
this guide makes sense.

The platform runs as **one long-lived process per environment** (one for paper,
a separate one for live). It's meant to be started in the morning before the
market opens and left running through the trading day. It handles its own
timing internally — you don't cron-schedule individual trades.

### Paper mode

```
python -m algo.start_paper
```

This runs against the fake broker. No real orders. Safe to experiment with once
H4 (the launch helpers) is done — the intraday stop-loss (C1) already works.

### Live mode

```
# You must explicitly acknowledge real money:
export I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes    # Windows: $env:I_UNDERSTAND_THIS_TRADES_REAL_MONEY="yes"
python -m algo.start_live
```

Live mode also refuses to start unless `brokers.yaml` actually says
`active_broker: KITE` — so you can't accidentally run the live starter against a
paper config, or vice versa.

### The daily login (live only)

Kite requires a fresh login token every day. The intended flow is to run the
login script each morning before starting the platform:

```
python scripts/generate_token.py      # (not built yet — blocker H4)
```

This mints the day's token and stores it where the platform can read it.

---

## 3. Starting and stopping cleanly

- **Startup order** is handled automatically by the platform: connect to the
  broker, clean up any leftover state from a crash (reconciliation), start the
  price feed, arm the timers.
- **Shutdown:** press **Ctrl-C**, or send the process a `SIGTERM` (what most
  process managers use to stop a service). The platform catches this and shuts
  down gracefully — it stops the timers, stops the strategies, closes the broker
  connection, and closes the database pool. **It does not close open
  positions** — an open position survives a restart and is picked back up.
- If startup fails partway through, the platform automatically tears down
  whatever it already started, so you're never left with a half-running process.

---

## 4. Suggested production setup (once the blockers are fixed)

- **Run it as a managed service** (e.g. a `systemd` unit on Linux) so it
  restarts automatically if it dies and stops with `SIGTERM`. Because the
  platform recovers cleanly from the database on startup, an automatic restart
  is safe.
- **One process per mode.** Never point a paper and a live process at the same
  database.
- **Inject secrets from the environment**, not from a committed `.env` file.
- **Back up the Postgres database** — it's the source of truth for every
  position and order. If you lose it, a restart can't recover.
- **Watch the logs for `CRITICAL` lines.** Until proper alerting exists (M3),
  scanning logs for `CRITICAL` and `froze` is the manual substitute — those mark
  frozen instruments and reconciliation problems that need a human.

---

## 5. A safe rollout path

1. Get the tests passing (see `installation.md`).
2. ~~Fix C1 (intraday monitoring)~~ — **done.** Build H4 (the missing launch
   helpers) so paper mode can actually start.
3. Run **paper mode** for a good stretch and confirm entries, stop-loss exits,
   target exits, and restarts all behave.
4. Fix H1, H2, H3 and add basic alerting (M3).
5. Only then, with tiny size, try **live mode** — and watch it closely.

Do not skip step 3. The whole point of the paper broker is to prove the strategy
behaves correctly with zero financial risk before real capital is involved.
