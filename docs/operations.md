# Operations Guide (Daily Runbook)

This is the "how to actually run it day to day" guide, written for whoever is
babysitting the platform during market hours.

> Reminder: the platform can't fully launch yet (see `deployment.md`, Section 1).
> This runbook describes how it's *meant* to be operated once the blockers are
> fixed, so it's ready when you are.

---

## The daily rhythm

A normal day looks like this:

| Time (IST) | What happens | What you do |
|---|---|---|
| Before ~09:00 | — | (Live only) Run the daily Kite login. Start the platform. |
| ~09:00–09:15 | Platform connects, cleans up any leftover state, arms timers | Watch the startup logs — confirm it reaches "platform started." |
| 09:20 | Entry fires for each instrument | Confirm each position opens (or was correctly skipped/blocked). |
| 09:20–15:15 | Position is monitored | Keep half an eye on the logs. |
| 15:15 | Hard cut-off — positions close | Confirm each position closed and the P&L looks sane. |
| After close | — | Stop the platform (Ctrl-C / SIGTERM). Skim the day's logs. |

---

## Starting up

**Paper:**
```
python -m algo.start_paper
```

**Live:**
```
# 1. Refresh today's Kite token (once per day)
python scripts/generate_token.py

# 2. Acknowledge real money and start
export I_UNDERSTAND_THIS_TRADES_REAL_MONEY=yes
python -m algo.start_live
```

### What a healthy startup looks like in the logs

You should see, in roughly this order:
- `starting platform`
- `created Account row ...` (first run only)
- `reconciliation complete: N subjects examined, M breaks recorded`
- `strategy instance ... is RUNNING` (one per instrument)
- `registered ...` and `scheduler started`
- `platform started with N strategy instance(s)`

If you see `M breaks recorded` with M greater than zero, something from a
previous run needed cleanup — check the troubleshooting guide.

---

## During the day: what to watch

Until real alerting exists, you're the monitor. Watch the logs for these words:

- **`CRITICAL`** — always investigate. This marks the serious stuff: a frozen
  instrument, a broker/database mismatch, a failed safety write.
- **`froze`** / **`FROZEN`** — an instrument hit an unexpected error and was
  parked for manual review. It will not trade again until a human clears it. The
  *other* instruments keep running normally.
- **`reconciliation break`** — the platform found a state it couldn't safely
  resolve on its own and recorded it for review.
- **`entry rejected`** — a safety check blocked an entry. This is often normal
  (e.g. already have a position, outside trading hours), but confirm the reason.

A quiet log between 09:20 and 15:15 is normal. The stop-loss and target *are*
being checked continuously in the background now (the monitoring heartbeat, ~every
2s) — a quiet log means "nothing crossed a threshold," not "nothing is watching."

---

## Stopping

Press **Ctrl-C**, or stop the service (which sends `SIGTERM`). The platform
shuts down gracefully: timers stop, strategies stop, the broker connection and
database pool close.

**Important:** stopping does **not** close open positions. An open straddle
survives the shutdown and is resumed when you next start the platform. If you
need to actually flatten positions, you currently have to do it manually through
the broker (there's no built-in "close everything" command yet — that's part of
the missing kill switch, blocker H2).

---

## Restarting after a crash

Just start it again. This is the whole point of the design:

1. The reconciler compares the database to the broker and cleans up anything the
   crash left mid-flight.
2. Each instrument then recovers from the database — an open position resumes
   monitoring, an interrupted close finishes, and so on.

You don't need to do anything special. But **do** read the startup logs after a
crash-restart to see what reconciliation did, and check for any frozen
instruments or reconciliation breaks.

---

## Handling a frozen instrument

If an instrument freezes, here's the situation:
- That one instrument has stopped trading. The others are unaffected.
- Its `StrategyInstance` row in the database is marked `FROZEN`.
- There's very likely an open or half-open position that needs a human decision.

What to do:
1. Read the logs to understand *why* it froze (look for the `CRITICAL` line and
   any `reconciliation break`).
2. Check the broker directly to see the real position.
3. Decide and act manually (e.g. close the position through the broker).
4. Once resolved, clearing the freeze so the instrument can trade again is a
   deliberate manual step in the database (unfreezing tooling isn't built yet).

Because freezing is deliberately conservative, treat every freeze as "stop and
look," not "it'll sort itself out."

---

## Things there is no button for yet

Be aware these operator actions are **not** available today:
- **Emergency stop / flatten everything** — not built (H2). You'd act through
  the broker directly.
- **Enforced daily loss limit** — configured but inert (H1). It won't stop you.
- **Automatic alerts** — none (M3). Watching logs is the manual substitute.

Knowing what the platform *won't* do for you is as important as knowing what it
will.
