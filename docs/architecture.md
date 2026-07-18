# How the Platform Works (Architecture)

This explains, in plain language, what the system is and how its parts fit
together. If you read only one doc, read this one.

> ⚠️ **Before running this with any money, read `deployment.md`.** A few
> important safety features are built but not yet fully connected — most
> importantly, the automatic stop-loss during the day. This is explained
> honestly in the deployment guide.

---

## What this thing does

It runs one trading strategy — a **short straddle** — on two indices, Nifty and
Sensex. Once each morning, for each index, it sells an at-the-money call and an
at-the-money put (that pair is the "straddle"). It then watches the position and
closes it when a profit target is hit, a stop-loss is hit, or a fixed cut-off
time arrives.

It can run in two modes, using the exact same strategy code:
- **Paper mode** — a fake ("simulation") broker. No real orders, no real money.
- **Live mode** — the real Kite (Zerodha) broker. Real orders, real money.

The whole design is built around one goal: **never place a wrong, duplicate, or
forgotten order — even if the program crashes at the worst possible moment.**

---

## The one idea that explains most of the code

**The database is the truth. Everything else can be rebuilt.**

If the program crashes and restarts, it doesn't guess what happened. It reads
the database, sees exactly where it left off, and continues. Prices held in
memory, the broker's own view, what was "in progress" — all of it is
reconstructed from the database on restart. This is why so much care goes into
writing to the database *before* doing anything risky.

A close second idea:

**Write down what you're about to do, before you do it.** Before the platform
sends any order to the broker, it first saves a note in the database that says
"I am about to send this order." If it crashes right after sending but before
hearing back, the restart sees that note and can safely check with the broker
instead of blindly sending the order again.

---

## The parts, from top to bottom

Think of it as layers. Each layer only talks to the one below it.

```
  You start the program        →   start_paper.py / start_live.py
        │                            (handles Ctrl-C, clean shutdown)
        ▼
  Everything gets built and     →   dependency_container.py
  wired together here               (the "assembly point")
        │
        ▼
  The helpers                   →   Scheduler   (fires the daily timers)
                                    RiskCore    (pre-trade safety checks)
                                    MarketData  (live prices)
                                    Reconciler  (crash cleanup on startup)
        │
        ▼
  The strategy itself           →   Strategy-1: decides entry, exit, monitoring
        │
        ▼
  The broker (interchangeable)  →   Simulation (paper)  OR  Kite (live)
        │
        ▼
  Storage                       →   Postgres database
```

### The parts, in one line each

- **Entrypoints** (`start_paper.py`, `start_live.py`) — the "on switch." They
  handle Ctrl-C, start everything, and shut down cleanly.
- **Container** (`dependency_container.py`) — the assembly point. It reads all
  the config files and builds every piece, plugging them into each other.
- **Scheduler** — a background timer. At 09:20 it says "time to enter"; at 15:15
  it says "time to close." It doesn't know *what* those mean; the strategy does.
- **Risk Core** — the bouncer at the door. Before any entry, it runs a list of
  safety checks (are we in trading hours? is there already a position? is there
  enough margin? etc.). If any check fails, no trade.
- **Market Data** — the live price feed, with a cache and an automatic fallback
  to "just ask the broker" if the live feed goes quiet.
- **Reconciler** — runs once, on startup. It compares the database against the
  broker's real state and fixes or flags anything left in a weird state by a
  crash, *before* the strategy is allowed to act.
- **Strategy-1** — the actual trading logic, split into small tested pieces:
  pick the strike, place the entry, watch the position, place the exit.
- **Broker** — the thing that actually places orders. Same interface whether
  it's the fake broker or the real one.
- **Database** — Postgres, the permanent record of every position and order.

For a file-by-file list of what's built and what's still a placeholder, see
`modules.md`.

---

## What happens during a normal day

### Morning: entering the trade (09:20)

1. The scheduler's timer hits 09:20 and tells the strategy "enter now."
2. Risk Core runs its safety checks. If anything fails, we stop here.
3. The strategy figures out the at-the-money strike and the exact call/put
   contracts.
4. **It saves the plan to the database first** (a position marked "entry in
   progress," plus the two orders it's about to place).
5. It places the call order, waits for it to fill, then the put order.
6. If both fill, the position is marked "open" and monitoring begins.
7. If something goes wrong (one leg fills, the other is rejected), it
   immediately buys back the filled leg so we're never left half-exposed, marks
   the position as errored, and freezes that instrument for a human to review.

### During the day: watching the position

The strategy is supposed to keep checking the live price against the profit
target and the stop-loss, and close early if either is hit.

> ✅ **This is now wired up.** A monitoring heartbeat re-checks every open
> position on a cadence (default every 2 seconds), and live ticks are routed to
> the strategy too — so the stop-loss and target are evaluated continuously
> through the day, not just at the cut-off. (This was formerly the top blocker,
> "C1"; see `deployment.md`.)

### Afternoon: closing (15:15)

At the cut-off time, the strategy closes both legs, works out the profit or
loss, and marks the position "closed." The close is done leg-by-leg and checks
each leg's own state first, so it's safe even if it was interrupted and
restarted midway.

---

## What happens if it crashes

This is the part the design cares about most. On restart:

1. The reconciler compares the database to the broker and cleans up anything
   left mid-flight (an order that was sent but never confirmed, etc.).
2. Then each strategy instance "recovers" — it reads its position from the
   database and picks up exactly where it was:
   - Nothing was happening → start fresh.
   - Position was open → resume watching it.
   - A close was interrupted → finish the close.
   - An entry was stuck half-done → mark it errored and freeze for review.

Because the reconciler runs first, recovery always works from a cleaned-up,
trustworthy database.

---

## About threads (the "many things at once" part)

The platform does a few things at the same time using background threads:

- one thread runs the **scheduler timer**,
- one thread receives **live prices** from the broker's websocket,
- (paper mode) one thread simulates the broker filling orders over time.

To keep this safe, each strategy instance processes one thing at a time behind
a lock — a price update and a timer can't run its logic simultaneously. And if
any strategy hits an unexpected error, only *that* instrument is frozen; the
others keep running.

**One known rough edge:** the scheduler handles instruments one after another on
a single thread. So when Nifty and Sensex both want to enter at 09:20, the
second one waits for the first to finish entering. For a strategy that cares
about the exact entry price, that small delay matters. This is noted in the
deployment guide (item "M1").

---

## The honest status

The individual pieces are well-built and thoroughly tested (563 automated
tests). The most important gap — the missing intraday stop-loss — has now been
**fixed** (the monitoring heartbeat described above). But several *other* safety
features are still built yet **not connected end-to-end** (the enforced loss
limit, the kill switch, the live-launch helpers), so the platform is **still not
ready for real money as-is**.

The full, honest list — with severity — is in `deployment.md` under
"Production Readiness & Known Limitations." Please read it before deploying.
