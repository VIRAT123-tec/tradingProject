# Installation Guide

How to get the platform set up on a machine from scratch. Follow the steps in
order. This gets you to the point where the **tests pass** — actually running
the platform is covered in `deployment.md`.

Estimated time: 15–30 minutes if Postgres is already installed.

---

## What you need first

- **Python 3.11 or newer.** Check with `python --version`.
- **PostgreSQL** (version 13+ recommended). The platform is built for Postgres,
  not SQLite. You'll need to be able to create a database and a user.
- **Git**, to get the code.
- A terminal. On Windows, the examples below use PowerShell.

---

## Step 1 — Get the code

```
git clone <your-repo-url>
cd "trading project/algo_platform"
```

All commands below assume you're in the `algo_platform` folder (the one with
`pyproject.toml` in it).

---

## Step 2 — Create a virtual environment

A virtual environment keeps this project's Python packages separate from the
rest of your system.

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**macOS / Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

You'll know it worked when your prompt shows `(venv)` at the start.

> Note: this repo currently uses a folder literally named `venv!` (with an
> exclamation mark). A plain `venv` is fine and cleaner — just use the same name
> consistently.

---

## Step 3 — Install the platform

This installs the platform and all the libraries it needs, including the test
tools:

```
pip install -e ".[dev]"
```

The `-e` means "editable" — your code changes take effect without reinstalling.
The `[dev]` part adds `pytest` for running the tests.

---

## Step 4 — Set up the database

Create an empty database and a user for it. Using the `psql` tool:

```sql
CREATE DATABASE algo;
CREATE USER algo_user WITH PASSWORD 'choose-a-password';
GRANT ALL PRIVILEGES ON DATABASE algo TO algo_user;
```

Then tell the platform how to reach it by setting `DATABASE_URL`. For local dev,
put it in a `.env` file in the project root:

```
DATABASE_URL=postgresql+psycopg2://algo_user:choose-a-password@localhost:5432/algo
```

(There's a `.env` template already in the repo — fill in the real value.)

---

## Step 5 — Create the database tables

The empty database has no tables yet. This command creates them all
(it reads the migration files in `src/algo/database/migrations`):

```
alembic upgrade head
```

If it succeeds, your database now has all the tables the platform needs
(positions, orders, accounts, and so on).

> If `alembic` isn't found, make sure your virtual environment is activated
> (Step 2) — it was installed as part of Step 3.

---

## Step 6 — Check everything works

Run the test suite:

```
pytest src/algo/tests -q
```

You should see all tests pass (currently **563 passed**). The tests use their
own temporary database, so they won't touch the one you just set up.

If the tests pass, your installation is good.

---

## Common install problems

**"DATABASE_URL is not set"** — you skipped Step 4, or your `.env` file isn't in
the project root, or your virtual environment can't see it. Double-check the
file location and that the variable name is spelled exactly `DATABASE_URL`.

**`psycopg2` build errors on install** — the project uses `psycopg2-binary`
which usually avoids this. If you hit it, make sure your `pip` is up to date
(`pip install --upgrade pip`).

**`alembic upgrade head` fails to connect** — your `DATABASE_URL` is wrong, or
Postgres isn't running, or the user doesn't have access to the database. Test
the connection details with `psql` first.

**Wrong Python version** — `python --version` must be 3.11 or higher. If your
system has an older default, you may need to call `python3.11` explicitly when
creating the virtual environment.

---

## What's next

Installation only gets you to "tests pass." Before you can actually *run* the
platform, read:

- `configuration.md` — to fill in real settings.
- `deployment.md` — **especially the "Production Readiness" section**, which
  explains why the platform can't fully launch yet and what's safe to do.
