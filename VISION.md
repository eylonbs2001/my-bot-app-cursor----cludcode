# FalconEye Scanner — Vision & Spec

## 1. Project Identity & Goal

- **The Project**: A high-frequency financial & crypto scanner bot.
- **The Mission**: Scan market data (Crypto/Stocks) and send real-time, actionable
  alerts to a private Telegram channel.
- **Target Infrastructure**: Hetzner VPS (Ubuntu 24.04, CX22 or higher), Docker-based.

## 2. Core Components

- **`scanner.py`** — the brain. Handles data fetching, technical analysis (TA),
  scoring, and Telegram delivery. Two top-level classes: `TradeManager`
  (Postgres + Redis state) and `FortressScanner` (scan loop + signal generation).
- **`main.py`** — thin entrypoint that imports `FortressScanner` and calls
  `run_forever()`. The Docker image runs `python -u scanner.py` directly.
- **`requirements.txt`** — pinned-range dependencies for clean
  `pip install` on a fresh server.
- **`Dockerfile`** — multi-stage build (builder + slim runtime). Final image
  runs as the unprivileged `falcon` user under `tini`.
- **`docker-compose.yml` / `docker-compose.prod.yml`** — local dev stack
  vs. production stack (the prod compose pins env, restart policy, log limits).
- **`setup-hetzner.sh`** — one-shot installer for a fresh Ubuntu 24.04 box
  (hardens SSH, installs Docker, creates `falcon` user, clones the repo,
  builds and starts the stack). Idempotent.

## 3. Technical Requirements

- **Indentation**: strict 4-space, no tabs.
- **Error handling**: resilient. Network/API blips must log and let the loop
  continue — never crash the process. Per-symbol worker failures are caught
  by `scan_exchange` so one bad symbol can't take down the scan.
- **Clean code**: no dead branches or stale debugging scaffolding.
- **Linter**: `ruff` config in `ruff.toml` (rules `E`, `F`, `I`, `W`;
  `E501` ignored project-wide; `E402`/`I001` ignored only in `scanner.py`
  because warning filters must run before pandas/pandas_ta imports).

## 4. Integration Details

### Telegram

The scanner talks to Telegram **directly via the HTTP Bot API** using the
`requests` library — *not* `python-telegram-bot`. Every send is a plain
`requests.post(f"https://api.telegram.org/bot{token}/sendMessage", ...)`
or `/sendPhoto` with an explicit `timeout=` kwarg.

Rationale: zero extra dependency, no async event-loop coupling, and the
admin command loop runs on a daemon thread with `getUpdates` long-polling.
The `_admin_send_text()` helper wraps admin replies with try/except so a
single Telegram blip can't abort a multi-step admin callback handler.

Tokens & chats:
- `TELEGRAM_BOT_TOKEN` / `TELEGRAM_TOKEN` — main bot token
- `VIP_TELEGRAM_BOT_TOKEN` — optional VIP-tier bot (falls back to main)
- `VIP_PLUS_TELEGRAM_BOT_TOKEN` — optional VIP+ bot (falls back to VIP)
- `TELEGRAM_CHAT_ID` / `CHAT_ID` — main broadcast chat
- VIP+ chat is configured via its own env var

### Financial Data

- **Crypto OHLCV**: `ccxt` against Binance, Bybit, OKX (with public-client
  retry fallback if an authenticated client hits an auth-restricted endpoint).
- **Order book / L2**: `fetch_order_book` for layer-6 wall checks.
- **Macro context**: BTC correlation derived from 15m OHLCV.
- **External advisory**: optional CMC + LunarCrush snapshots
  (cached, advisory-only — never used as a hard gate).
- **Persistence**: Postgres (`asyncpg`) for signals/journal,
  Redis (`redis.asyncio`) for cooldowns + active-trade hot cache,
  SQLite for local learning state (path auto-resolved across runtimes).

## 5. Known Caveats

- **No 429 / Retry-After handling on Telegram sends** — a sustained burst that
  trips Telegram's rate limit will drop messages. Acceptable today given per-key
  alert cooldowns, but worth wiring proper backoff if the alert volume grows.
- **`print()` is the logger** — no levels, no rotation. Container `--log-opt`
  caps the file size in prod; replace with `logging` module if/when a structured
  log pipeline is added.
