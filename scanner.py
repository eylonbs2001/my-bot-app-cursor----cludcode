import os
import json
import math
import sqlite3
import threading
import time
import tempfile
import io
import statistics
import asyncio
import warnings
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse
from zoneinfo import ZoneInfo

# IMPORTANT: warning suppression must be installed BEFORE pandas / pandas_ta /
# matplotlib are imported, otherwise their first call paths can register
# deprecation handlers that bypass our later filterwarnings() calls. We also
# deliberately omit the `category=` argument so the filter matches any
# Warning subclass (UserWarning, FutureWarning, DeprecationWarning, etc.)
# pandas may use across versions.
warnings.filterwarnings("ignore", message=r".*Converting to PeriodArray/Index.*")
warnings.filterwarnings("ignore", message=r".*VWAP.*datetime ordered.*")
warnings.filterwarnings("ignore", message=r".*Period.*timezone.*")
warnings.filterwarnings("ignore", category=FutureWarning, module=r"pandas_ta.*")
warnings.filterwarnings("ignore", category=UserWarning, module=r"pandas_ta.*")
# Belt-and-suspenders: also tell Python's runtime to silence these at the
# interpreter level for any thread spawned later by ThreadPoolExecutor.
os.environ.setdefault("PYTHONWARNINGS", "ignore::UserWarning,ignore::FutureWarning")

import ccxt
import matplotlib.pyplot as plt
import pandas as pd
import requests
import asyncpg
import redis.asyncio as redis_async
from openai import OpenAI
from dotenv import load_dotenv

try:
    import pandas_ta_classic as ta
except Exception:
    import pandas_ta as ta

load_dotenv()


def _urlparse_database_url(db_url: str) -> Any:
    s = db_url.strip()
    if s.startswith("postgres://"):
        s = "postgresql://" + s[len("postgres://") :]
    return urlparse(s)


def _asyncpg_ssl_kwarg(parsed: Any) -> Any:
    """Map sslmode / ssl query params from a DSN into asyncpg's ssl= argument."""
    qs = parse_qs(parsed.query)
    mode = (qs.get("sslmode") or [""])[0].lower()
    if mode in ("require", "verify-ca", "verify-full"):
        return True
    if (qs.get("ssl") or [""])[0].lower() in ("1", "true", "on"):
        return True
    return None


def _pg_connect_kwargs_placeholder_user_dsn(
    db_url: str, pg_user: str, pg_password_env: str
) -> Dict[str, Any]:
    """When DATABASE_URL uses the literal username `user`, connect as pg_user instead."""
    parsed = _urlparse_database_url(db_url)
    password = (pg_password_env or "").strip() or unquote(parsed.password or "")
    if not password:
        raise RuntimeError(
            "Postgres: DATABASE_URL username is placeholder 'user'. Set POSTGRES_PASSWORD "
            "to the real role's password, or fix DATABASE_URL with the correct user."
        )
    path = (parsed.path or "").lstrip("/")
    kwargs: Dict[str, Any] = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "user": pg_user,
        "password": password,
        "database": path or "postgres",
    }
    ssl_arg = _asyncpg_ssl_kwarg(parsed)
    if ssl_arg is not None:
        kwargs["ssl"] = ssl_arg
    return kwargs


class TradeManager:
    """Async Postgres + Redis trade persistence layer."""

    def __init__(self) -> None:
        self.db_url = (
            os.getenv("DATABASE_URL", "").strip()
            or os.getenv("DB_URL", "").strip()
        )
        self.pg_host = os.getenv("POSTGRES_HOST", "fortress-postgres")
        self.pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
        # Prefer DATABASE_URL/DB_URL (above); discrete vars are for local/docker only.
        self.pg_user = os.getenv("POSTGRES_USER", "falcon_admin")
        self.pg_password = os.getenv("POSTGRES_PASSWORD", "").strip()
        self.pg_database = os.getenv("POSTGRES_DB", "trading_db")
        self.redis_url = (
            os.getenv("REDIS_URL")
            or os.getenv("REDIS_PRIVATE_URL")
            or os.getenv("REDIS_PUBLIC_URL")
            or ""
        )
        self.redis_required = os.getenv("REDIS_REQUIRED", "false").strip().lower() == "true"
        self._redis_url_explicit = bool(self.redis_url)
        if not self.redis_url:
            # No Redis URL configured. Print a loud, parseable warning so this
            # never gets silently buried in logs (as it did previously when we
            # fell back to localhost on Railway and only failed at ping time).
            print(
                "[CRITICAL] REDIS_URL/REDIS_PRIVATE_URL/REDIS_PUBLIC_URL is not set. "
                "Falling back to redis://fortress-redis:6379/0 - this will fail unless "
                "the fortress-redis container is reachable on this network. Set "
                "REDIS_URL in your env."
            )
            self.redis_url = "redis://fortress-redis:6379/0"
        self.pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[redis_async.Redis] = None

    async def startup(self) -> List[dict]:
        # Pool config tuned for managed Postgres (Railway, Supabase, RDS) which
        # silently drop idle connections. max_inactive_connection_lifetime
        # forces asyncpg to retire connections before the server kills them
        # (avoiding 'SSL error: unexpected eof while reading').
        # command_timeout caps slow queries so a network hiccup can't hang the loop.
        pool_kwargs = dict(
            min_size=1,
            max_size=10,
            max_inactive_connection_lifetime=180.0,  # recycle every 3 min
            command_timeout=30.0,
            server_settings={"application_name": "falconeye_scanner"},
        )
        if self.db_url:
            parsed = _urlparse_database_url(self.db_url)
            url_user = unquote(parsed.username or "")
            # Many templates use postgresql://user:...@host — that literal role breaks
            # when the real Postgres user is falcon_admin (POSTGRES_USER / default).
            if url_user == "user" and self.pg_user != "user":
                print(
                    f"[POSTGRES] DATABASE_URL uses placeholder user 'user'; "
                    f"connecting as '{self.pg_user}' (host/db/ssl from URL)"
                )
                conn_kw = _pg_connect_kwargs_placeholder_user_dsn(
                    self.db_url, self.pg_user, self.pg_password
                )
                self.pool = await asyncpg.create_pool(**conn_kw, **pool_kwargs)
            else:
                print("[POSTGRES] connecting via DATABASE_URL/DB_URL (dsn)")
                self.pool = await asyncpg.create_pool(dsn=self.db_url, **pool_kwargs)
        else:
            print(
                "[POSTGRES] DATABASE_URL/DB_URL not set — using POSTGRES_* "
                "(set DATABASE_URL in production to avoid auth mismatch)"
            )
            if not self.pg_password:
                raise RuntimeError(
                    "Postgres: set DATABASE_URL/DB_URL, or set POSTGRES_PASSWORD "
                    "when using discrete POSTGRES_HOST/POSTGRES_USER."
                )
            print(
                f"[POSTGRES] connecting host={self.pg_host} port={self.pg_port} "
                f"db={self.pg_database} user={self.pg_user}"
            )
            self.pool = await asyncpg.create_pool(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password,
                database=self.pg_database,
                **pool_kwargs,
            )
        # Hard connectivity probe for startup visibility.
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        print("[POSTGRES] ok (probe SELECT 1)")
        if self._redis_url_explicit:
            print("[REDIS] connecting (url from env)")
        else:
            print("[REDIS] connecting (built-in fallback; set REDIS_URL in production)")
        self.redis = redis_async.from_url(self.redis_url, decode_responses=True)
        if self.redis is None:
            raise RuntimeError("Redis connection is not initialized; call startup() first")
        try:
            await self.redis.ping()
            print("[REDIS] ok (PING)")
        except Exception as exc:
            print(
                f"[REDIS] unavailable ({exc}). "
                "Continuing without Redis cache/state. Set REDIS_REQUIRED=true to fail fast."
            )
            self.redis = None
            if self.redis_required:
                raise
        await self._init_schema()
        return await self.recover_active_trades()

    async def _init_schema(self) -> None:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL DEFAULT '15m',
                    side TEXT NOT NULL,
                    entry_price DOUBLE PRECISION NOT NULL,
                    stop_loss DOUBLE PRECISION NOT NULL,
                    tp1 DOUBLE PRECISION NOT NULL,
                    tp2 DOUBLE PRECISION NOT NULL,
                    tp3 DOUBLE PRECISION NOT NULL,
                    score INTEGER NOT NULL,
                    rsi_snapshot DOUBLE PRECISION,
                    adx_snapshot DOUBLE PRECISION,
                    rel_volume_snapshot DOUBLE PRECISION,
                    status TEXT NOT NULL DEFAULT 'Pending',
                    last_price DOUBLE PRECISION,
                    original_message_id BIGINT,
                    original_chat_id TEXT,
                    chat_id TEXT,
                    last_target_hit INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            await conn.execute(
                "ALTER TABLE signals ADD COLUMN IF NOT EXISTS original_message_id BIGINT"
            )
            await conn.execute(
                "ALTER TABLE signals ADD COLUMN IF NOT EXISTS timeframe TEXT NOT NULL DEFAULT '15m'"
            )
            await conn.execute(
                "ALTER TABLE signals ADD COLUMN IF NOT EXISTS rsi_snapshot DOUBLE PRECISION"
            )
            await conn.execute(
                "ALTER TABLE signals ADD COLUMN IF NOT EXISTS adx_snapshot DOUBLE PRECISION"
            )
            await conn.execute(
                "ALTER TABLE signals ADD COLUMN IF NOT EXISTS rel_volume_snapshot DOUBLE PRECISION"
            )
            await conn.execute(
                "ALTER TABLE signals ADD COLUMN IF NOT EXISTS original_chat_id TEXT"
            )
            await conn.execute(
                "ALTER TABLE signals ADD COLUMN IF NOT EXISTS chat_id TEXT"
            )
            await conn.execute(
                "ALTER TABLE signals ADD COLUMN IF NOT EXISTS last_target_hit INTEGER NOT NULL DEFAULT 0"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signals_symbol_time_pg ON signals(symbol, timestamp)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signals_status_pg ON signals(status)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_journal (
                    id BIGSERIAL PRIMARY KEY,
                    signal_id BIGINT UNIQUE,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL DEFAULT '15m',
                    side TEXT NOT NULL,
                    entry_price DOUBLE PRECISION NOT NULL,
                    exit_price DOUBLE PRECISION,
                    score INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    result_reason TEXT,
                    last_target_hit INTEGER NOT NULL DEFAULT 0,
                    rsi_snapshot DOUBLE PRECISION,
                    adx_snapshot DOUBLE PRECISION,
                    rel_volume_snapshot DOUBLE PRECISION,
                    closed_at TIMESTAMPTZ
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_journal_time ON trade_journal(timestamp DESC)"
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_users (
                    chat_id TEXT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    chat_type TEXT NOT NULL DEFAULT 'private',
                    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                """
                INSERT INTO bot_settings(key, value)
                VALUES ('BOT_ACTIVE', 'true')
                ON CONFLICT (key) DO NOTHING
                """
            )
            await conn.execute(
                """
                INSERT INTO bot_settings(key, value)
                VALUES
                    ('CHANNEL_VIP_ACTIVE', 'true'),
                    ('CHANNEL_VIP_PLUS_ACTIVE', 'true'),
                    ('DAILY_SUMMARY_ACTIVE', 'true'),
                    ('TARGET_REPLY_ACTIVE', 'true')
                ON CONFLICT (key) DO NOTHING
                """
            )

    async def recover_active_trades(self) -> List[dict]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, exchange, symbol, side, entry_price, stop_loss, tp1, tp2, tp3, score, status
                FROM signals
                WHERE status = 'Pending'
                ORDER BY id ASC
                """
            )
        active = [dict(r) for r in rows]
        if self.redis is not None:
            for trade in active:
                await self.redis.hset(
                    f"active_trade:{trade['id']}",
                    mapping={k: str(v) for k, v in trade.items()},
                )
        return active

    async def has_recent_signal(self, symbol: str, minutes: int = 30) -> bool:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT 1
                FROM signals
                WHERE symbol = $1
                  AND timestamp >= NOW() - make_interval(mins => $2::int)
                LIMIT 1
                """,
                symbol,
                int(minutes),
            )
        return row is not None

    async def save_signal(self, exchange_name: str, symbol: str, setup: dict) -> int:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            signal_id = await conn.fetchval(
                """
                INSERT INTO signals (
                    exchange, symbol, timeframe, side, entry_price, stop_loss, tp1, tp2, tp3, score,
                    rsi_snapshot, adx_snapshot, rel_volume_snapshot, status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'Pending')
                RETURNING id
                """,
                exchange_name,
                symbol,
                str(setup.get("timeframe", "15m")),
                setup["side"],
                float(setup["entry"]),
                float(setup["sl"]),
                float(setup["tp1"]),
                float(setup["tp2"]),
                float(setup["tp3"]),
                int(setup["score"]),
                float(setup.get("rsi", 50.0)),
                float(setup.get("adx", 20.0)),
                float(setup.get("rel_volume", 1.0)),
            )
        if self.redis is not None:
            await self.redis.hset(
                f"active_trade:{signal_id}",
                mapping={
                    "id": str(signal_id),
                    "exchange": exchange_name,
                    "symbol": symbol,
                    "side": setup["side"],
                    "entry_price": str(float(setup["entry"])),
                    "stop_loss": str(float(setup["sl"])),
                    "tp1": str(float(setup["tp1"])),
                    "tp2": str(float(setup["tp2"])),
                    "tp3": str(float(setup["tp3"])),
                    "score": str(int(setup["score"])),
                    "status": "Pending",
                    "last_target_hit": "0",
                },
            )
        return int(signal_id)

    async def fetch_pending_signals(self) -> List[dict]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    exchange,
                    symbol,
                    side,
                    timestamp,
                    entry_price,
                    stop_loss,
                    tp1,
                    tp2,
                    tp3,
                    last_target_hit,
                    original_message_id,
                    original_chat_id,
                    chat_id
                FROM signals
                WHERE status IN ('Pending', 'T1_DONE', 'T2_DONE')
                ORDER BY id ASC
                """
            )
        return [dict(r) for r in rows]

    async def update_signal_status(self, signal_id: int, status: str, last_price: float) -> None:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE signals SET status = $1, last_price = $2 WHERE id = $3",
                status,
                float(last_price),
                int(signal_id),
            )
            if status in ("Hit TP", "Hit SL", "Timed Out", "Replaced", "Expired"):
                row = await conn.fetchrow(
                    """
                    SELECT id, exchange, symbol, timeframe, side, entry_price, score,
                           COALESCE(last_target_hit, 0) AS last_target_hit,
                           rsi_snapshot, adx_snapshot, rel_volume_snapshot
                    FROM signals
                    WHERE id = $1
                    """,
                    int(signal_id),
                )
                if row:
                    await conn.execute(
                        """
                        INSERT INTO trade_journal (
                            signal_id, exchange, symbol, timeframe, side, entry_price, exit_price, score,
                            status, result_reason, last_target_hit, rsi_snapshot, adx_snapshot, rel_volume_snapshot, closed_at
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
                        ON CONFLICT (signal_id) DO UPDATE SET
                            exit_price = EXCLUDED.exit_price,
                            status = EXCLUDED.status,
                            result_reason = EXCLUDED.result_reason,
                            last_target_hit = EXCLUDED.last_target_hit,
                            closed_at = NOW()
                        """,
                        int(row["id"]),
                        str(row["exchange"]),
                        str(row["symbol"]),
                        str(row["timeframe"] or "15m"),
                        str(row["side"]),
                        float(row["entry_price"]),
                        float(last_price),
                        int(row["score"]),
                        str(status),
                        str(status),
                        int(row["last_target_hit"]),
                        float(row["rsi_snapshot"] or 50.0),
                        float(row["adx_snapshot"] or 20.0),
                        float(row["rel_volume_snapshot"] or 1.0),
                    )
        if self.redis is not None:
            await self.redis.delete(f"active_trade:{signal_id}")

    async def fetch_recent_trade_journal(self, limit: int = 50) -> List[dict]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT signal_id, exchange, symbol, timeframe, status, score,
                       COALESCE(rsi_snapshot, 50) AS rsi_snapshot
                FROM trade_journal
                ORDER BY timestamp DESC
                LIMIT $1
                """,
                int(limit),
            )
        return [dict(r) for r in rows]

    async def get_daily_count_and_lowest_pending(self, day_utc: str) -> Tuple[int, Optional[dict]]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            total = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM signals
                WHERE (timestamp AT TIME ZONE 'UTC')::date = $1::date
                """,
                str(day_utc),
            )
            lowest = await conn.fetchrow(
                """
                SELECT id, score, last_price, entry_price
                FROM signals
                WHERE (timestamp AT TIME ZONE 'UTC')::date = $1::date
                  AND status IN ('Pending', 'T1_DONE', 'T2_DONE')
                ORDER BY score ASC, id ASC
                LIMIT 1
                """,
                str(day_utc),
            )
        return int(total or 0), (dict(lowest) if lowest else None)

    async def update_target_progress(self, signal_id: int, target_hit: int, last_price: float) -> None:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        progress_status = "Pending"
        if int(target_hit) == 1:
            progress_status = "T1_DONE"
        elif int(target_hit) == 2:
            progress_status = "T2_DONE"
        elif int(target_hit) >= 3:
            progress_status = "Hit TP"
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE signals
                SET last_target_hit = GREATEST(COALESCE(last_target_hit, 0), $1),
                    last_price = $2,
                    status = $3
                WHERE id = $4
                """,
                int(target_hit),
                float(last_price),
                progress_status,
                int(signal_id),
            )
        if self.redis is not None:
            await self.redis.hset(
                f"active_trade:{signal_id}",
                mapping={
                    "last_target_hit": str(int(target_hit)),
                    "last_price": str(float(last_price)),
                    "status": progress_status,
                },
            )

    async def attach_original_message(
        self,
        signal_id: int,
        chat_id: str,
        message_id: int,
    ) -> None:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE signals
                SET original_chat_id = COALESCE(original_chat_id, $1),
                    chat_id = COALESCE(chat_id, $1),
                    original_message_id = COALESCE(original_message_id, $2)
                WHERE id = $3
                """,
                str(chat_id),
                int(message_id),
                int(signal_id),
            )
        if self.redis is not None:
            await self.redis.hset(
                f"active_trade:{signal_id}",
                mapping={
                    "original_chat_id": str(chat_id),
                    "chat_id": str(chat_id),
                    "original_message_id": str(int(message_id)),
                },
            )

    async def sync_active_price(self, exchange_name: str, symbol: str, price: float) -> None:
        if self.redis is None:
            return
        await self.redis.set(f"price:{exchange_name}:{symbol}", f"{float(price):.10f}", ex=900)

    async def cache_layer_diagnostics(self, signal_id: int, setup: dict) -> None:
        if self.redis is None:
            return
        payload = {
            "vip_ok": str(bool(setup.get("vip_ok", False))),
            "vip_plus_ok": str(bool(setup.get("vip_plus_ok", False))),
            "vip_layers": json.dumps(setup.get("vip_layer_lines", [])),
            "vip_plus_layers": json.dumps(setup.get("vip_plus_layer_lines", [])),
        }
        await self.redis.hset(f"signal_layers:{signal_id}", mapping=payload)
        await self.redis.expire(f"signal_layers:{signal_id}", 60 * 60 * 24)

    async def is_symbol_on_cooldown(self, exchange_name: str, symbol: str) -> bool:
        if self.redis is None:
            return False
        key = f"signal_cd:{exchange_name}:{symbol}"
        val = await self.redis.get(key)
        return val is not None

    async def mark_symbol_cooldown(self, exchange_name: str, symbol: str, cooldown_seconds: int) -> None:
        if self.redis is None:
            return
        key = f"signal_cd:{exchange_name}:{symbol}"
        await self.redis.set(key, "1", ex=max(60, int(cooldown_seconds)))

    async def count_signals_last_window(self, window_seconds: int) -> int:
        if self.redis is None:
            return 0
        key = "signal_throttle_window"
        now = int(time.time())
        min_score = now - int(window_seconds)
        await self.redis.zremrangebyscore(key, 0, min_score)
        count = await self.redis.zcard(key)
        await self.redis.expire(key, max(120, int(window_seconds) + 30))
        return int(count or 0)

    async def register_signal_send(self, exchange_name: str, symbol: str, window_seconds: int) -> None:
        if self.redis is None:
            return
        key = "signal_throttle_window"
        now = int(time.time())
        member = f"{now}:{exchange_name}:{symbol}:{now % 1000000}"
        await self.redis.zadd(key, {member: now})
        await self.redis.zremrangebyscore(key, 0, now - int(window_seconds))
        await self.redis.expire(key, max(120, int(window_seconds) + 30))

    async def is_pair_blacklisted(self, symbol: str) -> bool:
        if self.redis is None:
            return False
        return bool(await self.redis.get(f"pair_blacklist:{symbol}"))

    async def blacklist_pair(self, symbol: str, ttl_seconds: int = 86400) -> None:
        if self.redis is None:
            return
        await self.redis.set(f"pair_blacklist:{symbol}", "1", ex=max(300, int(ttl_seconds)))

    async def fetch_signals_for_day(self, day_utc) -> List[tuple]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        # Normalize input: asyncpg's $1::date adapter requires a real datetime.date,
        # not a 'YYYY-MM-DD' string. Accept either and coerce.
        if isinstance(day_utc, datetime):
            day_value = day_utc.date()
        elif isinstance(day_utc, date):
            day_value = day_utc
        elif isinstance(day_utc, str):
            day_value = datetime.strptime(day_utc, "%Y-%m-%d").date()
        else:
            raise TypeError(f"fetch_signals_for_day: unsupported type {type(day_utc).__name__}")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
                    exchange, symbol, side, entry_price, status, COALESCE(last_target_hit, 0) AS last_target_hit
                FROM signals
                WHERE (timestamp AT TIME ZONE 'UTC')::date = $1
                ORDER BY timestamp ASC
                """,
                day_value,
            )
        return [
            (
                r["timestamp"],
                r["exchange"],
                r["symbol"],
                r["side"],
                float(r["entry_price"]),
                r["status"],
                int(r["last_target_hit"]),
            )
            for r in rows
        ]

    async def fetch_recent_closed_statuses(self, limit: int = 40) -> List[str]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT status
                FROM signals
                WHERE status IN ('Hit TP', 'Hit SL')
                ORDER BY id DESC
                LIMIT $1
                """,
                int(limit),
            )
        return [str(r["status"]) for r in rows]

    async def upsert_bot_user(
        self,
        chat_id: str,
        username: str = "",
        first_name: str = "",
        chat_type: str = "private",
    ) -> None:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bot_users(chat_id, username, first_name, chat_type, last_seen)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (chat_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    chat_type = EXCLUDED.chat_type,
                    last_seen = NOW()
                """,
                str(chat_id),
                str(username or ""),
                str(first_name or ""),
                str(chat_type or "private"),
            )

    async def count_users(self) -> int:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            value = await conn.fetchval("SELECT COUNT(DISTINCT chat_id) FROM bot_users")
        return int(value or 0)

    async def list_broadcast_targets(self) -> List[str]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id FROM bot_users")
        return [str(r["chat_id"]) for r in rows]

    async def get_admin_stats(self) -> tuple[int, float]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            total_active = await conn.fetchval(
                "SELECT COUNT(*) FROM signals WHERE status IN ('Pending', 'T1_DONE', 'T2_DONE')"
            )
            total_win = await conn.fetchval("SELECT COUNT(*) FROM signals WHERE status = 'Hit TP'")
            total_closed = await conn.fetchval(
                "SELECT COUNT(*) FROM signals WHERE status IN ('Hit TP', 'Hit SL')"
            )
        active_i = int(total_active or 0)
        wins_i = int(total_win or 0)
        closed_i = int(total_closed or 0)
        win_rate = (wins_i / closed_i * 100.0) if closed_i > 0 else 0.0
        return active_i, win_rate

    async def set_bot_active(self, active: bool) -> None:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bot_settings(key, value, updated_at)
                VALUES ('BOT_ACTIVE', $1, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """,
                "true" if active else "false",
            )

    async def is_bot_active(self) -> bool:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            value = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'BOT_ACTIVE'")
        return str(value or "true").strip().lower() == "true"

    async def set_setting(self, key: str, value: str) -> None:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bot_settings(key, value, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """,
                str(key),
                str(value),
            )

    async def get_setting(self, key: str, default: str = "") -> str:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            value = await conn.fetchval("SELECT value FROM bot_settings WHERE key = $1", str(key))
        if value is None:
            return default
        return str(value)

    async def get_settings_map(self) -> Dict[str, str]:
        if self.pool is None:
            raise RuntimeError("Postgres pool is not initialized; call startup() first")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM bot_settings")
        return {str(r["key"]): str(r["value"]) for r in rows}


class FortressScanner:
    """Fortress Scanner with Postgres + Redis trade memory."""

    def __init__(self) -> None:
        self._async_loop = asyncio.new_event_loop()
        self._async_thread = threading.Thread(
            target=self._run_loop_forever,
            args=(self._async_loop,),
            daemon=True,
        )
        self._async_thread.start()

        self.scan_interval_seconds = int(os.getenv("SCAN_INTERVAL_SECONDS", "5"))
        self.max_symbols = 35
        self.alert_cooldown_seconds = 60 * 60
        self.last_alert_at: Dict[str, float] = {}
        # Signal pacing controls (anti-burst): spread signals over time.
        self.min_signal_gap_seconds = int(os.getenv("MIN_SIGNAL_GAP_SECONDS", "45"))
        self.max_signals_per_cycle = int(os.getenv("MAX_SIGNALS_PER_CYCLE", "6"))
        self.last_global_signal_sent_at = 0.0
        self.cycle_signal_sent_count = 0
        self.signal_pacing_lock = threading.Lock()
        self.scan_semaphore_size = int(os.getenv("SCAN_CONCURRENCY", "24"))
        self.fast_ticker_cache_ttl_sec = float(os.getenv("FAST_TICKER_CACHE_TTL_SEC", "2.0"))
        self.fast_ticker_cache: Dict[str, dict] = {}
        self.virtual_balance = float(os.getenv("VIRTUAL_BALANCE", "10000"))
        self.risk_per_trade_pct = float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))
        self.max_risk_per_trade_pct = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "2.0"))
        self.slippage_expire_pct = float(os.getenv("SLIPPAGE_EXPIRE_PCT", "0.5"))
        self.adx_trend_min = float(os.getenv("ADX_TREND_MIN", "22"))
        # Keep local SQLite only for adaptive learning state.
        self.db_path = self._resolve_learning_db_path()
        self.db_executor = ThreadPoolExecutor(max_workers=1)

        self.last_daily_report_date = datetime.now(UTC).date()
        self.il_tz = ZoneInfo("Asia/Jerusalem")
        self.last_daily_report_sent_date_utc: Optional[str] = None
        self.min_score_threshold = 5
        self.volume_spike_threshold = 1.3
        # Lifetime Telegram send counters (for logs only). VIP+ uses vip_plus_messages_sent.
        self.vip_plus_messages_sent = 0
        self.vip_channel_messages_sent = 0
        self.macro_cache: Dict[str, dict] = {}
        self.macro_cache_ttl_sec = 300
        self.sync_cache: Dict[str, dict] = {}
        self.sync_cache_ttl_sec = 90
        self.ext_cache: Dict[str, dict] = {}
        self.ext_cache_ttl_sec = 120
        self.vip_strict_mode = (os.getenv("VIP_STRICT_MODE", "true").strip().lower() != "false")
        self.aggressive_signal_mode = os.getenv("AGGRESSIVE_SIGNAL_MODE", "false").strip().lower() == "true"
        self.layer_timeout_ms = int(os.getenv("LAYER_TIMEOUT_MS", "1800"))
        self.whale_alert_api_key = os.getenv("WHALE_ALERT_API_KEY", "").strip()
        self.social_api_key = os.getenv("SOCIAL_API_KEY", "").strip()
        self.lunarcrush_api_key = (
            os.getenv("LUNARCRUSH_API_KEY", "").strip() or self.social_api_key
        )
        self.cmc_api_key = (
            os.getenv("CMC_API_KEY", "").strip() or os.getenv("COINMARKETCAP_API_KEY", "").strip()
        )
        self.daily_flow = {
            "date": datetime.now(UTC).strftime("%Y-%m-%d"),
            "chat_sent": 0,
            "vip_plus_sent": 0,
            "global_sent": 0,
        }
        # Daily delivery targets (can be overridden from env).
        self.daily_target_chat_signals = int(os.getenv("DAILY_TARGET_CHAT_SIGNALS", "25"))
        self.daily_target_vip_plus_signals = int(os.getenv("DAILY_TARGET_VIP_PLUS_SIGNALS", "15"))
        # Publish when setup confidence meets this threshold (default: 75%).
        self.signal_publish_threshold_pct = float(os.getenv("SIGNAL_PUBLISH_THRESHOLD_PCT", "75"))
        self.signal_publish_threshold_score = max(
            1,
            min(10, int(math.ceil(self.signal_publish_threshold_pct / 10.0))),
        )
        # Minimum target distances (raw %, before leverage) to block tiny-profit signals.
        self.min_tp1_pct = float(os.getenv("MIN_TP1_PCT", "0.8"))
        self.min_tp3_pct = float(os.getenv("MIN_TP3_PCT", "2.0"))
        self.global_daily_signal_limit = int(os.getenv("GLOBAL_DAILY_SIGNAL_LIMIT", "30"))
        self.symbol_cooldown_seconds = int(os.getenv("SYMBOL_COOLDOWN_SECONDS", str(4 * 60 * 60)))
        self.global_throttle_window_sec = int(os.getenv("GLOBAL_THROTTLE_WINDOW_SEC", "600"))
        self.global_throttle_max_signals = int(os.getenv("GLOBAL_THROTTLE_MAX_SIGNALS", "2"))
        self.strict_quality_score = int(os.getenv("STRICT_QUALITY_SCORE", "9"))
        self.min_tp1_distance_pct = float(os.getenv("MIN_TP1_DISTANCE_PCT", "1.5"))
        self.vip_min_confidence_pct = float(os.getenv("VIP_MIN_CONFIDENCE_PCT", "75"))
        self.vip_plus_min_confidence_pct = float(os.getenv("VIP_PLUS_MIN_CONFIDENCE_PCT", "89"))
        self.vip_plus_downgrade_pct = float(os.getenv("VIP_PLUS_DOWNGRADE_PCT", "85"))
        self.trade_timeout_minutes = int(os.getenv("TRADE_TIMEOUT_MINUTES", "360"))
        self.exchange_score_penalty: Dict[str, int] = {}
        self.timeframe_score_penalty: Dict[str, int] = {}
        self.rsi_weight = float(os.getenv("RSI_WEIGHT", "1.0"))
        print(
            f"[DELIVERY] publish threshold={self.signal_publish_threshold_pct:.1f}% "
            f"(score>={self.signal_publish_threshold_score}/10)"
        )
        print(f"[DELIVERY] aggressive_signal_mode={self.aggressive_signal_mode}")

        self.exchanges = {
            "Binance": ccxt.binance(
                {
                    "apiKey": os.getenv("BINANCE_API_KEY"),
                    "secret": os.getenv("BINANCE_API_SECRET"),
                    "enableRateLimit": True,
                    "options": {"defaultType": "spot"},
                }
            ),
            "Bybit": ccxt.bybit(
                {
                    "apiKey": os.getenv("BYBIT_API_KEY"),
                    "secret": os.getenv("BYBIT_API_SECRET"),
                    "enableRateLimit": True,
                    "options": {"defaultType": "linear"},
                }
            ),
            "OKX": ccxt.okx(
                {
                    "apiKey": os.getenv("OKX_API_KEY"),
                    "secret": os.getenv("OKX_API_SECRET"),
                    "password": os.getenv("OKX_API_PASSWORD"),
                    "enableRateLimit": True,
                    "options": {"defaultType": "swap"},
                }
            ),
        }

        self.token = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN") or "").strip()
        self.vip_token = (os.getenv("VIP_TELEGRAM_BOT_TOKEN") or self.token).strip()
        self.vip_plus_token = (os.getenv("VIP_PLUS_TELEGRAM_BOT_TOKEN") or self.vip_token).strip()
        self.chat_id = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()
        self.vip_plus_chat_id = os.getenv("VIP_PLUS_CHAT_ID", "").strip()
        self.admin_chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
        self.admin_user_id = os.getenv("ADMIN_ID", "").strip()
        # Alerts are private-only by policy:
        # 1) Prefer ADMIN_ID (private user id).
        # 2) If ADMIN_ID is missing, allow ADMIN_CHAT_ID only when it is a
        #    private numeric id (positive, not group/supergroup id).
        self.alert_chat_id = self.admin_user_id
        if not self.alert_chat_id and self.admin_chat_id and not self.admin_chat_id.startswith("-"):
            self.alert_chat_id = self.admin_chat_id
        self.force_dual_test_send = (
            os.getenv("FORCE_DUAL_TEST_SEND", "false").strip().lower() == "true"
        )
        self.openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.openai_client: Optional[OpenAI] = None
        if self.openai_api_key:
            try:
                self.openai_client = OpenAI(api_key=self.openai_api_key)
                print("[OPENAI] client ready")
            except Exception as exc:
                print(f"[OPENAI] init failed: {exc}")
        self.last_heartbeat_hour_il: Optional[str] = None
        if self.token and self.chat_id:
            print(f"[TG] main channel ok (TELEGRAM_CHAT_ID={self.chat_id})")
        else:
            print("[TG] missing TELEGRAM_BOT_TOKEN/TELEGRAM_TOKEN or TELEGRAM_CHAT_ID/CHAT_ID")
        if self.vip_plus_chat_id:
            print(f"[TG] VIP+ destination ok (VIP_PLUS_CHAT_ID={self.vip_plus_chat_id})")
        if self.admin_chat_id:
            print(f"[TG] ADMIN_CHAT_ID set={self.admin_chat_id} (alerts use ADMIN_ID when set)")
        if not self.alert_chat_id:
            print(
                "[ADMIN] private alerts disabled: set ADMIN_ID (recommended) "
                "or positive ADMIN_CHAT_ID. Group IDs are blocked for alerts."
            )
        self.admin_pending_broadcast = False
        self.telegram_update_offset = 0
        # Infra alert anti-spam: send at most one detailed alert per component
        # during the cooldown window.
        self.infra_alert_cooldown_sec = int(os.getenv("INFRA_ALERT_COOLDOWN_SEC", "900"))
        self._infra_alert_last_sent: Dict[str, float] = {}

        self.trade_manager = TradeManager()
        try:
            recovered = self._run_async_task(self.trade_manager.startup())
            print(f"[TRADE] persistence ready | open signals recovered: {len(recovered)}")

            self.init_db()
            self.load_learning_state()
            self.status_thread = threading.Thread(target=self.status_watcher_loop, daemon=True)
            self.status_thread.start()
            self.daily_report_thread = threading.Thread(target=self.daily_report_loop, daemon=True)
            self.daily_report_thread.start()
            self.admin_thread = threading.Thread(target=self.admin_interface_loop, daemon=True)
            self.admin_thread.start()
        except Exception as exc:
            self._notify_if_infra_error(exc, "startup")
            if not self._infra_component_from_error(str(exc)):
                self.send_admin_notification(f"Startup failure: {exc}", loud=True)
            raise

    # -------------------------
    # 0) Local Learning-State (SQLite)
    # -------------------------
    @staticmethod
    def _run_loop_forever(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def init_db(self) -> None:
        def _create() -> None:
            db_dir = os.path.dirname(os.path.abspath(self.db_path))
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "PRAGMA journal_mode=WAL"
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS learning_state (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        min_score_threshold INTEGER NOT NULL,
                        volume_spike_threshold REAL NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.commit()

        self.db_executor.submit(_create).result()
        print(f"[LEARN] sqlite state file ready path={self.db_path}")

    @staticmethod
    def _resolve_learning_db_path() -> str:
        """Pick a writable SQLite path across local/dev/cloud runtimes."""
        configured = os.getenv("LEARNING_DB_PATH", "").strip()
        candidates = []
        if configured:
            candidates.append(configured)
        candidates.extend(
            [
                os.path.join(os.getcwd(), "signals_log.db"),
                os.path.join(tempfile.gettempdir(), "signals_log.db"),
            ]
        )
        for candidate in candidates:
            try:
                db_dir = os.path.dirname(os.path.abspath(candidate))
                if db_dir:
                    os.makedirs(db_dir, exist_ok=True)
                with open(candidate, "a", encoding="utf-8"):
                    pass
                return candidate
            except Exception:
                continue
        # Last resort: this will still raise later, but includes explicit path in logs.
        return os.path.join(tempfile.gettempdir(), "signals_log.db")

    def send_admin_notification(self, text: str, loud: bool = True) -> None:
        target_chat_id = self.alert_chat_id
        if not self.token or not target_chat_id:
            return
        # Absolute guard: never send operational alerts to groups/channels.
        if str(target_chat_id).startswith("-"):
            print(f"[ADMIN] blocked alert to non-private chat id: {target_chat_id}")
            return
        prefix = "ALERT:" if loud else "INFO:"
        body = f"{prefix} {text}".strip()
        payload = {
            "chat_id": target_chat_id,
            "text": body,
            "disable_notification": (not loud),
        }
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json=payload,
                timeout=15,
            )
            if not resp.ok:
                print(f"[ADMIN] notify failed ({resp.status_code}): {resp.text}")
        except Exception as exc:
            print(f"[ADMIN] notify error: {exc}")

    def _infra_component_from_error(self, text: str) -> Optional[str]:
        low = text.lower()
        if any(k in low for k in ["5432", "postgres", "asyncpg", "database", "db_url", "db "]):
            return "database"
        if any(k in low for k in ["redis", "6379"]):
            return "redis"
        return None

    def _infra_alert_cooldown_minutes(self) -> int:
        """Whole minutes matching infra_alert_cooldown_sec (minimum 1 for messaging)."""
        return max(1, int(self.infra_alert_cooldown_sec // 60))

    def _claim_infra_alert_slot(self, dedupe_key: str) -> bool:
        """
        Return True if an infra alert may be sent now; record send time for anti-spam.
        """
        now = time.time()
        if (now - self._infra_alert_last_sent.get(dedupe_key, 0.0)) < self.infra_alert_cooldown_sec:
            return False
        self._infra_alert_last_sent[dedupe_key] = now
        return True

    @staticmethod
    def _clip_alert_detail(text: str, limit: int = 500) -> str:
        raw = (text or "").strip()
        if not raw:
            return "unknown"
        return raw[:limit]

    def notify_infra_issue_once(self, component: str, exc: Exception, context: str) -> None:
        if not self._claim_infra_alert_slot(component):
            return
        cooldown_min = self._infra_alert_cooldown_minutes()
        error_text = str(exc).strip() or exc.__class__.__name__
        message = (
            "Bot infrastructure issue (aggregated alert)\n"
            f"Component: {'PostgreSQL' if component == 'database' else 'Redis'}\n"
            f"Context: {context}\n"
            f"Error: {self._clip_alert_detail(error_text)}\n"
            "Auto-recovery: the bot will keep retrying on the next cycles.\n"
            f"Anti-spam: next alert in ~{cooldown_min} minutes."
        )
        self.send_admin_notification(message, loud=True)

    def _notify_if_infra_error(self, exc: Exception, context: str) -> None:
        component = self._infra_component_from_error(str(exc))
        if component:
            self.notify_infra_issue_once(component=component, exc=exc, context=context)

    def notify_worker_failure_once(
        self, exchange_name: str, failed: int, total: int, first_error: str
    ) -> None:
        dedupe_key = f"workers_{exchange_name.lower()}"
        if not self._claim_infra_alert_slot(dedupe_key):
            return
        cooldown_min = self._infra_alert_cooldown_minutes()
        msg = (
            "Scanner engine issue (aggregated)\n"
            f"Exchange: {exchange_name}\n"
            f"Failed workers: {failed}/{total}\n"
            f"First error: {self._clip_alert_detail(first_error)}\n"
            "Note: this alert is sent only to the admin private chat.\n"
            f"Anti-spam: next message in ~{cooldown_min} minutes."
        )
        self.send_admin_notification(msg, loud=True)

    def configure_admin_menu_button(self) -> None:
        if not self.token or not self.admin_user_id:
            return
        try:
            # Hard reset defaults for everyone: no custom commands/menu.
            requests.post(
                f"https://api.telegram.org/bot{self.token}/deleteMyCommands",
                json={},
                timeout=15,
            )
            requests.post(
                f"https://api.telegram.org/bot{self.token}/setChatMenuButton",
                json={"menu_button": {"type": "default"}},
                timeout=15,
            )
            # Telegram menu button does not support custom text label per chat;
            # we scope /admin command to admin chat only and enable commands menu there.
            requests.post(
                f"https://api.telegram.org/bot{self.token}/setMyCommands",
                json={
                    "commands": [{"command": "admin", "description": "Admin panel"}],
                    "scope": {"type": "chat", "chat_id": str(self.admin_user_id)},
                },
                timeout=15,
            )
            requests.post(
                f"https://api.telegram.org/bot{self.token}/setChatMenuButton",
                json={"chat_id": str(self.admin_user_id), "menu_button": {"type": "commands"}},
                timeout=15,
            )
        except Exception as exc:
            print(f"[ADMIN] menu setup error: {exc}")

    def _is_admin_message(self, msg: dict) -> bool:
        user_id = str((msg.get("from") or {}).get("id") or "")
        chat_id = str((msg.get("chat") or {}).get("id") or "")
        return user_id == str(self.admin_user_id) or chat_id == str(self.admin_user_id)

    def send_admin_dashboard(self) -> None:
        if not self.token or not self.admin_user_id:
            return
        settings = self._run_async_task(self.trade_manager.get_settings_map())
        vip_on = settings.get("CHANNEL_VIP_ACTIVE", "true").lower() == "true"
        vip_plus_on = settings.get("CHANNEL_VIP_PLUS_ACTIVE", "true").lower() == "true"
        daily_on = settings.get("DAILY_SUMMARY_ACTIVE", "true").lower() == "true"
        target_on = settings.get("TARGET_REPLY_ACTIVE", "true").lower() == "true"
        bot_on = settings.get("BOT_ACTIVE", "true").lower() == "true"
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "Stats", "callback_data": "admin_stats"},
                    {"text": "Users", "callback_data": "admin_users"},
                ],
                [{"text": "Emergency stop", "callback_data": "admin_stop"}],
                [
                    {"text": f"VIP: {'ON' if vip_on else 'OFF'}", "callback_data": "admin_toggle_vip"},
                    {
                        "text": f"VIP+: {'ON' if vip_plus_on else 'OFF'}",
                        "callback_data": "admin_toggle_vip_plus",
                    },
                ],
                [
                    {"text": f"Daily: {'ON' if daily_on else 'OFF'}", "callback_data": "admin_toggle_daily"},
                    {"text": f"Replies: {'ON' if target_on else 'OFF'}", "callback_data": "admin_toggle_target"},
                ],
                [
                    {"text": "Test VIP", "callback_data": "admin_test_vip"},
                    {"text": "Test VIP+", "callback_data": "admin_test_vip_plus"},
                ],
                [{"text": "Send daily now", "callback_data": "admin_send_daily_now"}],
                [{"text": "Broadcast", "callback_data": "admin_broadcast"}],
            ]
        }
        payload = {
            "chat_id": str(self.admin_user_id),
            "text": (
                "*Admin dashboard*\n"
                f"Bot: {'ENABLED' if bot_on else 'DISABLED'}\n"
                f"VIP={'ON' if vip_on else 'OFF'} | VIP+={'ON' if vip_plus_on else 'OFF'}\n"
                f"Daily={'ON' if daily_on else 'OFF'} | Replies={'ON' if target_on else 'OFF'}"
            ),
            "parse_mode": "Markdown",
            "reply_markup": keyboard,
        }
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json=payload,
                timeout=20,
            )
        except Exception as exc:
            print(f"[ADMIN] dashboard send error: {exc}")

    def _admin_send_text(self, chat_id: str, text: str, *, timeout: float = 20) -> None:
        """Resilient sendMessage for admin replies. Logs and swallows transient failures
        so a single Telegram blip never aborts a callback handler mid-flow."""
        if not self.token:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": str(chat_id), "text": text},
                timeout=timeout,
            )
        except Exception as exc:
            print(f"[TG] admin reply failed chat={chat_id}: {exc}")

    def handle_admin_callback(self, query: dict) -> None:
        callback_id = query.get("id")
        data = str(query.get("data") or "")
        msg = query.get("message") or {}
        if not msg:
            return
        if not self._is_admin_message({"from": query.get("from"), "chat": msg.get("chat", {})}):
            return

        chat_id = str((msg.get("chat") or {}).get("id") or self.admin_user_id)
        if data == "admin_stats":
            total_active, win_rate = self._run_async_task(self.trade_manager.get_admin_stats())
            self._admin_send_text(chat_id, f"Stats\nActive trades: {total_active}\nWin rate: {win_rate:.2f}%")
        elif data == "admin_users":
            users_count = self._run_async_task(self.trade_manager.count_users())
            self._admin_send_text(chat_id, f"Unique users: {users_count}")
        elif data == "admin_stop":
            active = self._run_async_task(self.trade_manager.is_bot_active())
            new_state = not active
            self._run_async_task(self.trade_manager.set_bot_active(new_state))
            self._admin_send_text(chat_id, f"Bot: {'ENABLED' if new_state else 'DISABLED'}")
        elif data == "admin_broadcast":
            self.admin_pending_broadcast = True
            self._admin_send_text(chat_id, "Send your broadcast message now.")
        elif data == "admin_toggle_vip":
            v = self._run_async_task(self.trade_manager.get_setting("CHANNEL_VIP_ACTIVE", "true")).lower() == "true"
            self._run_async_task(self.trade_manager.set_setting("CHANNEL_VIP_ACTIVE", "false" if v else "true"))
            self._admin_send_text(chat_id, f"VIP channel {'OFF' if v else 'ON'}")
            self.send_admin_dashboard()
        elif data == "admin_toggle_vip_plus":
            v = self._run_async_task(self.trade_manager.get_setting("CHANNEL_VIP_PLUS_ACTIVE", "true")).lower() == "true"
            self._run_async_task(self.trade_manager.set_setting("CHANNEL_VIP_PLUS_ACTIVE", "false" if v else "true"))
            self._admin_send_text(chat_id, f"VIP+ channel {'OFF' if v else 'ON'}")
            self.send_admin_dashboard()
        elif data == "admin_toggle_daily":
            v = self._run_async_task(self.trade_manager.get_setting("DAILY_SUMMARY_ACTIVE", "true")).lower() == "true"
            self._run_async_task(self.trade_manager.set_setting("DAILY_SUMMARY_ACTIVE", "false" if v else "true"))
            self._admin_send_text(chat_id, f"Daily report {'OFF' if v else 'ON'}")
            self.send_admin_dashboard()
        elif data == "admin_toggle_target":
            v = self._run_async_task(self.trade_manager.get_setting("TARGET_REPLY_ACTIVE", "true")).lower() == "true"
            self._run_async_task(self.trade_manager.set_setting("TARGET_REPLY_ACTIVE", "false" if v else "true"))
            self._admin_send_text(chat_id, f"Target replies {'OFF' if v else 'ON'}")
            self.send_admin_dashboard()
        elif data == "admin_test_vip":
            self._admin_send_text(str(self.chat_id), "Admin VIP test ping")
            self._admin_send_text(chat_id, "VIP test sent")
        elif data == "admin_test_vip_plus":
            self._admin_send_text(str(self.vip_plus_chat_id), "Admin VIP+ test ping")
            self._admin_send_text(chat_id, "VIP+ test sent")
        elif data == "admin_send_daily_now":
            day = datetime.now(UTC).strftime("%Y-%m-%d")
            self.send_daily_performance_report(day)
            self._admin_send_text(chat_id, "Daily report sent")

        if callback_id:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                    json={"callback_query_id": callback_id, "text": "OK"},
                    timeout=10,
                )
            except Exception as exc:
                print(f"[TG] answerCallbackQuery failed: {exc}")

    def handle_admin_broadcast(self, text: str) -> None:
        targets = set(self._run_async_task(self.trade_manager.list_broadcast_targets()))
        if self.chat_id:
            targets.add(str(self.chat_id))
        if self.vip_plus_chat_id:
            targets.add(str(self.vip_plus_chat_id))
        sent = 0
        for chat in targets:
            try:
                resp = requests.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json={"chat_id": chat, "text": text},
                    timeout=15,
                )
                if resp.ok:
                    sent += 1
            except Exception as exc:
                print(f"[TG] broadcast send failed chat={chat} err={exc}")
                continue
        self._admin_send_text(str(self.admin_user_id), f"Broadcast delivered to {sent} chats.")

    def admin_interface_loop(self) -> None:
        if not self.token:
            return
        self.configure_admin_menu_button()
        while True:
            try:
                params = {"timeout": 30}
                if self.telegram_update_offset > 0:
                    params["offset"] = self.telegram_update_offset
                resp = requests.get(
                    f"https://api.telegram.org/bot{self.token}/getUpdates",
                    params=params,
                    timeout=35,
                )
                if not resp.ok:
                    time.sleep(2)
                    continue
                payload = resp.json()
                if not payload.get("ok"):
                    time.sleep(2)
                    continue
                for upd in payload.get("result", []):
                    self.telegram_update_offset = int(upd.get("update_id", 0)) + 1
                    msg = upd.get("message")
                    cb = upd.get("callback_query")
                    if cb:
                        chat = (cb.get("message") or {}).get("chat", {})
                        frm = cb.get("from") or {}
                        self._run_async_task(
                            self.trade_manager.upsert_bot_user(
                                chat_id=str(chat.get("id") or frm.get("id") or ""),
                                username=str(frm.get("username") or ""),
                                first_name=str(frm.get("first_name") or ""),
                                chat_type=str(chat.get("type") or "private"),
                            )
                        )
                        self.handle_admin_callback(cb)
                        continue
                    if not msg:
                        continue
                    chat = msg.get("chat") or {}
                    frm = msg.get("from") or {}
                    self._run_async_task(
                        self.trade_manager.upsert_bot_user(
                            chat_id=str(chat.get("id") or ""),
                            username=str(frm.get("username") or ""),
                            first_name=str(frm.get("first_name") or ""),
                            chat_type=str(chat.get("type") or "private"),
                        )
                    )
                    text = str(msg.get("text") or "")
                    if self._is_admin_message(msg):
                        if text.startswith("/admin"):
                            self.send_admin_dashboard()
                        elif self.admin_pending_broadcast and text and (not text.startswith("/")):
                            self.admin_pending_broadcast = False
                            self.handle_admin_broadcast(text)
                    # Security: ignore /admin from everyone else completely.
            except Exception as exc:
                print(f"[ADMIN] interface loop error: {exc}")
                time.sleep(3)

    def style_signal_message(self, raw_text: str, tier: str) -> str:
        """
        Optional AI polish layer for readability and grammar.
        Falls back to original text if OpenAI is unavailable.
        """
        if not self.openai_client:
            return raw_text
        try:
            prompt = (
                "You are a professional crypto signal copy editor. "
                "Rewrite the message to be clean, compact, and visually aligned for Telegram. "
                "Preserve all numbers and trading values exactly. "
                "Do not invent metrics. Keep concise separators and strong readability. "
                "Do not use markdown/html tags. Return plain text only. "
                f"Tier={tier}\n\n"
                f"Original message:\n{raw_text}"
            )
            resp = self.openai_client.responses.create(
                model="gpt-4o-mini",
                input=prompt,
                max_output_tokens=900,
            )
            out = (resp.output_text or "").strip()
            if not out:
                return raw_text
            if tier == "VIP_PLUS":
                return self.ensure_vip_plus_separators(out)
            return out
        except Exception as exc:
            print(f"[OPENAI] styling failed: {exc}")
            return raw_text

    def build_technical_analysis_blocks(self, symbol: str, side_caps: str, setup: dict) -> Dict[str, str]:
        entry = float(setup.get("entry", 0.0))
        ema20 = float(setup.get("ema20", 0.0))
        ema50 = float(setup.get("ema50", 0.0))
        ema200 = float(setup.get("ema200", 0.0))
        pivot = float(setup.get("pivot", 0.0))
        s1 = float(setup.get("s1", 0.0))
        r1 = float(setup.get("r1", 0.0))
        rsi = float(setup.get("rsi", 50.0))
        adx = float(setup.get("adx", 20.0))
        rel_vol = float(setup.get("rel_volume", 1.0))
        macd = float(setup.get("macd", 0.0))
        macd_signal = float(setup.get("macd_signal", 0.0))
        sl = float(setup.get("sl", 0.0))
        structure_bias = str(setup.get("structure_bias", "BOS continuation"))

        zone_context = "demand-zone retest" if side_caps == "LONG" else "supply-zone retest"
        context_fallback = (
            f"{side_caps} continuation setup with {structure_bias} on 15m, aligned with 4H trend and "
            f"{zone_context} near {entry:,.2f}."
        )
        levels_fallback = (
            f"EMA20 {ema20:,.2f}, EMA50 {ema50:,.2f}, EMA200 {ema200:,.2f}; "
            f"pivot {pivot:,.2f}, S1 {s1:,.2f}, R1 {r1:,.2f} define floors and ceilings."
        )
        macd_state = "bullish crossover" if macd >= macd_signal else "bearish crossover"
        indicators_fallback = (
            f"RSI {rsi:.2f}, ADX {adx:.2f}, MACD {macd:.4f} vs signal {macd_signal:.4f} ({macd_state}); "
            f"relative volume at {rel_vol:.2f}x confirms participation."
        )
        risk_fallback = (
            f"Setup invalidates on sustained trade through stop-loss {sl:,.2f}; monitor for momentum failure, "
            f"volume contraction, or a decisive EMA50/EMA200 breach."
        )
        fallback = {
            "context_detailed": context_fallback,
            "levels_detailed": levels_fallback,
            "indicators_detailed": indicators_fallback,
            "risk_detailed": risk_fallback,
        }
        if not self.openai_client:
            return fallback

        try:
            prompt = (
                "You are a senior hedge-fund crypto analyst. Draft a professional, concise technical breakdown "
                "for a Telegram signal. Return strict JSON with keys: context_detailed, levels_detailed, "
                "indicators_detailed, risk_detailed. Each value must be one sentence (max 220 chars), factual, "
                "no hype, no markdown, and aligned with the provided data.\n\n"
                f"Symbol: {symbol}\n"
                f"Side: {side_caps}\n"
                f"Entry: {entry:.6f}\n"
                f"Stop: {sl:.6f}\n"
                f"Structure: {structure_bias}\n"
                f"EMAs: 20={ema20:.6f}, 50={ema50:.6f}, 200={ema200:.6f}\n"
                f"Pivot levels: pivot={pivot:.6f}, s1={s1:.6f}, r1={r1:.6f}\n"
                f"Indicators: RSI={rsi:.4f}, ADX={adx:.4f}, MACD={macd:.6f}, MACD_signal={macd_signal:.6f}, rel_volume={rel_vol:.4f}x\n"
                f"Liquidity status: {setup.get('liquidity_status', 'N/A')}\n"
            )
            resp = self.openai_client.responses.create(
                model="gpt-4o-mini",
                input=prompt,
                max_output_tokens=450,
            )
            raw = (resp.output_text or "").strip()
            parsed = json.loads(raw)
            required = ("context_detailed", "levels_detailed", "indicators_detailed", "risk_detailed")
            for key in required:
                if not isinstance(parsed.get(key), str) or not parsed[key].strip():
                    return fallback
            return {k: str(parsed[k]).strip() for k in required}
        except Exception as exc:
            print(f"[OPENAI] technical analysis generation failed: {exc}")
            return fallback

    @staticmethod
    def ensure_vip_plus_separators(text: str) -> str:
        """
        Ensure visible separators survive AI rewriting for VIP+ readability.
        """
        sep = "━━━━━━━━━━━━━━"
        sections = [
            "VIP+ EXTENSIONS",
            "Technical Analysis",
            "Liquidity (Multi-Horizon)",
            "Risk / Reward",
            "Volume Snapshot",
            "Macro Environment",
            "Synchronicity Check",
            "VIP+ Layer Audit (1-8)",
            "Social Pulse",
            "Technical Edge",
            "CMC Market Context",
        ]
        out = text
        for s in sections:
            idx = out.find(s)
            if idx <= 0:
                continue
            prev_chunk = out[max(0, idx - 40):idx]
            if sep not in prev_chunk:
                out = out[:idx] + f"{sep}\n\n" + out[idx:]
        # Normalize noisy separator patterns and whitespace around emoji/title lines.
        out = re.sub(rf"(?:{sep}\s*){{2,}}", f"{sep}\n\n", out)
        out = re.sub(r"\n{3,}", "\n\n", out)
        # If a section line starts with emoji + title, keep exactly one separator before it.
        out = re.sub(
            rf"\n*(?:{sep}\n\n)?((?:✨|📊|💧|⚖️|📈|🌐|🔄|🧪|🌍|🧠|🏛️)\s*<b>[^\\n]+</b>)",
            rf"\n\n{sep}\n\n\1",
            out,
        )
        # Remove separator if it appears directly after a bullet/content line symbol.
        out = re.sub(r"([:|])\s*\n\n" + sep, r"\1\n\n" + sep, out)
        out = out.strip()
        return out

    def load_learning_state(self) -> None:
        def _load() -> Optional[tuple]:
            with sqlite3.connect(self.db_path) as conn:
                return conn.execute(
                    "SELECT min_score_threshold, volume_spike_threshold FROM learning_state WHERE id = 1"
                ).fetchone()

        row = self.db_executor.submit(_load).result()
        if row:
            self.min_score_threshold = int(row[0])
            self.volume_spike_threshold = float(row[1])
        # Keep startup thresholds practical for flow (avoid over-strict legacy values).
        self.min_score_threshold = max(5, min(self.min_score_threshold, 6))
        self.volume_spike_threshold = max(1.2, min(self.volume_spike_threshold, 1.6))
        print(
            f"[LEARN] thresholds loaded | min_score={self.min_score_threshold} | rel_vol={self.volume_spike_threshold:.2f}x"
        )

    def save_learning_state(self) -> None:
        updated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        def _save() -> None:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO learning_state (id, min_score_threshold, volume_spike_threshold, updated_at)
                    VALUES (1, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        min_score_threshold = excluded.min_score_threshold,
                        volume_spike_threshold = excluded.volume_spike_threshold,
                        updated_at = excluded.updated_at
                    """,
                    (self.min_score_threshold, self.volume_spike_threshold, updated_at),
                )
                conn.commit()

        self.db_executor.submit(_save).result()

    def has_recent_signal(self, symbol: str, minutes: int = 30) -> bool:
        return bool(
            self._run_async_task(self.trade_manager.has_recent_signal(symbol=symbol, minutes=minutes))
        )

    def save_signal(self, exchange_name: str, symbol: str, setup: dict) -> int:
        signal_id = self._run_async_task(
            self.trade_manager.save_signal(exchange_name=exchange_name, symbol=symbol, setup=setup)
        )
        print(f"[SIGNAL] saved id={signal_id} {exchange_name} {symbol} {setup['side']}")
        return signal_id

    def update_pending_signal_statuses(self) -> None:
        pending_rows = self._run_async_task(self.trade_manager.fetch_pending_signals())
        if not pending_rows:
            return

        for row in pending_rows:
            signal_id = int(row["id"])
            exchange_name = row["exchange"]
            symbol = row["symbol"]
            side = row["side"]
            stop_loss = float(row["stop_loss"])
            tp1 = float(row["tp1"])
            tp2 = float(row["tp2"])
            tp3 = float(row["tp3"])
            last_target_hit = int(row.get("last_target_hit") or 0)
            ex = self.exchanges.get(exchange_name)
            if not ex:
                continue
            try:
                ticker = ex.fetch_ticker(symbol)
                current_price = ticker.get("last")
                if current_price is None:
                    continue
                current_price = float(current_price)
                self._run_async_task(
                    self.trade_manager.sync_active_price(exchange_name, symbol, current_price)
                )
            except Exception as exc:
                print(f"[SIGNAL] sync_active_price failed symbol={symbol} err={exc}")
                continue

            new_status = None
            target_hit = 0
            if side == "LONG":
                if current_price <= float(stop_loss):
                    new_status = "Hit SL"
                else:
                    if current_price >= float(tp1):
                        target_hit = 1
                    if current_price >= float(tp2):
                        target_hit = 2
                    if current_price >= float(tp3):
                        target_hit = 3
                    if target_hit >= 3:
                        new_status = "Hit TP"
            else:
                if current_price >= float(stop_loss):
                    new_status = "Hit SL"
                else:
                    if current_price <= float(tp1):
                        target_hit = 1
                    if current_price <= float(tp2):
                        target_hit = 2
                    if current_price <= float(tp3):
                        target_hit = 3
                    if target_hit >= 3:
                        new_status = "Hit TP"

            if target_hit > last_target_hit:
                for t in range(last_target_hit + 1, target_hit + 1):
                    self.send_target_hit_update(
                        row=row,
                        target_number=t,
                        current_price=current_price,
                        leverage="x10",
                    )
                self._run_async_task(
                    self.trade_manager.update_target_progress(
                        signal_id=int(signal_id),
                        target_hit=int(target_hit),
                        last_price=float(current_price),
                    )
                )
                print(f"[SIGNAL] id={signal_id} reached TP level T{target_hit}")

            if not new_status:
                ts = row.get("timestamp")
                if isinstance(ts, datetime):
                    opened_at = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
                else:
                    opened_at = datetime.now(UTC)
                age_minutes = (datetime.now(UTC) - opened_at).total_seconds() / 60.0
                if age_minutes >= float(self.trade_timeout_minutes):
                    new_status = "Timed Out"
                    print(f"[SIGNAL] id={signal_id} timed out after {age_minutes:.0f}m")
                else:
                    continue

            self._run_async_task(
                self.trade_manager.update_signal_status(
                    signal_id=int(signal_id),
                    status=new_status,
                    last_price=float(current_price),
                )
            )
            print(f"[SIGNAL] id={signal_id} status -> {new_status}")

    def adaptive_learning_step(self) -> None:
        """Self-tune thresholds based on the last 50 closed trades."""
        trades = self._run_async_task(self.trade_manager.fetch_recent_trade_journal(limit=50))
        if len(trades) < 10:
            return

        statuses = [str(t.get("status") or "") for t in trades]
        wins = sum(1 for s in statuses if s == "Hit TP")
        win_rate = wins / len(statuses)
        old_score = self.min_score_threshold
        old_vol = self.volume_spike_threshold

        if win_rate < 0.45:
            self.min_score_threshold = min(9, self.min_score_threshold + 1)
            self.volume_spike_threshold = min(3.0, self.volume_spike_threshold + 0.1)
        elif win_rate > 0.60:
            self.min_score_threshold = max(5, self.min_score_threshold - 1)
            self.volume_spike_threshold = max(1.6, self.volume_spike_threshold - 0.1)

        if old_score != self.min_score_threshold or abs(old_vol - self.volume_spike_threshold) > 1e-9:
            self.save_learning_state()
            print(
                f"[LEARN] tuned thresholds | win_rate={win_rate:.2%} | min_score {old_score}->{self.min_score_threshold} | rel_vol {old_vol:.2f}->{self.volume_spike_threshold:.2f}"
            )
        # Per-exchange / timeframe penalty model.
        exch_stats: Dict[str, List[str]] = {}
        tf_stats: Dict[str, List[str]] = {}
        for t in trades:
            exch = str(t.get("exchange") or "unknown")
            tf = str(t.get("timeframe") or "15m")
            exch_stats.setdefault(exch, []).append(str(t.get("status") or ""))
            tf_stats.setdefault(tf, []).append(str(t.get("status") or ""))
        new_ex_pen: Dict[str, int] = {}
        for exch, vals in exch_stats.items():
            if len(vals) < 6:
                continue
            fail_rate = sum(1 for v in vals if v != "Hit TP") / len(vals)
            if fail_rate >= 0.60:
                new_ex_pen[exch] = 1
            if fail_rate >= 0.72:
                new_ex_pen[exch] = 2
        new_tf_pen: Dict[str, int] = {}
        for tf, vals in tf_stats.items():
            if len(vals) < 6:
                continue
            fail_rate = sum(1 for v in vals if v != "Hit TP") / len(vals)
            if fail_rate >= 0.60:
                new_tf_pen[tf] = 1
            if fail_rate >= 0.72:
                new_tf_pen[tf] = 2
        self.exchange_score_penalty = new_ex_pen
        self.timeframe_score_penalty = new_tf_pen
        # RSI de-weight if it correlates with false signals.
        rsi_vals = [float(t.get("rsi_snapshot") or 50.0) for t in trades]
        losing_idx = [i for i, s in enumerate(statuses) if s == "Hit SL"]
        if losing_idx:
            rsi_extreme_losses = sum(1 for i in losing_idx if abs(rsi_vals[i] - 50.0) >= 15.0)
            ratio = rsi_extreme_losses / len(losing_idx)
            if ratio >= 0.55:
                self.rsi_weight = max(0.5, self.rsi_weight - 0.1)
            elif ratio <= 0.35:
                self.rsi_weight = min(1.0, self.rsi_weight + 0.05)
        print(
            f"[LEARN] penalties ex={self.exchange_score_penalty} tf={self.timeframe_score_penalty} "
            f"rsi_weight={self.rsi_weight:.2f}"
        )
        # Pair scorecard: auto-blacklist weak pairs for 24h.
        pair_stats: Dict[str, List[str]] = {}
        for t in trades:
            sym = str(t.get("symbol") or "")
            if not sym:
                continue
            pair_stats.setdefault(sym, []).append(str(t.get("status") or ""))
        for sym, vals in pair_stats.items():
            if len(vals) < 6:
                continue
            sl_ratio = sum(1 for v in vals if v == "Hit SL") / len(vals)
            if sl_ratio >= 0.5:
                try:
                    self._run_async_task(self.trade_manager.blacklist_pair(sym, ttl_seconds=24 * 60 * 60))
                    print(f"[LEARN] blacklisted {sym} for 24h (SL ratio={sl_ratio:.2f})")
                except Exception as exc:
                    print(f"[LEARN] blacklist failed for {sym}: {exc}")

    def status_watcher_loop(self) -> None:
        while True:
            try:
                self.update_pending_signal_statuses()
                self.adaptive_learning_step()
            except Exception as exc:
                print(f"[LOOP] status_watcher error: {exc}")
                self._notify_if_infra_error(exc, "status_watcher_loop")
                if not self._infra_component_from_error(str(exc)):
                    self.send_admin_notification(f"Status watcher error: {exc}", loud=True)
            time.sleep(300)

    def fetch_signals_for_day(self, day_utc: str) -> List[tuple]:
        return self._run_async_task(self.trade_manager.fetch_signals_for_day(day_utc))

    def evaluate_signal_result(
        self, exchange_name: str, symbol: str, side: str, entry_price: float, status: str
    ) -> Optional[Tuple[float, str]]:
        ex = self.exchanges.get(exchange_name)
        if not ex:
            return None
        if entry_price <= 0:
            return None
        try:
            ticker = ex.fetch_ticker(symbol)
            current_price = ticker.get("last")
            if current_price is None:
                return None
            current_price = float(current_price)
        except Exception:
            return None

        # Daily performance should reflect actual market change since alert time.
        move_pct = ((current_price - float(entry_price)) / float(entry_price)) * 100
        # Guardrail: unrealistic jumps are usually bad source data (demo/test rows).
        if abs(move_pct) > 300:
            return None
        if status == "Hit TP":
            verdict = "✅"
        elif status == "Hit SL":
            verdict = "❌"
        else:
            if side == "LONG":
                verdict = "✅" if move_pct >= 0 else "❌"
            else:
                verdict = "✅" if move_pct <= 0 else "❌"
        return move_pct, verdict

    def send_daily_performance_report(self, day_utc: str) -> None:
        if not self.token:
            return
        if self._run_async_task(self.trade_manager.get_setting("DAILY_SUMMARY_ACTIVE", "true")).strip().lower() != "true":
            print("[REPORT] skip | DAILY_SUMMARY_ACTIVE=false")
            return

        rows = self.fetch_signals_for_day(day_utc)
        if not rows:
            print(f"[REPORT] no rows for day={day_utc}")
            return

        entries: List[str] = []
        total_hits = 0
        total_missed = 0
        for _timestamp, exchange_name, symbol, side, entry_price, status, last_target_hit in rows:
            coin = str(symbol).replace("/USDT", "")
            result = self.evaluate_signal_result(
                exchange_name=exchange_name,
                symbol=symbol,
                side=side,
                entry_price=float(entry_price),
                status=status,
            )
            is_hit = int(last_target_hit) >= 1
            if result is None:
                status_emoji = "✅" if is_hit else "❌"
                entries.append(f"{coin} | {float(entry_price):.3f} | N/A% {status_emoji}")
                if status_emoji == "✅":
                    total_hits += 1
                else:
                    total_missed += 1
                continue

            move_pct, verdict = result
            status_emoji = "✅" if is_hit else "❌"
            # Required logic: ✅ if any target hit; otherwise missed/losing state is ❌.
            if not is_hit and status == "Hit SL":
                status_emoji = "❌"
            elif not is_hit and verdict == "❌":
                status_emoji = "❌"
            entries.append(f"{coin} | {float(entry_price):.3f} | {move_pct:+.2f}% {status_emoji}")
            if status_emoji == "✅":
                total_hits += 1
            else:
                total_missed += 1

        report_body = "\n".join(entries)
        total = total_hits + total_missed
        win_rate = (total_hits / total * 100.0) if total > 0 else 0.0
        report_text = (
            "📊 <b>DAILY SUMMARY</b>\n"
            f"<b>{day_utc}</b>\n"
            "<pre>"
            f"{report_body}\n"
            "</pre>\n"
            "Summary:\n"
            f"✅ Hits: {total_hits}\n"
            f"❌ Missed: {total_missed}\n"
            f"💰 Win Rate: {win_rate:.2f}%"
        )
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        target_chats = []
        if self.chat_id:
            target_chats.append(self.chat_id)
        if self.vip_plus_chat_id and self.vip_plus_chat_id not in target_chats:
            target_chats.append(self.vip_plus_chat_id)

        if not target_chats:
            print("[REPORT] skip | no TELEGRAM_CHAT_ID / broadcast targets configured")
            return

        success_count = 0
        for target_chat in target_chats:
            try:
                response = requests.post(
                    url,
                    json={"chat_id": target_chat, "text": report_text, "parse_mode": "HTML"},
                    timeout=20,
                )
                if response.ok:
                    success_count += 1
                    print(f"[REPORT] daily report sent to {target_chat} for {day_utc}")
                else:
                    print(f"[REPORT] failed for {target_chat} ({response.status_code}): {response.text}")
            except Exception as exc:
                print(f"[REPORT] send error for {target_chat}: {exc}")

        if success_count == 0:
            print(f"[REPORT] daily report failed for all targets on {day_utc}")

    def daily_report_loop(self) -> None:
        while True:
            try:
                now_il = datetime.now(self.il_tz)
                hour_key = now_il.strftime("%Y-%m-%d %H")
                if now_il.minute == 0 and self.last_heartbeat_hour_il != hour_key:
                    self.send_admin_notification(
                        f"Hourly heartbeat ({now_il.strftime('%Y-%m-%d %H:00')} IL): scanner healthy",
                        loud=False,
                    )
                    self.last_heartbeat_hour_il = hour_key
                # Daily summary schedule: 22:00 IL on odd weekday set:
                # Sunday, Tuesday, Thursday, Saturday.
                # Python weekday(): Monday=0 ... Sunday=6
                allowed_weekdays = {6, 1, 3, 5}
                if now_il.hour == 22 and now_il.minute == 0 and now_il.weekday() in allowed_weekdays:
                    report_day = now_il.strftime("%Y-%m-%d")
                    if self.last_daily_report_sent_date_utc != report_day:
                        self.send_daily_performance_report(report_day)
                        self.last_daily_report_sent_date_utc = report_day
            except Exception as exc:
                print(f"[REPORT] loop error: {exc}")
                self._notify_if_infra_error(exc, "daily_report_loop")
                if not self._infra_component_from_error(str(exc)):
                    self.send_admin_notification(f"Daily/heartbeat loop error: {exc}", loud=True)
            time.sleep(60)

    # -------------------------
    # 1) High-Speed Data Engine
    # -------------------------
    def top_usdt_symbols(self, exchange_name: str, ex: ccxt.Exchange) -> List[str]:
        try:
            tickers = ex.fetch_tickers()
            ranked = []
            fallback_symbols: List[str] = []
            for symbol, info in tickers.items():
                if "/USDT" not in symbol:
                    continue
                if ":" in symbol:
                    continue
                quote_volume = info.get("quoteVolume") or 0
                base_volume = info.get("baseVolume") or 0
                last_price = info.get("last") or info.get("close") or 0
                est_quote_vol = float(base_volume) * float(last_price) if base_volume and last_price else 0
                resolved_vol = float(quote_volume) if quote_volume and float(quote_volume) > 0 else est_quote_vol
                if resolved_vol > 0:
                    ranked.append((symbol, resolved_vol))
                else:
                    fallback_symbols.append(symbol)
            ranked.sort(key=lambda x: x[1], reverse=True)
            symbols = [s for s, _ in ranked[: self.max_symbols]]
            if len(symbols) < self.max_symbols and fallback_symbols:
                needed = self.max_symbols - len(symbols)
                symbols.extend(fallback_symbols[:needed])
            print(f"[SCAN] {exchange_name} | universe size={len(symbols)} USDT pairs (volume-ranked)")
            return symbols
        except Exception as exc:
            # If auth keys are invalid or restricted, retry once with public market-data client.
            err_text = str(exc)
            auth_hint = any(
                hint in err_text.lower()
                for hint in ["invalid api-key", "api-key", "authentication", "permission", "auth"]
            )
            if auth_hint:
                try:
                    public_factory = {
                        "Binance": ccxt.binance,
                        "Bybit": ccxt.bybit,
                        "OKX": ccxt.okx,
                    }.get(exchange_name)
                    if public_factory:
                        public_ex = public_factory({"enableRateLimit": True})
                        tickers = public_ex.fetch_tickers()
                        ranked: List[Tuple[str, float]] = []
                        fallback_symbols: List[str] = []
                        for symbol, info in tickers.items():
                            if not symbol.endswith("/USDT"):
                                continue
                            if ":" in symbol:
                                continue
                            quote_volume = info.get("quoteVolume") or 0
                            base_volume = info.get("baseVolume") or 0
                            last_price = info.get("last") or info.get("close") or 0
                            est_quote_vol = float(base_volume) * float(last_price) if base_volume and last_price else 0
                            resolved_vol = float(quote_volume) if quote_volume and float(quote_volume) > 0 else est_quote_vol
                            if resolved_vol > 0:
                                ranked.append((symbol, resolved_vol))
                            else:
                                fallback_symbols.append(symbol)
                        ranked.sort(key=lambda x: x[1], reverse=True)
                        symbols = [s for s, _ in ranked[: self.max_symbols]]
                        if len(symbols) < self.max_symbols and fallback_symbols:
                            needed = self.max_symbols - len(symbols)
                            symbols.extend(fallback_symbols[:needed])
                        print(
                            f"[SCAN] {exchange_name} | auth failed, using public market data | "
                            f"universe size={len(symbols)} USDT pairs"
                        )
                        return symbols
                except Exception as fallback_exc:
                    print(f"[SCAN] {exchange_name} | public market fallback failed: {fallback_exc}")
            print(f"[SCAN] {exchange_name} | failed to build symbol universe: {exc}")
            self.send_admin_notification(f"{exchange_name} top symbol fetch error: {exc}", loud=True)
            return []

    def fetch_multi_timeframes(self, ex: ccxt.Exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
        timeframes = {"4h": 220, "1h": 220, "15m": 220, "5m": 220}
        out: Dict[str, pd.DataFrame] = {}
        exchange_id = str(getattr(ex, "id", "")).lower()

        def _public_exchange_fallback() -> Optional[ccxt.Exchange]:
            try:
                if "binance" in exchange_id:
                    return ccxt.binance({"enableRateLimit": True})
                if "bybit" in exchange_id:
                    return ccxt.bybit({"enableRateLimit": True})
                if "okx" in exchange_id:
                    return ccxt.okx({"enableRateLimit": True})
            except Exception:
                return None
            return None

        def _retry_ohlcv(tf: str, lim: int, attempts: int = 4) -> Optional[List[list]]:
            for idx in range(attempts):
                try:
                    return ex.fetch_ohlcv(symbol, timeframe=tf, limit=lim)
                except Exception as exc:
                    err_text = str(exc).lower()
                    auth_hint = any(
                        hint in err_text for hint in ["invalid api-key", "api-key", "authentication", "permission", "auth"]
                    )
                    if auth_hint:
                        public_ex = _public_exchange_fallback()
                        if public_ex is not None:
                            try:
                                rows = public_ex.fetch_ohlcv(symbol, timeframe=tf, limit=lim)
                                if rows:
                                    return rows
                            except Exception:
                                pass
                    if idx == attempts - 1:
                        return None
                    time.sleep(min(2.5, 0.25 * (2 ** idx)))
            return None

        def _fetch(tf: str, lim: int) -> Tuple[str, Optional[pd.DataFrame]]:
            rows = _retry_ohlcv(tf, lim)
            if not rows:
                return tf, None
            df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            return tf, df

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(_fetch, tf, lim) for tf, lim in timeframes.items()]
            for f in as_completed(futures):
                tf, df = f.result()
                if df is None or df.empty:
                    return None
                out[tf] = df
        return out

    def get_realtime_price_fast(self, ex: ccxt.Exchange, symbol: str) -> Optional[float]:
        key = f"{getattr(ex, 'id', 'ex')}:{symbol}"
        now = time.time()
        cached = self.fast_ticker_cache.get(key)
        if cached and (now - float(cached.get("ts", 0.0))) < self.fast_ticker_cache_ttl_sec:
            return float(cached.get("price"))
        try:
            ticker = ex.fetch_ticker(symbol)
            px = ticker.get("last") or ticker.get("close")
            if px is None:
                return None
            price = float(px)
            self.fast_ticker_cache[key] = {"ts": now, "price": price}
            return price
        except Exception:
            return None

    @staticmethod
    def compute_position_size(entry: float, sl: float, equity: float, risk_pct: float) -> float:
        risk_usd = max(0.0, float(equity) * (float(risk_pct) / 100.0))
        risk_per_unit = abs(float(entry) - float(sl))
        if risk_per_unit <= 0:
            return 0.0
        return risk_usd / risk_per_unit

    # -------------------------
    # 2) The Intelligence Core
    # -------------------------
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        data = df.copy()
        # pandas-ta VWAP expects a DatetimeIndex
        if "ts" in data.columns:
            data = data.sort_values("ts").set_index("ts", drop=False)
        data["ema20"] = ta.ema(data["close"], length=20)
        data["ema50"] = ta.ema(data["close"], length=50)
        data["ema200"] = ta.ema(data["close"], length=200)
        adx = ta.adx(data["high"], data["low"], data["close"], length=14)
        data["adx"] = adx["ADX_14"] if adx is not None and "ADX_14" in adx else None
        data["rsi"] = ta.rsi(data["close"], length=14)
        stochrsi = ta.stochrsi(data["close"], length=14, rsi_length=14, k=3, d=3)
        data["stoch_k"] = stochrsi["STOCHRSIk_14_14_3_3"] if stochrsi is not None else None
        data["stoch_d"] = stochrsi["STOCHRSId_14_14_3_3"] if stochrsi is not None else None
        data["obv"] = ta.obv(data["close"], data["volume"])
        data["vwap"] = ta.vwap(
            high=data["high"],
            low=data["low"],
            close=data["close"],
            volume=data["volume"],
        )
        data["vwma20"] = ta.vwma(data["close"], data["volume"], length=20)
        data["atr"] = ta.atr(data["high"], data["low"], data["close"], length=14)
        macd = ta.macd(data["close"], fast=12, slow=26, signal=9)
        data["macd"] = macd["MACD_12_26_9"] if macd is not None else None
        data["macd_signal"] = macd["MACDs_12_26_9"] if macd is not None else None
        if hasattr(ta, "volume_sma"):
            data["vol_sma20"] = ta.volume_sma(data["volume"], length=20)
        else:
            data["vol_sma20"] = ta.sma(data["volume"], length=20)
        data["rel_volume"] = data["volume"] / data["vol_sma20"]
        return data

    @staticmethod
    def pivot_levels(df_1h: pd.DataFrame) -> Dict[str, float]:
        prev = df_1h.iloc[-2]
        high, low, close = float(prev["high"]), float(prev["low"]), float(prev["close"])
        pivot = (high + low + close) / 3
        r1 = 2 * pivot - low
        s1 = 2 * pivot - high
        r2 = pivot + (high - low)
        s2 = pivot - (high - low)
        return {"pivot": pivot, "r1": r1, "s1": s1, "r2": r2, "s2": s2}

    @staticmethod
    def find_last_order_block_before_bos(
        df_15m: pd.DataFrame, side: str
    ) -> Optional[Tuple[float, float]]:
        """
        Find last OB on 15m: opposite candle before a BOS impulse.
        LONG  -> last bearish candle before upside BOS.
        SHORT -> last bullish candle before downside BOS.
        Returns (zone_low, zone_high).
        """
        if len(df_15m) < 20:
            return None

        for i in range(len(df_15m) - 4, 3, -1):
            o = float(df_15m.iloc[i]["open"])
            h = float(df_15m.iloc[i]["high"])
            low_v = float(df_15m.iloc[i]["low"])
            c = float(df_15m.iloc[i]["close"])
            next_close = float(df_15m.iloc[i + 1]["close"])
            next2_high = float(df_15m.iloc[i + 2]["high"])
            next2_low = float(df_15m.iloc[i + 2]["low"])
            prev_high = max(float(df_15m.iloc[i - 1]["high"]), float(df_15m.iloc[i - 2]["high"]))
            prev_low = min(float(df_15m.iloc[i - 1]["low"]), float(df_15m.iloc[i - 2]["low"]))

            if side == "LONG":
                bearish_base = c < o
                upside_bos = next_close > prev_high and next2_high > h
                if bearish_base and upside_bos:
                    return (low_v, h)
            else:
                bullish_base = c > o
                downside_bos = next_close < prev_low and next2_low < low_v
                if bullish_base and downside_bos:
                    return (low_v, h)

        return None

    @staticmethod
    def find_latest_fvg(df_15m: pd.DataFrame, side: str) -> Optional[Tuple[float, float, float]]:
        """
        Returns (lower, upper, equilibrium) for latest bullish/bearish FVG.
        Bullish FVG (LONG): low[i] > high[i-2]
        Bearish FVG (SHORT): high[i] < low[i-2]
        """
        if len(df_15m) < 6:
            return None

        for i in range(len(df_15m) - 1, 1, -1):
            h_prev2 = float(df_15m.iloc[i - 2]["high"])
            l_prev2 = float(df_15m.iloc[i - 2]["low"])
            l_now = float(df_15m.iloc[i]["low"])
            h_now = float(df_15m.iloc[i]["high"])

            if side == "LONG" and l_now > h_prev2:
                lower, upper = h_prev2, l_now
                eq = lower + (upper - lower) * 0.5
                return lower, upper, eq

            if side == "SHORT" and h_now < l_prev2:
                lower, upper = h_now, l_prev2
                eq = lower + (upper - lower) * 0.5
                return lower, upper, eq

        return None

    @staticmethod
    def _is_power_session(ts_utc: pd.Timestamp) -> bool:
        dt = ts_utc.to_pydatetime()
        hour = dt.hour
        # London + New York active windows (UTC approximation).
        return (7 <= hour <= 11) or (13 <= hour <= 17)

    @staticmethod
    def _hh_hl_or_lh_ll(df: pd.DataFrame, side: str) -> bool:
        if len(df) < 6:
            return False
        h1 = float(df.iloc[-1]["high"])
        h2 = float(df.iloc[-2]["high"])
        h3 = float(df.iloc[-3]["high"])
        l1 = float(df.iloc[-1]["low"])
        l2 = float(df.iloc[-2]["low"])
        l3 = float(df.iloc[-3]["low"])
        if side == "LONG":
            return (h1 > h2 > h3) and (l1 > l2 > l3)
        return (h1 < h2 < h3) and (l1 < l2 < l3)

    async def _btc_eth_correlation_gate(self, side: str) -> dict:
        cache_key = f"btc_eth:{side}"
        now = time.time()
        cached = self.ext_cache.get(cache_key)
        if cached and (now - cached["ts"]) < self.ext_cache_ttl_sec:
            return cached["data"]
        ex = self.exchanges.get("Binance") or self.exchanges.get("Bybit")
        if not ex:
            data = {"ok": False, "reason": "No exchange"}
            self.ext_cache[cache_key] = {"ts": now, "data": data}
            return data
        try:
            btc_rows, eth_rows = await asyncio.gather(
                asyncio.to_thread(ex.fetch_ohlcv, "BTC/USDT", "15m", 90),
                asyncio.to_thread(ex.fetch_ohlcv, "ETH/USDT", "15m", 90),
            )
            if not btc_rows or not eth_rows:
                data = {"ok": False, "reason": "Missing BTC/ETH"}
            else:
                btc = pd.DataFrame(btc_rows, columns=["ts", "open", "high", "low", "close", "volume"])
                eth = pd.DataFrame(eth_rows, columns=["ts", "open", "high", "low", "close", "volume"])
                btc = self.add_indicators(btc)
                eth = self.add_indicators(eth)
                btc_last = btc.iloc[-1]
                btc_resistance = float(btc_last["close"]) >= float(btc["high"].iloc[-30:].max()) * 0.998
                btc_bear_div = (
                    float(btc.iloc[-1]["close"]) > float(btc.iloc[-5]["close"])
                    and float(btc.iloc[-1]["rsi"]) < float(btc.iloc[-5]["rsi"])
                )
                eth_bear_div = (
                    float(eth.iloc[-1]["close"]) > float(eth.iloc[-5]["close"])
                    and float(eth.iloc[-1]["rsi"]) < float(eth.iloc[-5]["rsi"])
                )
                if side == "LONG":
                    ok = not (btc_resistance or btc_bear_div or eth_bear_div)
                else:
                    ok = True
                reason = "Green Light" if ok else "BTC/ETH Correlation Caution"
                data = {"ok": ok, "reason": reason}
        except Exception:
            data = {"ok": False, "reason": "Correlation unavailable"}
        self.ext_cache[cache_key] = {"ts": now, "data": data}
        return data

    async def _layer5_mtf_sync(self, ex: ccxt.Exchange, symbol: str, side: str) -> dict:
        matrix = await self._async_fetch_timeframe_matrix(ex, symbol)
        needed = ["1m", "5m", "15m"]
        if not all(tf in matrix for tf in needed):
            return {"ok": False, "text": "Incomplete 1m/5m/15m"}
        rsi_ok = True
        macd_ok = True
        for tf in needed:
            d = matrix[tf]
            last = d.iloc[-1]
            if side == "LONG":
                rsi_ok = rsi_ok and float(last["rsi"]) >= 50 and float(last["rsi"]) >= float(d.iloc[-2]["rsi"])
                macd_ok = macd_ok and float(last["macd"]) >= float(last["macd_signal"])
            else:
                rsi_ok = rsi_ok and float(last["rsi"]) <= 50 and float(last["rsi"]) <= float(d.iloc[-2]["rsi"])
                macd_ok = macd_ok and float(last["macd"]) <= float(last["macd_signal"])
        ok = bool(rsi_ok and macd_ok)
        return {"ok": ok, "text": "RSI+MACD Synced" if ok else "RSI/MACD Desync"}

    async def _layer6_order_book_multi(
        self, symbol: str, side: str, entry: float, sl: float, current_price: float
    ) -> dict:
        exchanges = []
        for name in ["Binance", "Bybit"]:
            ex = self.exchanges.get(name)
            if ex:
                exchanges.append((name, ex))
        if not exchanges:
            return {"ok": False, "text": "No L2 source"}
        ratios: List[float] = []
        confirmed: List[str] = []
        support_wall_ok = False
        for name, ex in exchanges:
            try:
                ob = await asyncio.to_thread(ex.fetch_order_book, symbol, 80)
                bids = ob.get("bids", []) or []
                asks = ob.get("asks", []) or []
                lower = current_price * (1 - 0.015)
                upper = current_price * (1 + 0.015)
                bid_usd = sum(float(p) * float(v) for p, v in bids if lower <= float(p) <= upper)
                ask_usd = sum(float(p) * float(v) for p, v in asks if lower <= float(p) <= upper)
                ratio = (bid_usd / max(ask_usd, 1e-9)) if side == "LONG" else (ask_usd / max(bid_usd, 1e-9))
                # Layer 6 strict wall rule: >= $500k within 1% from current price
                wall_band = current_price * 0.01
                wall_thresh = 500_000.0
                relevant_levels = bids if side == "LONG" else asks
                has_wall = any(
                    abs(float(p) - current_price) <= wall_band and (float(p) * float(v)) >= wall_thresh
                    for p, v in relevant_levels
                )
                if ratio >= 2.0 and has_wall:
                    confirmed.append(name)
                if has_wall:
                    support_wall_ok = True
                ratios.append(ratio)
            except Exception as exc:
                print(f"[PROBE] order-book wall probe failed: {exc}")
                continue
        ok = len(confirmed) >= 1 and (max(ratios) >= 2.0 if ratios else False)
        text = f"2:1 + wall ({'/'.join(confirmed)})" if ok else "Imbalance/Wall not confirmed"
        return {
            "ok": ok,
            "text": text,
            "support_wall_ok": support_wall_ok,
            "max_ratio": max(ratios) if ratios else 0.0,
        }

    async def _layer7_whale_flow(self, symbol: str, side: str, layer6_meta: dict) -> dict:
        cache_key = f"whale_internal:{symbol}:{side}"
        now = time.time()
        cached = self.ext_cache.get(cache_key)
        if cached and (now - cached["ts"]) < self.ext_cache_ttl_sec:
            return cached["data"]

        exchanges = []
        for name in ["Binance", "Bybit"]:
            ex = self.exchanges.get(name)
            if ex:
                exchanges.append((name, ex))
        if not exchanges:
            data = {"ok": False, "text": "No exchange for layer7"}
            self.ext_cache[cache_key] = {"ts": now, "data": data}
            return data

        large_trade_threshold = 150_000.0
        large_trade_count = 0
        bulls_usd = 0.0
        bears_usd = 0.0
        cut_ms = int((time.time() - 5 * 60) * 1000)
        for _, ex in exchanges:
            try:
                trades = await asyncio.to_thread(ex.fetch_trades, symbol, None, 200)
                for t in trades or []:
                    ts = int(t.get("timestamp") or 0)
                    if ts < cut_ms:
                        continue
                    price = float(t.get("price") or 0.0)
                    amount = float(t.get("amount") or 0.0)
                    usd_notional = price * amount
                    if usd_notional >= large_trade_threshold:
                        large_trade_count += 1
                    side_trade = str(t.get("side") or "").lower()
                    # taker buy -> buyer aggression
                    if side_trade == "buy":
                        bulls_usd += usd_notional
                    elif side_trade == "sell":
                        bears_usd += usd_notional
            except Exception as exc:
                print(f"[PROBE] whale-flow taker probe failed: {exc}")
                continue

        large_trades_ok = large_trade_count >= 3
        # Bulls must be at least 30% above bears for long; opposite for short
        if side == "LONG":
            volume_delta_ok = bulls_usd >= (bears_usd * 1.3 if bears_usd > 0 else 1.0)
        else:
            volume_delta_ok = bears_usd >= (bulls_usd * 1.3 if bulls_usd > 0 else 1.0)
        wall_sync_ok = bool(layer6_meta.get("support_wall_ok", False))
        ok = bool(large_trades_ok and volume_delta_ok and wall_sync_ok)
        text = (
            f"LargeTrades={large_trade_count} | WallSync={'OK' if wall_sync_ok else 'FAIL'} | "
            f"Delta={'OK' if volume_delta_ok else 'FAIL'}"
        )
        data = {"ok": ok, "text": text}
        self.ext_cache[cache_key] = {"ts": now, "data": data}
        return data

    def run_layer7_live_test(self, top_n: int = 5, exchange_name: str = "Binance") -> None:
        ex = self.exchanges.get(exchange_name)
        if not ex:
            print(f"[DEV-L7] exchange unavailable: {exchange_name}")
            return
        symbols = self.top_usdt_symbols(exchange_name, ex)[:top_n]
        print(f"[DEV-L7] running on {exchange_name} top {len(symbols)} symbols")
        for symbol in symbols:
            try:
                rows = ex.fetch_ohlcv(symbol, timeframe="5m", limit=80)
                if not rows:
                    print(f"[DEV-L7] {symbol} FAIL | no ohlcv")
                    continue
                d5 = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
                d5["ts"] = pd.to_datetime(d5["ts"], unit="ms", utc=True)
                current_price = float(d5.iloc[-1]["close"])
                atr = float(self.add_indicators(d5).iloc[-1]["atr"] or 0.0)
                if atr <= 0:
                    print(f"[DEV-L7] {symbol} FAIL | ATR unavailable")
                    continue
                entry = current_price
                side = "LONG"
                sl = entry - (1.5 * atr)
                layer6 = self._run_async_task(self._layer6_order_book_multi(symbol, side, entry, sl, current_price))
                layer7 = self._run_async_task(self._layer7_whale_flow(symbol, side, layer6))
                status = "PASS" if layer7.get("ok") else "FAIL"
                print(
                    f"[DEV-L7] {symbol} {status} | L6={layer6.get('text')} | L7={layer7.get('text')}"
                )
            except Exception as exc:
                print(f"[DEV-L7] {symbol} FAIL | error={exc}")

    async def _layer8_social_session(self, symbol: str, ts_utc: pd.Timestamp) -> dict:
        session_ok = self._is_power_session(ts_utc)
        if not self.lunarcrush_api_key:
            return {
                "ok": False,
                "text": "LunarCrush key missing",
                "galaxy_score": 0.0,
                "alt_rank": 0,
            }
        # Safe mode latch: after first external failure, do not retry in this runtime.
        if self.ext_cache.get("lunar_api_timeout_latched", {}).get("down"):
            return {
                "ok": False,
                "text": "API_TIMEOUT",
                "galaxy_score": 0.0,
                "alt_rank": 0,
                "api_timeout": True,
            }
        base = symbol.split("/")[0].upper()
        cache_key = f"lunar:{base}"
        now = time.time()
        cached = self.ext_cache.get(cache_key)
        if cached and (now - cached["ts"]) < self.ext_cache_ttl_sec:
            lc = cached["data"]
            galaxy_score = float(lc.get("galaxy_score", 0.0))
            alt_rank = int(lc.get("alt_rank", 0))
            social_ok = galaxy_score > 65.0
            ok = bool(session_ok and social_ok)
            session_text = "London/NY active" if session_ok else "Outside power session"
            return {
                "ok": ok,
                "text": f"Galaxy={galaxy_score:.1f} | AltRank={alt_rank} | {session_text}",
                "galaxy_score": galaxy_score,
                "alt_rank": alt_rank,
                "api_timeout": False,
            }
        galaxy_score = 0.0
        alt_rank = 0
        try:
            # Direct request (free-tier-friendly endpoint provided by user)
            resp = await asyncio.to_thread(
                requests.get,
                f"https://lucid-api.lunarcrash.com/v1/coins/{base}",
                headers={"Authorization": f"Bearer {self.lunarcrush_api_key}"},
                timeout=8,
            )
            if resp.ok:
                body = resp.json()
                # Handle both object and list payload styles safely.
                if isinstance(body, dict):
                    payload = body.get("data", body)
                    if isinstance(payload, list):
                        payload = payload[0] if payload else {}
                    galaxy_score = float(payload.get("galaxy_score") or 0.0)
                    alt_rank = int(payload.get("alt_rank") or 0)
            else:
                self.ext_cache["lunar_api_timeout_latched"] = {"down": True, "ts": now}
                return {
                    "ok": False,
                    "text": "API_TIMEOUT",
                    "galaxy_score": 0.0,
                    "alt_rank": 0,
                    "api_timeout": True,
                }
        except Exception:
            self.ext_cache["lunar_api_timeout_latched"] = {"down": True, "ts": now}
            return {
                "ok": False,
                "text": "API_TIMEOUT",
                "galaxy_score": 0.0,
                "alt_rank": 0,
                "api_timeout": True,
            }
        self.ext_cache[cache_key] = {
            "ts": now,
            "data": {"galaxy_score": galaxy_score, "alt_rank": alt_rank},
        }
        social_ok = galaxy_score > 65.0
        ok = bool(session_ok and social_ok)
        if session_ok:
            session_text = "London/NY active"
        else:
            session_text = "Outside power session"
        return {
            "ok": ok,
            "text": f"Galaxy={galaxy_score:.1f} | AltRank={alt_rank} | {session_text}",
            "galaxy_score": galaxy_score,
            "alt_rank": alt_rank,
            "api_timeout": False,
        }

    async def evaluate_layer_stack(
        self,
        ex: ccxt.Exchange,
        symbol: str,
        side: str,
        d5: pd.DataFrame,
        d15: pd.DataFrame,
        entry: float,
        sl: float,
        current_price: float,
        volume_spike: bool,
        in_demand_zone: bool,
        in_supply_zone: bool,
        long_fvg_eq_retested: bool,
        short_fvg_eq_retested: bool,
    ) -> dict:
        trend_15 = float(d15.iloc[-1]["close"]) > float(d15.iloc[-1]["ema200"])
        bos_5_up = float(d5.iloc[-1]["close"]) > max(float(d5.iloc[-2]["high"]), float(d5.iloc[-3]["high"]))
        bos_5_down = float(d5.iloc[-1]["close"]) < min(float(d5.iloc[-2]["low"]), float(d5.iloc[-3]["low"]))
        hh_hl_ok = self._hh_hl_or_lh_ll(d5, side)
        layer1_ok = (bos_5_up and trend_15 and hh_hl_ok) if side == "LONG" else (bos_5_down and (not trend_15) and hh_hl_ok)

        layer2_ok = (
            (in_demand_zone and long_fvg_eq_retested) if side == "LONG" else (in_supply_zone and short_fvg_eq_retested)
        )
        layer3_ok = bool(volume_spike)
        layer4 = await self._btc_eth_correlation_gate(side)
        layer5 = await self._layer5_mtf_sync(ex, symbol, side)
        layer4_ok = bool(layer4["ok"])
        layer5_ok = bool(layer5["ok"])

        vip_layers = {
            "L1_MarketStructure": {"ok": layer1_ok, "text": "BOS + HH/HL"},
            "L2_OrderBlockFVG": {"ok": layer2_ok, "text": "OB Retest + FVG"},
            "L3_RelativeVolume": {"ok": layer3_ok, "text": "1.5x Volume Spike"},
            "L4_BtcEthCorrelation": {"ok": layer4_ok, "text": layer4["reason"]},
            "L5_MtfSync": {"ok": layer5_ok, "text": layer5["text"]},
        }
        vip_pass_count = sum(1 for v in vip_layers.values() if v["ok"])
        vip_ok = all(v["ok"] for v in vip_layers.values())
        # Soft gate: allow one layer miss to avoid losing gold opportunities.
        vip_soft_ok = vip_pass_count >= 4 and bool(vip_layers["L1_MarketStructure"]["ok"]) and bool(
            vip_layers["L2_OrderBlockFVG"]["ok"]
        )

        layer6 = await self._layer6_order_book_multi(symbol, side, entry, sl, current_price)
        layer7 = await self._layer7_whale_flow(symbol, side, layer6)
        # Credit optimization: query LunarCrush only if layers 1-7 passed.
        if vip_ok and bool(layer6["ok"]) and bool(layer7["ok"]):
            layer8 = await self._layer8_social_session(symbol, d5.iloc[-1]["ts"])
        else:
            layer8 = {"ok": False, "text": "Skipped (layers 1-7 not passed)"}
        vip_plus_layers = {
            "L6_OrderBookDepth": {"ok": bool(layer6["ok"]), "text": layer6["text"]},
            "L7_WhaleFlow": {"ok": bool(layer7["ok"]), "text": layer7["text"]},
            "L8_SocialSession": {"ok": bool(layer8["ok"]), "text": layer8["text"]},
        }
        full_layers = {**vip_layers, **vip_plus_layers}
        vip_plus_pass_count = sum(1 for v in full_layers.values() if v["ok"])
        # Survival mode: if layer 8 external API times out, allow VIP+ by layers 1-7.
        layer8_timeout = bool(layer8.get("api_timeout", False) or str(layer8.get("text", "")) == "API_TIMEOUT")
        if layer8_timeout:
            vip_plus_ok = vip_ok and bool(vip_plus_layers["L6_OrderBookDepth"]["ok"]) and bool(vip_plus_layers["L7_WhaleFlow"]["ok"])
        else:
            vip_plus_ok = vip_ok and all(v["ok"] for v in vip_plus_layers.values())
        # Soft VIP+ gate: allow one miss overall, but whales/social/orderbook must pass.
        vip_plus_soft_ok = (
            vip_plus_pass_count >= 7
            and bool(vip_plus_layers["L6_OrderBookDepth"]["ok"])
            and bool(vip_plus_layers["L7_WhaleFlow"]["ok"])
            and bool(vip_plus_layers["L8_SocialSession"]["ok"])
        )

        layer_lines_vip = [
            f"{name}: {'PASS' if meta['ok'] else 'FAIL'} ({meta['text']})" for name, meta in vip_layers.items()
        ]
        layer_lines_vip_plus = [
            f"{name}: {'PASS' if meta['ok'] else 'FAIL'} ({meta['text']})"
            for name, meta in {**vip_layers, **vip_plus_layers}.items()
        ]
        return {
            "vip_ok": vip_ok,
            "vip_plus_ok": vip_plus_ok,
            "vip_soft_ok": vip_soft_ok,
            "vip_plus_soft_ok": vip_plus_soft_ok,
            "vip_pass_count": vip_pass_count,
            "vip_plus_pass_count": vip_plus_pass_count,
            "vip_layers": vip_layers,
            "vip_plus_layers": vip_plus_layers,
            "vip_layer_lines": layer_lines_vip,
            "vip_plus_layer_lines": layer_lines_vip_plus,
            "social_galaxy_score": float(layer8.get("galaxy_score", 0.0)),
            "social_alt_rank": int(layer8.get("alt_rank", 0)),
        }

    @staticmethod
    def analyze_order_book_depth(
        order_book: dict, current_price: float, entry_price: float, stop_loss: float, side: str
    ) -> dict:
        """
        Analyze level-2 order book:
        - imbalance inside +/-1.5% range
        - wall detection (>5x average size) near SL
        - long validation: block if heavy sell wall above entry
        """
        bids = order_book.get("bids", []) or []
        asks = order_book.get("asks", []) or []
        lower = current_price * (1 - 0.015)
        upper = current_price * (1 + 0.015)

        filtered_bids = [(float(p), float(v)) for p, v in bids if lower <= float(p) <= upper]
        filtered_asks = [(float(p), float(v)) for p, v in asks if lower <= float(p) <= upper]

        bid_vol = sum(v for _, v in filtered_bids)
        ask_vol = sum(v for _, v in filtered_asks)

        avg_bid_size = (sum(v for _, v in filtered_bids) / len(filtered_bids)) if filtered_bids else 0.0
        avg_ask_size = (sum(v for _, v in filtered_asks) / len(filtered_asks)) if filtered_asks else 0.0
        bid_wall_threshold = avg_bid_size * 5 if avg_bid_size > 0 else float("inf")
        ask_wall_threshold = avg_ask_size * 5 if avg_ask_size > 0 else float("inf")

        near_sl_band = max(abs(entry_price - stop_loss) * 0.25, current_price * 0.0015)
        near_sl_bid_walls = [
            (p, v) for p, v in filtered_bids if abs(p - stop_loss) <= near_sl_band and v >= bid_wall_threshold
        ]
        near_sl_ask_walls = [
            (p, v) for p, v in filtered_asks if abs(p - stop_loss) <= near_sl_band and v >= ask_wall_threshold
        ]

        # Blocking wall near/above entry for LONG; near/below entry for SHORT.
        entry_block_band = current_price * 0.0035
        sell_wall_above_entry = any(
            (p >= entry_price and p <= entry_price + entry_block_band and v >= ask_wall_threshold)
            for p, v in filtered_asks
        )
        buy_wall_below_entry = any(
            (p <= entry_price and p >= entry_price - entry_block_band and v >= bid_wall_threshold)
            for p, v in filtered_bids
        )

        # Imbalance gate
        long_imbalance_ok = bid_vol >= ask_vol * 2 if ask_vol > 0 else bid_vol > 0
        short_imbalance_ok = ask_vol >= bid_vol * 2 if bid_vol > 0 else ask_vol > 0
        imbalance_ok = long_imbalance_ok if side == "LONG" else short_imbalance_ok

        # Additional block by adverse wall
        blocked_by_wall = sell_wall_above_entry if side == "LONG" else buy_wall_below_entry

        total = bid_vol + ask_vol
        bulls_pct = (bid_vol / total * 100) if total > 0 else 0.0
        bears_pct = (ask_vol / total * 100) if total > 0 else 0.0
        leader = "Bulls" if bid_vol >= ask_vol else "Bears"
        wall_note = (
            "Support Wall Detected"
            if (side == "LONG" and near_sl_bid_walls) or (side == "SHORT" and near_sl_ask_walls)
            else "No Defensive Wall"
        )
        depth_summary = f"{leader} lead {bulls_pct:.0f}/{bears_pct:.0f}% | {wall_note}"

        return {
            "imbalance_ok": imbalance_ok,
            "blocked_by_wall": blocked_by_wall,
            "depth_summary": depth_summary,
        }

    def _run_async_task(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self._async_loop)
        try:
            return future.result()
        except Exception as exc:
            self._notify_if_infra_error(exc, "async task")
            raise

    async def _async_fetch_ohlcv(self, ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
        try:
            rows = await asyncio.to_thread(ex.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit)
            if not rows:
                return None
            df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            return df
        except Exception:
            return None

    async def _async_fetch_timeframe_matrix(self, ex: ccxt.Exchange, symbol: str) -> Dict[str, pd.DataFrame]:
        matrix = {"1m": 240, "5m": 240, "15m": 240, "1h": 240, "4h": 240, "1d": 240}
        tasks = {
            tf: asyncio.create_task(self._async_fetch_ohlcv(ex, symbol, tf, lim)) for tf, lim in matrix.items()
        }
        out: Dict[str, pd.DataFrame] = {}
        for tf, task in tasks.items():
            df = await task
            if df is not None and not df.empty:
                out[tf] = self.add_indicators(df)
        return out

    def _pick_derivatives_symbol(self, raw_symbol: str) -> List[str]:
        base = raw_symbol.split("/")[0]
        return [
            raw_symbol,
            f"{base}/USDT:USDT",
            f"{base}/USDT:USDC",
            f"{base}/USD:USD",
        ]

    async def _exchange_spike_probe(self, ex: ccxt.Exchange, symbol: str) -> dict:
        try:
            ticker = await asyncio.to_thread(ex.fetch_ticker, symbol)
            last_price = float(ticker.get("last") or ticker.get("close") or 0.0)
            df = await self._async_fetch_ohlcv(ex, symbol, "5m", 40)
            if df is None or len(df) < 25:
                return {"ok": False}
            data = self.add_indicators(df)
            last = data.iloc[-1]
            avg_vol = float(last["vol_sma20"]) if float(last["vol_sma20"]) > 0 else 0.0
            cur_vol = float(last["volume"])
            spike = avg_vol > 0 and cur_vol > (avg_vol * 1.5)
            return {"ok": True, "price": last_price, "spike": spike}
        except Exception:
            return {"ok": False}

    async def _cross_exchange_sync(self, symbol: str) -> dict:
        targets = []
        for name in ["Binance", "Bybit", "OKX"]:
            ex = self.exchanges.get(name)
            if not ex:
                continue
            targets.append((name, ex))
        tasks = [asyncio.create_task(self._exchange_spike_probe(ex, symbol)) for _, ex in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid: List[Tuple[str, float, bool]] = []
        for idx, res in enumerate(results):
            if isinstance(res, Exception) or not isinstance(res, dict) or not res.get("ok"):
                continue
            valid.append((targets[idx][0], float(res["price"]), bool(res["spike"])))

        spike_rows = [(n, p) for n, p, s in valid if s]
        if len(spike_rows) < 2:
            return {"confirmed": False, "text": "Not Confirmed"}

        prices = [p for _, p in spike_rows if p > 0]
        if len(prices) < 2:
            return {"confirmed": False, "text": "Not Confirmed"}
        spread_pct = (max(prices) - min(prices)) / max(min(prices), 1e-9) * 100
        if spread_pct > 0.2:
            return {"confirmed": False, "text": "Not Confirmed"}

        names = "/".join(n for n, _ in spike_rows[:3])
        return {"confirmed": True, "text": f"Confirmed ({names})"}

    async def _derivatives_layer(self, symbol: str) -> dict:
        derivative_sources = [self.exchanges.get("Bybit"), self.exchanges.get("OKX")]
        for ex in derivative_sources:
            if not ex:
                continue
            for s in self._pick_derivatives_symbol(symbol):
                try:
                    oi_hist = await asyncio.to_thread(ex.fetch_open_interest_history, s, "15m", None, 2)
                    if not oi_hist or len(oi_hist) < 2:
                        continue
                    oi_prev = float(oi_hist[-2].get("openInterestAmount") or oi_hist[-2].get("openInterestValue") or oi_hist[-2].get("openInterest") or 0.0)
                    oi_last = float(oi_hist[-1].get("openInterestAmount") or oi_hist[-1].get("openInterestValue") or oi_hist[-1].get("openInterest") or 0.0)
                    oi_bullish = oi_last > oi_prev
                    fr = await asyncio.to_thread(ex.fetch_funding_rate, s)
                    funding_rate = float(fr.get("fundingRate") or 0.0) * 100.0
                    return {
                        "ok": True,
                        "oi_bullish": oi_bullish,
                        "funding_rate_pct": funding_rate,
                        "text": "Bullish" if oi_bullish else "Flat/Weak",
                    }
                except Exception as exc:
                    print(f"[PROBE] derivatives layer probe failed: {exc}")
                    continue
        return {"ok": False, "oi_bullish": False, "funding_rate_pct": 0.0, "text": "Unavailable"}

    async def _async_synchronicity_validation(self, ex: ccxt.Exchange, symbol: str, side: str) -> dict:
        cache_key = f"sync:{ex.id}:{symbol}:{side}"
        now = time.time()
        cached = self.sync_cache.get(cache_key)
        if cached and (now - cached["ts"]) < self.sync_cache_ttl_sec:
            return cached["data"]

        matrix = await self._async_fetch_timeframe_matrix(ex, symbol)
        needed = ["1m", "5m", "15m", "1h", "4h", "1d"]
        if not all(tf in matrix for tf in needed):
            data = {
                "ok": False,
                "mtf_ok": False,
                "mtf_text": "Incomplete",
                "exchange_ok": False,
                "exchange_text": "Not Confirmed",
                "oi_ok": False,
                "oi_text": "Unavailable",
                "funding_rate_pct": 0.0,
            }
            self.sync_cache[cache_key] = {"ts": now, "data": data}
            return data

        mtf_pass_count = 0
        for tf in ["15m", "1h", "4h", "1d"]:
            last = matrix[tf].iloc[-1]
            if float(last["close"]) > float(last["ema200"]):
                mtf_pass_count += 1
        rsi_1m_up = float(matrix["1m"].iloc[-1]["rsi"]) > float(matrix["1m"].iloc[-2]["rsi"])
        rsi_5m_up = float(matrix["5m"].iloc[-1]["rsi"]) > float(matrix["5m"].iloc[-2]["rsi"])
        if rsi_1m_up:
            mtf_pass_count += 1
        if rsi_5m_up:
            mtf_pass_count += 1
        mtf_ok = mtf_pass_count == 6 if side == "LONG" else True

        exchange_sync = await self._cross_exchange_sync(symbol)
        der = await self._derivatives_layer(symbol)

        oi_ok = der["ok"] and der["oi_bullish"]
        funding_ok = der["ok"] and der["funding_rate_pct"] <= 0.05
        if side != "LONG":
            funding_ok = True

        data = {
            "ok": bool(mtf_ok and exchange_sync["confirmed"] and oi_ok and funding_ok),
            "mtf_ok": mtf_ok,
            "mtf_text": f"{mtf_pass_count}/6 Intervals",
            "exchange_ok": exchange_sync["confirmed"],
            "exchange_text": exchange_sync["text"],
            "oi_ok": oi_ok,
            "oi_text": der["text"],
            "funding_rate_pct": float(der.get("funding_rate_pct") or 0.0),
        }
        self.sync_cache[cache_key] = {"ts": now, "data": data}
        return data

    def detect_setup(self, ex: ccxt.Exchange, symbol: str, dfs: Dict[str, pd.DataFrame]) -> Optional[dict]:
        d4h = self.add_indicators(dfs["4h"])
        d1h = self.add_indicators(dfs["1h"])
        d15 = self.add_indicators(dfs["15m"])
        d5 = self.add_indicators(dfs["5m"])

        if d4h.iloc[-1].isna().any() or d1h.iloc[-1].isna().any() or d15.iloc[-1].isna().any() or d5.iloc[-1].isna().any():
            return None

        last_4h = d4h.iloc[-1]
        last_1h = d1h.iloc[-1]
        last_15 = d15.iloc[-1]
        prev_15 = d15.iloc[-2]
        prev2_15 = d15.iloc[-3]
        last_5 = d5.iloc[-1]
        prev_5 = d5.iloc[-2]
        pivots = self.pivot_levels(d1h)
        price = float(last_5["close"])
        if float(last_1h["adx"]) < self.adx_trend_min:
            return None

        # Layer 1: HTF filter (4H EMA200) + MSS/BOS on 15m
        trend_up = float(last_4h["close"]) > float(last_4h["ema200"])
        trend_down = float(last_4h["close"]) < float(last_4h["ema200"])
        bos_up = float(last_15["close"]) > max(float(prev_15["high"]), float(prev2_15["high"]))
        bos_down = float(last_15["close"]) < min(float(prev_15["low"]), float(prev2_15["low"]))

        # Layer 2: Order Blocks + FVG equilibrium retest on 15m
        demand_zone = self.find_last_order_block_before_bos(d15, "LONG")
        supply_zone = self.find_last_order_block_before_bos(d15, "SHORT")
        in_demand_zone = bool(demand_zone and demand_zone[0] <= price <= demand_zone[1] * 1.002)
        in_supply_zone = bool(supply_zone and supply_zone[0] * 0.998 <= price <= supply_zone[1])

        long_fvg = self.find_latest_fvg(d15, "LONG")
        short_fvg = self.find_latest_fvg(d15, "SHORT")
        long_fvg_eq_retested = bool(long_fvg and long_fvg[0] <= price <= long_fvg[1] and price <= long_fvg[2])
        short_fvg_eq_retested = bool(short_fvg and short_fvg[0] <= price <= short_fvg[1] and price >= short_fvg[2])

        # Volume gate: current volume must exceed adaptive SMA20 multiplier.
        avg_volume = float(last_5["vol_sma20"]) if float(last_5["vol_sma20"]) > 0 else 0.0
        current_volume = float(last_5["volume"])
        volume_spike = avg_volume > 0 and current_volume > (avg_volume * self.volume_spike_threshold)

        # Layer 3 trigger components
        long_osc = (last_15["rsi"] < 30) and (last_15["stoch_k"] > last_15["stoch_d"]) and (last_15["stoch_k"] < 25)
        short_osc = (last_15["rsi"] > 70) and (last_15["stoch_k"] < last_15["stoch_d"]) and (last_15["stoch_k"] > 75)
        rsi_recover_long = (float(prev_15["rsi"]) < 30) and (float(last_15["rsi"]) > float(prev_15["rsi"]))
        rsi_recover_short = (float(prev_15["rsi"]) > 70) and (float(last_15["rsi"]) < float(prev_15["rsi"]))
        rsi_signal_ok = self.rsi_weight >= 0.65
        if not rsi_signal_ok:
            long_osc = False
            short_osc = False
            rsi_recover_long = False
            rsi_recover_short = False
        at_support = price <= pivots["s1"] * 1.005
        at_resistance = price >= pivots["r1"] * 0.995
        macd_cross_up = (prev_5["macd"] <= prev_5["macd_signal"]) and (last_5["macd"] > last_5["macd_signal"])
        macd_cross_down = (prev_5["macd"] >= prev_5["macd_signal"]) and (last_5["macd"] < last_5["macd_signal"])
        vwap_long = (
            float(last_15["close"]) > float(last_15["vwap"])
            and float(last_1h["close"]) > float(last_1h["vwap"])
            and float(last_15.get("vwma20", last_15["close"])) >= float(last_15["ema20"])
        )
        vwap_short = (
            float(last_15["close"]) < float(last_15["vwap"])
            and float(last_1h["close"]) < float(last_1h["vwap"])
            and float(last_15.get("vwma20", last_15["close"])) <= float(last_15["ema20"])
        )

        if self.aggressive_signal_mode:
            # Throughput mode: require trend + break + momentum core, while allowing
            # looser liquidity/location conditions so we don't starve the feed.
            long_ready = (
                trend_up
                and bos_up
                and (macd_cross_up or (long_osc or rsi_recover_long))
                and (vwap_long or in_demand_zone or long_fvg_eq_retested or at_support)
            )
            short_ready = (
                trend_down
                and bos_down
                and (macd_cross_down or (short_osc or rsi_recover_short))
                and (vwap_short or in_supply_zone or short_fvg_eq_retested or at_resistance)
            )
        else:
            long_ready = (
                trend_up
                and bos_up
                and in_demand_zone
                and long_fvg_eq_retested
                and volume_spike
                and (long_osc or rsi_recover_long)
                and macd_cross_up
                and vwap_long
                and at_support
            )
            short_ready = (
                trend_down
                and bos_down
                and in_supply_zone
                and short_fvg_eq_retested
                and volume_spike
                and (short_osc or rsi_recover_short)
                and macd_cross_down
                and vwap_short
                and at_resistance
            )
        if not (long_ready or short_ready):
            if self.aggressive_signal_mode:
                relv = float(last_5.get("rel_volume", 0.0))
                rsi15 = float(last_15.get("rsi", 50.0))
                # Emergency throughput fallback: a simpler momentum structure when
                # full institutional stack has no candidates for prolonged periods.
                long_ready = bool(
                    trend_up
                    and (bos_up or macd_cross_up or float(last_5.get("close", 0.0)) > float(last_5.get("vwap", 0.0)))
                    and (rsi15 >= 42 and rsi15 <= 78)
                    and (relv >= 0.95)
                )
                short_ready = bool(
                    trend_down
                    and (bos_down or macd_cross_down or float(last_5.get("close", 0.0)) < float(last_5.get("vwap", 0.0)))
                    and (rsi15 <= 58 and rsi15 >= 22)
                    and (relv >= 0.95)
                )
            if not (long_ready or short_ready):
                return None

        side = "LONG" if long_ready else "SHORT"
        atr = float(last_5["atr"])
        if atr <= 0:
            return None

        entry = price
        # Layer 4: dynamic ATR stop at 1.5x ATR
        sl = entry - (1.5 * atr) if side == "LONG" else entry + (1.5 * atr)
        risk = abs(entry - sl)
        tp1 = entry + (1.5 * risk) if side == "LONG" else entry - (1.5 * risk)
        tp2 = entry + (2.5 * risk) if side == "LONG" else entry - (2.5 * risk)
        strong_level = pivots["r2"] if side == "LONG" else pivots["s2"]
        tp3_ratio = entry + (4.0 * risk) if side == "LONG" else entry - (4.0 * risk)
        tp3 = max(tp3_ratio, strong_level) if side == "LONG" else min(tp3_ratio, strong_level)

        confluence_checks = [
            bool(trend_up or trend_down),
            bool(last_1h["adx"] > 25),
            bool(long_osc or short_osc),
            bool(volume_spike),
            bool(at_support or at_resistance),
            bool(macd_cross_up or macd_cross_down),
            bool((side == "LONG" and last_5["obv"] > d5.iloc[-5]["obv"]) or (side == "SHORT" and last_5["obv"] < d5.iloc[-5]["obv"])),
            bool((side == "LONG" and price > last_5["vwap"]) or (side == "SHORT" and price < last_5["vwap"])),
            bool(last_5["rel_volume"] > 2.5),
            bool(abs(price - pivots["pivot"]) / max(price, 1e-9) < 0.02),
        ]
        score_1_10 = max(1, min(10, sum(1 for x in confluence_checks if x)))
        if self.rsi_weight < 1.0:
            score_1_10 = max(1, score_1_10 - int(round((1.0 - self.rsi_weight) * 2.0)))
        indicator_5 = [
            bool(macd_cross_up or macd_cross_down),
            bool(vwap_long or vwap_short),
            bool(float(last_1h["adx"]) >= self.adx_trend_min),
            bool(long_osc or short_osc or rsi_recover_long or rsi_recover_short),
            bool((side == "LONG" and price > float(last_15.get("vwma20", last_15["close"]))) or (side == "SHORT" and price < float(last_15.get("vwma20", last_15["close"])))),
        ]
        indicator_alignment_count_5 = sum(1 for x in indicator_5 if x)
        mtf_confluence_ok = bool((side == "LONG" and trend_up) or (side == "SHORT" and trend_down))
        vwap_institutional_ok = bool(vwap_long if side == "LONG" else vwap_short)

        tech_breakdown = []
        if at_support:
            tech_breakdown.append("Price action at Support")
        if at_resistance:
            tech_breakdown.append("Price action at Resistance")
        if long_osc or short_osc:
            tech_breakdown.append("RSI + StochRSI alignment")
        if volume_spike:
            tech_breakdown.append("High Relative Volume Spike")
        if macd_cross_up or macd_cross_down:
            tech_breakdown.append("MACD cross confirmed on 5m")

        potential_gain_pct = abs((tp3 - entry) / entry) * 100
        potential_loss_pct = abs((entry - sl) / entry) * 100
        rr = potential_gain_pct / max(potential_loss_pct, 1e-9)
        min_rr = 1.8 if self.aggressive_signal_mode else 2.5
        if rr < min_rr:
            return None

        liquidity_bits: List[str] = []
        if volume_spike:
            liquidity_bits.append("High Volume Confirmation")
        if (side == "LONG" and in_demand_zone) or (side == "SHORT" and in_supply_zone):
            liquidity_bits.append("Order Block Retest")
        if (side == "LONG" and long_fvg_eq_retested) or (side == "SHORT" and short_fvg_eq_retested):
            liquidity_bits.append("FVG Equilibrium Fill")
        liquidity_status = " + ".join(liquidity_bits) if liquidity_bits else "Liquidity Neutral"
        liquidity_confirmation = (
            " + ".join(liquidity_bits[:2]) if liquidity_bits else "No Liquidity Confirmation"
        )

        # -------------------------
        # Phase 2.4: Global Macro Correlation
        # -------------------------
        macro = self.get_macro_environment(ex, symbol)
        if side == "LONG" and (not self.aggressive_signal_mode):
            # Block longs in dollar bullish regime and risk-off stablecoin flow.
            if macro["dxy_rsi_1h"] > 60:
                return None
            if not macro["usdt_dominance_risk_on"]:
                return None
            if not macro["sector_strength_ok"]:
                return None

        # Relative performance vs BTC (score boost / short filter).
        btc_change = macro["btc_change_pct"]
        asset_change = macro["asset_change_pct"]
        if side == "LONG":
            # Negative behavior in corrections: reject if asset drops notably more than BTC.
            if (not self.aggressive_signal_mode) and btc_change < 0 and asset_change < (btc_change * 1.2):
                return None
            if macro["relative_strength"]:
                score_1_10 = min(10, score_1_10 + 1)
        else:
            # For shorts, avoid names that are too positively resilient while BTC weakens.
            if (not self.aggressive_signal_mode) and btc_change < 0 and asset_change > btc_change * 0.5:
                return None

        # -------------------------
        # VIP / VIP+ Layered Gate (strict institutional filter stack)
        # -------------------------
        try:
            layered = self._run_async_task(
                asyncio.wait_for(
                    self.evaluate_layer_stack(
                        ex=ex,
                        symbol=symbol,
                        side=side,
                        d5=d5,
                        d15=d15,
                        entry=entry,
                        sl=sl,
                        current_price=price,
                        volume_spike=volume_spike,
                        in_demand_zone=in_demand_zone,
                        in_supply_zone=in_supply_zone,
                        long_fvg_eq_retested=long_fvg_eq_retested,
                        short_fvg_eq_retested=short_fvg_eq_retested,
                    ),
                    timeout=max(0.5, self.layer_timeout_ms / 1000.0),
                )
            )
        except Exception as exc:
            if self.aggressive_signal_mode:
                layered = {
                    "vip_ok": True,
                    "vip_soft_ok": True,
                    "vip_plus_ok": False,
                    "vip_plus_soft_ok": False,
                    "vip_layer_lines": [f"Layer stack bypassed in aggressive mode: {exc}"],
                    "vip_plus_layer_lines": [],
                    "social_text": "Bypassed (aggressive mode)",
                    "order_book_text": "Bypassed (aggressive mode)",
                    "whale_text": "Bypassed (aggressive mode)",
                    "order_book_depth": "Bypassed",
                    "social_galaxy_score": 0.0,
                }
            else:
                return None
        if self.vip_strict_mode and (not self.aggressive_signal_mode) and not layered.get("vip_ok", False):
            print(f"[GATE] {symbol} rejected VIP stack | " + " | ".join(layered.get("vip_layer_lines", [])))
            return None

        # Keep prior synchronicity diagnostics as additional data point.
        sync = self._run_async_task(self._async_synchronicity_validation(ex, symbol, side))

        # Retain local depth snapshot for message detail (non-hard gate now; layered engine handles gating).
        try:
            order_book = ex.fetch_order_book(symbol, limit=50)
            ob_meta = self.analyze_order_book_depth(order_book, price, entry, sl, side)
            order_book_depth = ob_meta["depth_summary"]
        except Exception:
            order_book_depth = "Unavailable"
        risk_pct = max(0.1, min(self.max_risk_per_trade_pct, self.risk_per_trade_pct))
        pos_size = self.compute_position_size(entry=entry, sl=sl, equity=self.virtual_balance, risk_pct=risk_pct)

        return {
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "score": score_1_10,
            "breakdown": " + ".join(tech_breakdown[:3]),
            "potential_gain_pct": potential_gain_pct,
            "potential_loss_pct": potential_loss_pct,
            "rr": rr,
            "adx": float(last_1h["adx"]),
            "rsi": float(last_15["rsi"]),
            "rel_volume": float(last_5["rel_volume"]),
            "macd": float(last_5["macd"]),
            "macd_signal": float(last_5["macd_signal"]),
            "ema20": float(last_15["ema20"]),
            "ema50": float(last_15["ema50"]),
            "ema200": float(last_4h["ema200"]),
            "pivot": float(pivots["pivot"]),
            "s1": float(pivots["s1"]),
            "r1": float(pivots["r1"]),
            "structure_bias": "bullish BOS breakout" if side == "LONG" else "bearish BOS breakdown",
            "liquidity_status": liquidity_status,
            "liquidity_confirmation": liquidity_confirmation,
            "order_book_depth": order_book_depth,
            "macro_dxy_status": macro["dxy_status_text"],
            "macro_sector_status": macro["sector_status_text"],
            "macro_capital_flow": macro["capital_flow_text"],
            "sync_mtf_status": sync.get("mtf_text", "N/A"),
            "sync_exchange_status": sync.get("exchange_text", "N/A"),
            "sync_oi_status": sync.get("oi_text", "N/A"),
            "timeframe": "15m",
            "position_size": float(pos_size),
            "risk_pct": float(risk_pct),
            "indicator_alignment_count_5": int(indicator_alignment_count_5),
            "volume_confirmed": bool(volume_spike),
            "mtf_confluence_ok": bool(mtf_confluence_ok),
            "vwap_institutional_ok": bool(vwap_institutional_ok),
            "vip_ok": layered.get("vip_ok", False),
            "vip_plus_ok": layered.get("vip_plus_ok", False),
            "vip_layer_lines": layered.get("vip_layer_lines", []),
            "vip_plus_layer_lines": layered.get("vip_plus_layer_lines", []),
        }

    def _calc_rsi_from_series(self, values: List[float], period: int = 14) -> float:
        if len(values) < period + 1:
            return 50.0
        gains = []
        losses = []
        for i in range(-period, 0):
            delta = values[i] - values[i - 1]
            gains.append(max(delta, 0.0))
            losses.append(max(-delta, 0.0))
        avg_gain = statistics.fmean(gains) if gains else 0.0
        avg_loss = statistics.fmean(losses) if losses else 0.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _fetch_yahoo_closes_1h(self, symbol: str) -> List[float]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {"range": "5d", "interval": "60m"}
        r = self.http_get(url, params=params, timeout=20)
        if not r:
            return []
        result = r.get("chart", {}).get("result", [])
        if not result:
            return []
        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        return [float(x) for x in closes if x is not None]

    def _calc_beta(self, asset_returns: List[float], btc_returns: List[float]) -> float:
        n = min(len(asset_returns), len(btc_returns))
        if n < 10:
            return 0.0
        a = asset_returns[-n:]
        b = btc_returns[-n:]
        mean_a = statistics.fmean(a)
        mean_b = statistics.fmean(b)
        cov = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n)) / n
        var_b = sum((x - mean_b) ** 2 for x in b) / n
        if var_b <= 1e-12:
            return 0.0
        return cov / var_b

    def _returns(self, closes: List[float]) -> List[float]:
        out: List[float] = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            cur = closes[i]
            if prev <= 0:
                continue
            out.append((cur - prev) / prev)
        return out

    def _sector_for_symbol(self, symbol: str) -> str:
        sector_map = {
            "L1": {"BTC/USDT", "ETH/USDT", "SOL/USDT", "ADA/USDT", "AVAX/USDT"},
            "DEFI": {"UNI/USDT", "AAVE/USDT", "LINK/USDT", "MKR/USDT", "COMP/USDT"},
            "MEME": {"DOGE/USDT", "SHIB/USDT", "PEPE/USDT", "FLOKI/USDT", "BONK/USDT"},
            "AI": {"FET/USDT", "AGIX/USDT", "RNDR/USDT", "TAO/USDT", "WLD/USDT"},
        }
        for name, coins in sector_map.items():
            if symbol in coins:
                return name
        return "GENERAL"

    def _sector_strength(self, ex: ccxt.Exchange, symbol: str) -> Tuple[bool, float]:
        sector = self._sector_for_symbol(symbol)
        sector_baskets = {
            "L1": ["BTC/USDT", "ETH/USDT", "SOL/USDT", "ADA/USDT", "AVAX/USDT"],
            "DEFI": ["UNI/USDT", "AAVE/USDT", "LINK/USDT", "MKR/USDT", "COMP/USDT"],
            "MEME": ["DOGE/USDT", "SHIB/USDT", "PEPE/USDT", "FLOKI/USDT", "BONK/USDT"],
            "AI": ["FET/USDT", "AGIX/USDT", "RNDR/USDT", "TAO/USDT", "WLD/USDT"],
            "GENERAL": ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"],
        }
        basket = sector_baskets.get(sector, sector_baskets["GENERAL"])
        up_count = 0
        checked = 0
        for s in basket:
            try:
                rows = ex.fetch_ohlcv(s, timeframe="1h", limit=220)
                if not rows or len(rows) < 200:
                    continue
                closes = [float(r[4]) for r in rows]
                ema200 = ta.ema(pd.Series(closes), length=200).iloc[-1]
                if closes[-1] > float(ema200):
                    up_count += 1
                checked += 1
            except Exception as exc:
                print(f"[PROBE] sector-strength EMA probe failed: {exc}")
                continue
        if checked == 0:
            return False, 0.0
        ratio = up_count / checked
        return ratio >= 0.6, ratio

    def http_get(self, url: str, params: Optional[dict] = None, timeout: int = 20) -> Optional[dict]:
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if not resp.ok:
                return None
            return resp.json()
        except Exception:
            return None

    def _fetch_cmc_snapshot(self, symbol: str) -> dict:
        """
        Advisory-only CMC metadata. Never used as a hard gate.
        """
        base = symbol.split("/")[0].upper()
        cache_key = f"cmc:{base}"
        now = time.time()
        cached = self.ext_cache.get(cache_key)
        if cached and (now - cached["ts"]) < 300:
            return cached["data"]
        if not self.cmc_api_key:
            return {"ok": False, "text": "CMC unavailable"}
        try:
            resp = requests.get(
                "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
                params={"symbol": base},
                headers={"X-CMC_PRO_API_KEY": self.cmc_api_key, "Accept": "application/json"},
                timeout=6,
            )
            if not resp.ok:
                data = {"ok": False, "text": f"CMC HTTP {resp.status_code}"}
                self.ext_cache[cache_key] = {"ts": now, "data": data}
                return data
            body = resp.json()
            coin = body.get("data", {}).get(base, {})
            rank = int(coin.get("cmc_rank") or 0)
            q = coin.get("quote", {}).get("USD", {})
            vol24 = float(q.get("volume_24h") or 0.0)
            change24 = float(q.get("percent_change_24h") or 0.0)
            data = {
                "ok": True,
                "rank": rank,
                "vol24": vol24,
                "change24": change24,
                "text": f"Rank #{rank} | 24h Vol ${vol24:,.0f} | 24h {change24:+.2f}%",
            }
            self.ext_cache[cache_key] = {"ts": now, "data": data}
            return data
        except Exception:
            return {"ok": False, "text": "CMC timeout"}

    def get_macro_environment(self, ex: ccxt.Exchange, symbol: str) -> dict:
        cache_key = f"macro:{ex.id}:{symbol}"
        now = time.time()
        cached = self.macro_cache.get(cache_key)
        if cached and (now - cached["ts"]) < self.macro_cache_ttl_sec:
            return cached["data"]

        dxy_closes = self._fetch_yahoo_closes_1h("DX-Y.NYB")
        dxy_rsi = self._calc_rsi_from_series(dxy_closes, 14) if dxy_closes else 50.0
        dxy_status_text = "Inverse Confirmed" if dxy_rsi <= 60 else "Dollar Bullish Caution"

        # US10Y pulled for regime context (not hard-gated yet).
        _us10y_closes = self._fetch_yahoo_closes_1h("^TNX")

        # Beta + relative strength vs BTC.
        try:
            asset_rows = ex.fetch_ohlcv(symbol, timeframe="15m", limit=120)
            btc_rows = ex.fetch_ohlcv("BTC/USDT", timeframe="15m", limit=120)
        except Exception as exc:
            # Auth-restricted clients can fail on market data endpoints; retry with
            # public exchange client so macro scoring doesn't collapse to zero setups.
            err_text = str(exc).lower()
            auth_hint = any(
                hint in err_text for hint in ["invalid api-key", "api-key", "authentication", "permission", "auth"]
            )
            if auth_hint:
                public_factory = {
                    "binance": ccxt.binance,
                    "bybit": ccxt.bybit,
                    "okx": ccxt.okx,
                }.get(str(ex.id).lower())
                if public_factory is not None:
                    try:
                        public_ex = public_factory({"enableRateLimit": True})
                        asset_rows = public_ex.fetch_ohlcv(symbol, timeframe="15m", limit=120)
                        btc_rows = public_ex.fetch_ohlcv("BTC/USDT", timeframe="15m", limit=120)
                    except Exception:
                        asset_rows = []
                        btc_rows = []
                else:
                    asset_rows = []
                    btc_rows = []
            else:
                asset_rows = []
                btc_rows = []
        asset_closes = [float(r[4]) for r in asset_rows] if asset_rows else []
        btc_closes = [float(r[4]) for r in btc_rows] if btc_rows else []
        asset_returns = self._returns(asset_closes)
        btc_returns = self._returns(btc_closes)
        beta_vs_btc = self._calc_beta(asset_returns, btc_returns)
        asset_change_pct = ((asset_closes[-1] - asset_closes[-12]) / asset_closes[-12] * 100) if len(asset_closes) >= 12 else 0.0
        btc_change_pct = ((btc_closes[-1] - btc_closes[-12]) / btc_closes[-12] * 100) if len(btc_closes) >= 12 else 0.0
        relative_strength = abs(btc_change_pct) < 0.35 and asset_change_pct > 0.35

        # Stablecoin dominance proxy: (USDT+USDC market cap) / total market cap.
        global_data = self.http_get("https://api.coingecko.com/api/v3/global")
        markets = self.http_get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "ids": "tether,usd-coin,dai"},
        )
        total_mcap = (
            float(global_data["data"]["total_market_cap"]["usd"])
            if global_data and "data" in global_data
            else 0.0
        )
        stable_mcap = 0.0
        if isinstance(markets, list):
            for m in markets:
                stable_mcap += float(m.get("market_cap") or 0.0)
        usdt_dom = (stable_mcap / total_mcap * 100) if total_mcap > 0 else 0.0

        prev_dom = self.macro_cache.get("usdt_dom_prev", {}).get("value")
        usdt_dominance_risk_on = True
        if prev_dom is not None:
            usdt_dominance_risk_on = usdt_dom <= (prev_dom + 0.03)  # static/down bias
        self.macro_cache["usdt_dom_prev"] = {"value": usdt_dom, "ts": now}
        capital_flow_text = "Risk-On" if usdt_dominance_risk_on else "Stablecoin Defensive"

        # Sector heatmap validation.
        sector_ok, sector_ratio = self._sector_strength(ex, symbol)
        sector_status_text = "Dominant" if sector_ok else "Weak Rotation"

        data = {
            "dxy_rsi_1h": dxy_rsi,
            "dxy_status_text": dxy_status_text,
            "beta_vs_btc": beta_vs_btc,
            "relative_strength": relative_strength,
            "asset_change_pct": asset_change_pct,
            "btc_change_pct": btc_change_pct,
            "usdt_dominance_risk_on": usdt_dominance_risk_on,
            "capital_flow_text": capital_flow_text,
            "sector_strength_ok": sector_ok,
            "sector_status_text": f"{sector_status_text} ({sector_ratio*100:.0f}%)",
        }
        self.macro_cache[cache_key] = {"ts": now, "data": data}
        return data

    # -------------------------
    # 4) Elite Telegram UI
    # -------------------------
    @staticmethod
    def escape_markdown_v2(text: str) -> str:
        escape_chars = r"_*[]()~`>#+-=|{}.!"
        out = []
        for ch in text:
            if ch in escape_chars:
                out.append("\\")
            out.append(ch)
        return "".join(out)

    @staticmethod
    def mdv2_code(text: Any) -> str:
        value = str(text).replace("\\", "\\\\").replace("`", "\\`")
        return f"`{value}`"

    @staticmethod
    def format_elapsed(ts: datetime, now: Optional[datetime] = None) -> str:
        ref = now or datetime.now(UTC)
        diff = max(0, int((ref - ts).total_seconds()))
        days = diff // 86400
        hours = (diff % 86400) // 3600
        minutes = (diff % 3600) // 60
        if days > 0:
            return f"{days}d {hours}h"
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"

    def send_target_hit_update(
        self,
        *,
        row: dict,
        target_number: int,
        current_price: float,
        leverage: str = "x10",
    ) -> None:
        if not self.token:
            return
        if self._run_async_task(self.trade_manager.get_setting("TARGET_REPLY_ACTIVE", "true")).strip().lower() != "true":
            return
        original_chat_id = str(row.get("chat_id") or row.get("original_chat_id") or "").strip()
        original_message_id = row.get("original_message_id")
        if (not original_chat_id) or (not original_message_id):
            return
        entry = float(row.get("entry_price") or 0.0)
        if entry <= 0:
            return
        side = str(row.get("side") or "LONG")
        symbol = str(row.get("symbol") or "")
        target_price = float(row.get(f"tp{target_number}") or current_price)
        raw_pct = abs((target_price - entry) / max(entry, 1e-9)) * 100
        lev = 10.0
        try:
            lev = float(str(leverage).lower().replace("x", "").strip())
        except Exception:
            lev = 10.0
        profit_pct = raw_pct * lev
        ts = row.get("timestamp")
        if isinstance(ts, datetime):
            signal_ts = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
        else:
            signal_ts = datetime.now(UTC)
        elapsed = self.format_elapsed(signal_ts)
        entry_text = f"${entry:,.2f}"
        if side == "SHORT":
            # Profit sign remains positive; target is lower than entry by design.
            profit_pct = abs(profit_pct)

        text = (
            f"🎯 *{self.escape_markdown_v2(symbol)} TARGET {target_number} HIT\\!* 📈\n"
            "━━━━━━━━━━━━━━\n"
            f"💰 *Profit:* {self.mdv2_code(f'+{profit_pct:.2f}')}% \\(with {self.escape_markdown_v2(leverage)}\\)\n"
            f"⏳ *Time Elapsed:* {self.mdv2_code(elapsed)}\n"
            f"📥 *Original Entry:* {self.mdv2_code(entry_text)}\n"
            "━━━━━━━━━━━━━━\n"
            f"✅ *Status:* Goal {target_number} reached successfully\\."
        )
        payload = {
            "chat_id": original_chat_id,
            "text": text,
            "parse_mode": "MarkdownV2",
            "reply_to_message_id": int(original_message_id),
            "allow_sending_without_reply": True,
        }
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json=payload,
                timeout=20,
            )
            if not resp.ok:
                print(
                    f"[TG] target_hit reply failed status={resp.status_code} | "
                    f"signal_id={row.get('id')} target=T{target_number} | {resp.text}"
                )
        except Exception as exc:
            print(f"[TG] target_hit reply error signal_id={row.get('id')} target=T{target_number} | {exc}")

    def build_chart_image_bytes(self, exchange_name: str, symbol: str, setup: Optional[dict] = None) -> Optional[bytes]:
        """Build a high-quality 1H candlestick chart from live exchange OHLCV."""
        ex = self.exchanges.get(exchange_name)
        if not ex:
            return None
        try:
            timeframe = "1h"
            rows = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=120)
            if not rows:
                return None

            ohlc = [
                {
                    "ts": int(r[0]),
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                }
                for r in rows
            ]
            fig, ax = plt.subplots(figsize=(12, 6), facecolor="#0b0f14")
            ax.set_facecolor("#0b0f14")
            bull_color = "#00d084"
            bear_color = "#ff4d4f"
            candle_width = 0.58

            for i, candle in enumerate(ohlc):
                o = candle["open"]
                h = candle["high"]
                low_px = candle["low"]
                c = candle["close"]
                color = bull_color if c >= o else bear_color
                ax.plot([i, i], [low_px, h], color=color, linewidth=1.25, solid_capstyle="round")
                body_low = min(o, c)
                body_height = max(abs(c - o), 1e-9)
                rect = plt.Rectangle(
                    (i - candle_width / 2, body_low),
                    candle_width,
                    body_height,
                    facecolor=color,
                    edgecolor=color,
                    linewidth=0.8,
                )
                ax.add_patch(rect)

            last_close = float(ohlc[-1]["close"])
            ax.set_title(
                f"{symbol} • 1H Candles • {exchange_name} Live Feed",
                color="#e5e7eb",
                fontsize=14,
                fontweight="bold",
            )
            ax.tick_params(axis="x", colors="#9ca3af", labelsize=8)
            ax.tick_params(axis="y", colors="#d1d5db", labelsize=9)
            for spine in ax.spines.values():
                spine.set_color("#374151")
            ax.grid(color="#1f2937", linestyle="--", linewidth=0.5, alpha=0.6)
            ax.set_xlim(-1, len(ohlc))
            x_ticks = [0, len(ohlc) // 3, (2 * len(ohlc)) // 3, len(ohlc) - 1]
            x_labels = []
            for idx in x_ticks:
                ts = datetime.fromtimestamp(ohlc[idx]["ts"] / 1000, tz=UTC)
                x_labels.append(ts.strftime("%d %b %H:%M"))
            ax.set_xticks(x_ticks)
            ax.set_xticklabels(x_labels, rotation=0)

            # Visual Genius overlays: S/R, trendline proxy, liquidity sweep, and trade levels.
            highs = [c["high"] for c in ohlc]
            lows = [c["low"] for c in ohlc]
            sr_res = max(highs[-18:])
            sr_sup = min(lows[-18:])
            ax.axhline(sr_res, color="#f59e0b", linestyle="--", linewidth=1.0, alpha=0.8)
            ax.axhline(sr_sup, color="#60a5fa", linestyle="--", linewidth=1.0, alpha=0.8)
            ax.text(1, sr_res, "R", color="#f59e0b", fontsize=8, va="bottom")
            ax.text(1, sr_sup, "S", color="#60a5fa", fontsize=8, va="top")

            # Simple trendline using last swing lows/highs (visual proxy).
            x0 = max(0, len(ohlc) - 20)
            x1 = len(ohlc) - 1
            trend_y0 = min(lows[-20:-10]) if len(ohlc) >= 20 else lows[0]
            trend_y1 = min(lows[-10:]) if len(ohlc) >= 10 else lows[-1]
            ax.plot([x0, x1], [trend_y0, trend_y1], color="#a78bfa", linewidth=1.2, alpha=0.9)

            # Liquidity sweep marker: long wick extremes over the last N candles.
            sweep_start = max(0, len(ohlc) - 12)
            sweep_idx = max(
                range(sweep_start, len(ohlc)),
                key=lambda i: abs(ohlc[i]["high"] - ohlc[i]["low"]),
            )
            ax.scatter([sweep_idx], [ohlc[sweep_idx]["high"]], color="#f97316", s=30, zorder=5)
            ax.text(sweep_idx, ohlc[sweep_idx]["high"], "Sweep", color="#f97316", fontsize=7, va="bottom")

            if setup:
                entry = float(setup.get("entry", 0.0))
                sl = float(setup.get("sl", 0.0))
                tp1 = float(setup.get("tp1", 0.0))
                tp2 = float(setup.get("tp2", 0.0))
                tp3 = float(setup.get("tp3", 0.0))
                if entry > 0:
                    ax.axhline(entry, color="#3b82f6", linestyle="-", linewidth=1.3, alpha=0.95)
                    ax.text(len(ohlc) - 2, entry, "Entry", color="#3b82f6", fontsize=8, va="bottom")
                if sl > 0:
                    ax.axhline(sl, color="#ef4444", linestyle="-", linewidth=1.2, alpha=0.9)
                    ax.text(len(ohlc) - 2, sl, "SL", color="#ef4444", fontsize=8, va="top")
                for idx, tp in enumerate([tp1, tp2, tp3], start=1):
                    if tp > 0:
                        ax.axhline(tp, color="#22c55e", linestyle="-.", linewidth=1.0, alpha=0.9)
                        ax.text(len(ohlc) - 2, tp, f"TP{idx}", color="#22c55e", fontsize=8, va="bottom")

            ax.axhline(last_close, color="#facc15", linestyle=":", linewidth=1.2, alpha=0.95)
            ax.text(
                len(ohlc) - 2,
                last_close,
                f"Last {last_close:,.4f}",
                color="#facc15",
                fontsize=8,
                va="bottom",
            )
            ax.text(
                0.01,
                0.01,
                f"Source: {exchange_name} {timeframe} OHLCV",
                transform=ax.transAxes,
                color="#9ca3af",
                fontsize=8,
                ha="left",
                va="bottom",
            )

            fig.tight_layout()

            buffer = io.BytesIO()
            fig.savefig(buffer, format="png", dpi=220, facecolor=fig.get_facecolor())
            plt.close(fig)
            buffer.seek(0)
            return buffer.getvalue()
        except Exception:
            return None

    def send_telegram(self, exchange_name: str, symbol: str, setup: dict) -> None:
        if not self.vip_token:
            print(f"[TG] skip send | no bot token (set TELEGRAM_BOT_TOKEN / VIP_TELEGRAM_BOT_TOKEN) | {exchange_name} {symbol}")
            return

        key = f"{exchange_name}:{symbol}:{setup['side']}"
        now = time.time()
        if key in self.last_alert_at and (now - self.last_alert_at[key]) < self.alert_cooldown_seconds:
            cd = self.alert_cooldown_seconds
            print(f"[TG] skip duplicate key={key} per_key_cooldown={cd}s (~{cd / 60.0:.1f} min)")
            return
        ex_for_slippage = self.exchanges.get(exchange_name)
        if ex_for_slippage is not None:
            live_price = self.get_realtime_price_fast(ex_for_slippage, symbol)
            entry_px = float(setup.get("entry", 0.0) or 0.0)
            if live_price and entry_px > 0:
                slippage_pct = abs((live_price - entry_px) / entry_px) * 100.0
                if slippage_pct > self.slippage_expire_pct:
                    print(
                        f"[EXEC] skip expired {exchange_name} {symbol} | "
                        f"slippage {slippage_pct:.2f}% > max {self.slippage_expire_pct:.2f}%"
                    )
                    if setup.get("signal_id") is not None:
                        try:
                            self._run_async_task(
                                self.trade_manager.update_signal_status(
                                    signal_id=int(setup["signal_id"]),
                                    status="Expired",
                                    last_price=float(live_price),
                                )
                            )
                        except Exception as exc:
                            print(f"[EXEC] failed to mark signal {setup.get('signal_id')} expired: {exc}")
                    return
        self._refresh_daily_flow_state()
        now_il = datetime.now(self.il_tz)
        sec_of_day = (now_il.hour * 3600) + (now_il.minute * 60) + now_il.second
        day_progress = min(1.0, max(0.0, sec_of_day / 86400.0))
        sent_today = int(self.daily_flow.get("chat_sent", 0))
        expected_by_now = self.daily_target_chat_signals * day_progress
        adaptive_gap_seconds = self.min_signal_gap_seconds
        if sent_today < (expected_by_now - 2):
            adaptive_gap_seconds = max(12, int(self.min_signal_gap_seconds * 0.6))
        elif sent_today > (expected_by_now + 2):
            adaptive_gap_seconds = int(self.min_signal_gap_seconds * 1.5)
        with self.signal_pacing_lock:
            if self.max_signals_per_cycle > 0 and self.cycle_signal_sent_count >= self.max_signals_per_cycle:
                print(
                    f"[PACE] skip {exchange_name} {symbol} | cycle_cap "
                    f"{self.cycle_signal_sent_count}/{self.max_signals_per_cycle}"
                )
                return
            since_last = now - self.last_global_signal_sent_at
            if since_last < adaptive_gap_seconds:
                print(
                    f"[PACE] skip {exchange_name} {symbol} | min_gap "
                    f"{since_last:.1f}s / need {adaptive_gap_seconds}s"
                )
                return

        try:
            signal_id_opt: Optional[int] = None
            try:
                if setup.get("signal_id") is not None:
                    signal_id_opt = int(setup.get("signal_id"))
            except Exception:
                signal_id_opt = None
            self._refresh_daily_flow_state()
            side_caps = "LONG" if setup["side"] == "LONG" else "SHORT"
            stop_pct = abs((setup["entry"] - setup["sl"]) / max(setup["entry"], 1e-9)) * 100
            signal_strength = max(80, min(99, int(setup["score"] * 10)))
            rr_text = f"1:{setup['rr']:.2f}"
            tech_blocks = self.build_technical_analysis_blocks(symbol=symbol, side_caps=side_caps, setup=setup)
            leverage = setup.get("leverage", "x10")
            entry_text = f"${setup['entry']:,.2f}"
            sl_text = f"${setup['sl']:,.2f}"
            tp1_text = f"${setup['tp1']:,.2f}"
            tp2_text = f"${setup['tp2']:,.2f}"
            tp3_text = f"${setup['tp3']:,.2f}"
            stop_pct_text = f"{stop_pct:.2f}"
            chat_base_caption = (
                "💎🦅 *FALCON ALERT* 🦅💎\n\n"
                f"*{self.mdv2_code(symbol)}*\n"
                f"Position: *{self.mdv2_code(side_caps)}* 📈\n"
                f"Leverage: *{self.mdv2_code(leverage)}*\n"
                "━━━━━━━━━━━━━━\n"
                f"📥 *Entry:* {self.mdv2_code(entry_text)}\n"
                f"🛑 *Stop Loss:* {self.mdv2_code(sl_text)} \\({self.mdv2_code(f'{stop_pct_text}%')}\\)\n"
                "━━━━━━━━━━━━━━\n"
                "*Targets*\n"
                f"🎯 T1: {self.mdv2_code(tp1_text)}\n"
                f"🎯 T2: {self.mdv2_code(tp2_text)}\n"
                f"🎯 T3: {self.mdv2_code(tp3_text)}"
            )
            vip_caption = (
                "⚡️🚨 *ELITE SIGNAL* 🚨⚡️\n\n"
                f"*{self.mdv2_code(symbol)}*\n"
                f"Position: *{self.mdv2_code(side_caps)}* 📈\n"
                f"Leverage: *{self.mdv2_code(leverage)}*\n"
                "━━━━━━━━━━━━━━\n"
                f"📥 *Entry:* {self.mdv2_code(entry_text)}\n"
                f"🛑 *Stop Loss:* {self.mdv2_code(sl_text)} \\({self.mdv2_code(f'{stop_pct_text}%')}\\)\n"
                "━━━━━━━━━━━━━━\n"
                "*Targets*\n"
                f"🎯 T1: {self.mdv2_code(tp1_text)}\n"
                f"🎯 T2: {self.mdv2_code(tp2_text)}\n"
                f"🎯 T3: {self.mdv2_code(tp3_text)}\n"
                "━━━━━━━━━━━━━━\n"
                "📊 *Technical Analysis*\n"
                f"🔹 *Context:* {self.mdv2_code(tech_blocks['context_detailed'])}\n"
                f"🔹 *Support/Resistance:* {self.mdv2_code(tech_blocks['levels_detailed'])}\n"
                f"🔹 *Indicators:* {self.mdv2_code(tech_blocks['indicators_detailed'])}\n"
                f"🔹 *Risk:* {self.mdv2_code(tech_blocks['risk_detailed'])}\n"
                "━━━━━━━━━━━━━━\n"
                "💧 *Liquidity*\n"
                f"🔹 Status: {self.mdv2_code(setup.get('liquidity_status', 'Liquidity Neutral'))}\n"
                f"🔹 Confirmation: {self.mdv2_code(setup.get('liquidity_confirmation', 'No Liquidity Confirmation'))}\n"
                "━━━━━━━━━━━━━━\n"
                "⚖️ *Risk / Reward*\n"
                f"🔹 Ratio: {self.mdv2_code(rr_text)}\n"
                f"🔹 Risk to SL: {self.mdv2_code(f'{stop_pct_text}%')}\n"
                "━━━━━━━━━━━━━━\n"
                "✨ *VIP\\+ EXTENSIONS*\n"
                f"🔥 Confidence Level: {self.mdv2_code(f'{signal_strength}%')}\n"
                "━━━━━━━━━━━━━━"
            )
            # VIP regular template is deterministic MarkdownV2 for copy-ready values.
            vip_chart_image_bytes = self.build_chart_image_bytes(exchange_name, symbol, setup=setup)

            def _send(
                chat_id: str,
                caption: str,
                include_live_button: bool = False,
                force_text: bool = False,
                parse_mode: Optional[str] = None,
                token: Optional[str] = None,
            ) -> Optional[int]:
                use_token = (token or self.vip_token or self.token).strip()
                if not use_token:
                    return None
                live_symbol = symbol.replace("/", "")
                live_url = (
                    f"https://www.binance.com/en/trade/{live_symbol}"
                    if exchange_name.lower() == "binance"
                    else f"https://www.bybit.com/trade/usdt/{live_symbol}"
                )
                max_caption_len = 1024
                if len(caption) > max_caption_len:
                    tail_anchor = "\n━━━━━━━━━━━━━━\n⚖️ *Risk / Reward*"
                    if tail_anchor not in caption:
                        tail_anchor = "\n━━━━━━━━━━━━━━\n💧 *Liquidity*"
                    if tail_anchor in caption:
                        head, tail = caption.split(tail_anchor, 1)
                        tail = tail_anchor + tail
                        keep_head = max_caption_len - len(tail) - 3
                        if keep_head > 120:
                            caption = head[:keep_head] + "..." + tail
                        else:
                            caption = caption[: max_caption_len - 3] + "..."
                    else:
                        caption = caption[: max_caption_len - 3] + "..."
                reply_markup = (
                    {"inline_keyboard": [[{"text": "📊 Live Chart", "url": live_url}]]}
                    if include_live_button
                    else None
                )
                if (not force_text) and vip_chart_image_bytes:
                    data_payload = {"chat_id": chat_id, "caption": caption}
                    if parse_mode:
                        data_payload["parse_mode"] = parse_mode
                    if reply_markup:
                        data_payload["reply_markup"] = json.dumps(reply_markup)
                    resp = requests.post(
                        f"https://api.telegram.org/bot{use_token}/sendPhoto",
                        data=data_payload,
                        files={"photo": ("fortress_chart.png", vip_chart_image_bytes, "image/png")},
                        timeout=30,
                    )
                else:
                    json_payload = {"chat_id": chat_id, "text": caption}
                    if parse_mode:
                        json_payload["parse_mode"] = parse_mode
                    if reply_markup:
                        json_payload["reply_markup"] = reply_markup
                    resp = requests.post(
                        f"https://api.telegram.org/bot{use_token}/sendMessage",
                        json=json_payload,
                        timeout=20,
                    )
                if not resp.ok:
                    print(f"[TG] send failed status={resp.status_code} chat={chat_id} | {exchange_name} {symbol} | {resp.text}")
                    return None
                try:
                    body = resp.json()
                    return int(body.get("result", {}).get("message_id"))
                except Exception:
                    return None

            sent_any = False

            force_dual = bool(setup.get("force_dual_send", False) or self.force_dual_test_send)
            vip_plus_quality = bool(setup.get("vip_plus_ok", False))
            vip_plus_soft = bool(setup.get("vip_plus_soft_ok", False))
            vip_quality = bool(setup.get("vip_ok", False))
            vip_soft = bool(setup.get("vip_soft_ok", False))
            setup_score = int(setup.get("score", 0))
            signal_strength = max(80, min(99, int(setup_score * 10)))
            indicator_align = int(setup.get("indicator_alignment_count_5", 0))
            volume_ok = bool(setup.get("volume_confirmed", False))
            mtf_ok = bool(setup.get("mtf_confluence_ok", False))
            vwap_ok = bool(setup.get("vwap_institutional_ok", False))
            vip_logic_ok = indicator_align >= 3 and volume_ok
            vip_plus_logic_ok = indicator_align >= 5 and mtf_ok and vwap_ok and volume_ok

            # Dynamic daily target windows tuned for higher daily throughput.
            vip_plus_sent_today = int(self.daily_flow.get("vip_plus_sent", 0))
            chat_sent_today = int(self.daily_flow.get("chat_sent", 0))

            allow_vip_plus = False
            if force_dual:
                allow_vip_plus = True
            else:
                vip_plus_soft_cap = max(1, self.daily_target_vip_plus_signals // 2)
                vip_plus_target_cap = max(vip_plus_soft_cap + 1, self.daily_target_vip_plus_signals)
                if vip_plus_sent_today < vip_plus_soft_cap:
                    allow_vip_plus = vip_plus_soft
                elif vip_plus_sent_today < vip_plus_target_cap:
                    allow_vip_plus = vip_plus_quality
                else:
                    allow_vip_plus = vip_plus_quality and int(setup.get("score", 0)) >= 9

            allow_vip_chat = False
            if force_dual:
                allow_vip_chat = True
            else:
                chat_soft_cap = max(1, self.daily_target_chat_signals // 2)
                chat_target_cap = max(chat_soft_cap + 1, self.daily_target_chat_signals)
                if chat_sent_today < chat_soft_cap:
                    allow_vip_chat = vip_soft
                elif chat_sent_today < chat_target_cap:
                    allow_vip_chat = vip_quality
                else:
                    allow_vip_chat = vip_quality and int(setup.get("score", 0)) >= 9

            vip_channel_on = self._run_async_task(
                self.trade_manager.get_setting("CHANNEL_VIP_ACTIVE", "true")
            ).strip().lower() == "true"
            vip_plus_channel_on = self._run_async_task(
                self.trade_manager.get_setting("CHANNEL_VIP_PLUS_ACTIVE", "true")
            ).strip().lower() == "true"
            if not vip_channel_on:
                allow_vip_chat = False
            if not vip_plus_channel_on:
                allow_vip_plus = False

            allow_vip_chat = bool(
                allow_vip_chat
                and vip_channel_on
                and vip_logic_ok
                and signal_strength >= self.vip_min_confidence_pct
            )
            allow_vip_plus = bool(
                allow_vip_plus
                and vip_plus_channel_on
                and self.vip_plus_chat_id
                and vip_plus_logic_ok
                and signal_strength >= self.vip_plus_min_confidence_pct
            )
            if allow_vip_plus and signal_strength < self.vip_plus_downgrade_pct:
                allow_vip_plus = False
                allow_vip_chat = bool(allow_vip_chat or (vip_channel_on and vip_logic_ok))

            # VIP+ gets only 99%-tier confirmations (layers 1-8 all pass).
            if self.vip_plus_chat_id and allow_vip_plus:
                msg_id = _send(
                    self.vip_plus_chat_id,
                    vip_caption,
                    include_live_button=True,
                    parse_mode="MarkdownV2",
                    token=self.vip_plus_token,
                )
                if msg_id:
                    self.vip_plus_messages_sent += 1
                    self.daily_flow["vip_plus_sent"] = vip_plus_sent_today + 1
                    sent_any = True
                    if signal_id_opt is not None:
                        self._run_async_task(
                            self.trade_manager.attach_original_message(
                                signal_id=signal_id_opt,
                                chat_id=self.vip_plus_chat_id,
                                message_id=msg_id,
                            )
                        )
                    setup["original_message_saved"] = True
                else:
                    # Fallback: ensure VIP+ still receives signal if photo flow failed.
                    fallback_msg_id = _send(
                        self.vip_plus_chat_id,
                        vip_caption,
                        include_live_button=False,
                        force_text=True,
                        token=self.vip_plus_token,
                    )
                    if fallback_msg_id:
                        self.vip_plus_messages_sent += 1
                        self.daily_flow["vip_plus_sent"] = vip_plus_sent_today + 1
                        sent_any = True
                        if signal_id_opt is not None:
                            self._run_async_task(
                                self.trade_manager.attach_original_message(
                                    signal_id=signal_id_opt,
                                    chat_id=self.vip_plus_chat_id,
                                    message_id=fallback_msg_id,
                                )
                            )
                        setup["original_message_saved"] = True
                        print(f"[TG] VIP+ sent as plain text fallback | {exchange_name} {symbol}")
            elif self.vip_plus_chat_id:
                print(
                    f"[TG] VIP+ not sent (gates) | {exchange_name} {symbol} | "
                    f"allow={allow_vip_plus} vip_plus_ok={setup.get('vip_plus_ok')} "
                    f"vip_plus_soft_ok={setup.get('vip_plus_soft_ok')} vip_plus_sent_today={vip_plus_sent_today}"
                )

            # Regular VIP now uses chart-first layout as well (photo + caption),
            # so the chart appears above the signal title in Telegram.
            if self.chat_id and allow_vip_chat:
                msg_id = _send(
                    self.chat_id,
                    chat_base_caption,
                    include_live_button=True,
                    parse_mode="MarkdownV2",
                    token=self.vip_token,
                )
                if msg_id:
                    self.vip_channel_messages_sent += 1
                    self.daily_flow["chat_sent"] = chat_sent_today + 1
                    sent_any = True
                    if not setup.get("original_message_saved"):
                        if signal_id_opt is not None:
                            self._run_async_task(
                                self.trade_manager.attach_original_message(
                                    signal_id=signal_id_opt,
                                    chat_id=self.chat_id,
                                    message_id=msg_id,
                                )
                            )
                        setup["original_message_saved"] = True

            if sent_any:
                self.last_alert_at[key] = now
                with self.signal_pacing_lock:
                    self.last_global_signal_sent_at = now
                    self.cycle_signal_sent_count += 1
                self.daily_flow["global_sent"] = int(self.daily_flow.get("global_sent", 0)) + 1
                try:
                    self._run_async_task(
                        self.trade_manager.mark_symbol_cooldown(
                            exchange_name=exchange_name,
                            symbol=symbol,
                            cooldown_seconds=self.symbol_cooldown_seconds,
                        )
                    )
                    self._run_async_task(
                        self.trade_manager.register_signal_send(
                            exchange_name=exchange_name,
                            symbol=symbol,
                            window_seconds=self.global_throttle_window_sec,
                        )
                    )
                except Exception as exc:
                    print(f"[THROTTLE] persist send metadata failed: {exc}")
                print(
                    f"[TG] sent | {exchange_name} {symbol} | score={setup['score']}/10 | "
                    f"vip_plus_total={self.vip_plus_messages_sent} vip_channel_total={self.vip_channel_messages_sent}"
                )
            else:
                print(
                    f"[TG] not sent (no channel accepted this setup) | {exchange_name} {symbol}"
                )
        except Exception as exc:
            print(f"[TG] send exception | {exchange_name} {symbol} | {exc}")
            self.send_admin_notification(
                f"Telegram send error | {exchange_name} {symbol} | {exc}",
                loud=True,
            )

    # -------------------------
    # 5) Technical Optimization + DB integration
    # -------------------------
    def process_symbol(self, exchange_name: str, ex: ccxt.Exchange, symbol: str) -> None:
        self._refresh_daily_flow_state()
        if int(self.daily_flow.get("global_sent", 0)) >= self.global_daily_signal_limit:
            return
        try:
            if self._run_async_task(self.trade_manager.is_pair_blacklisted(symbol)):
                print(f"[QUALITY] skipped {exchange_name} {symbol} (blacklisted 24h)")
                return
        except Exception as exc:
            # Fail-open on blacklist gate (DB hiccup) but log it — silent failure here
            # could let a blacklisted pair through unnoticed.
            print(f"[QUALITY] blacklist check unavailable for {exchange_name} {symbol}: {exc}")
        try:
            if self._run_async_task(
                self.trade_manager.is_symbol_on_cooldown(exchange_name=exchange_name, symbol=symbol)
            ):
                print(f"[COOLDOWN] skipped {exchange_name} {symbol} (symbol cooldown active)")
                return
            sent_in_window = self._run_async_task(
                self.trade_manager.count_signals_last_window(self.global_throttle_window_sec)
            )
            if int(sent_in_window) >= self.global_throttle_max_signals:
                print(
                    f"[THROTTLE] skipped {exchange_name} {symbol} "
                    f"(window {sent_in_window}/{self.global_throttle_max_signals} in {self.global_throttle_window_sec}s)"
                )
                return
        except Exception as exc:
            print(f"[THROTTLE] Redis rate window unavailable, continuing without Redis gate: {exc}")
        data = self.fetch_multi_timeframes(ex, symbol)
        if not data:
            return
        fast_price = self.get_realtime_price_fast(ex, symbol)
        if fast_price and fast_price > 0:
            for tf in ("5m", "15m"):
                if tf in data and (not data[tf].empty):
                    data[tf].iat[-1, data[tf].columns.get_loc("close")] = float(fast_price)
                    if float(data[tf].iloc[-1]["high"]) < float(fast_price):
                        data[tf].iat[-1, data[tf].columns.get_loc("high")] = float(fast_price)
                    if float(data[tf].iloc[-1]["low"]) > float(fast_price):
                        data[tf].iat[-1, data[tf].columns.get_loc("low")] = float(fast_price)
        # Institutional MTF confluence: 5m idea must align with 15m and 1h trend.
        d15 = self.add_indicators(data["15m"])
        d1h = self.add_indicators(data["1h"])
        if d15.empty or d1h.empty:
            return
        setup = self.detect_setup(ex, symbol, data)
        if (not setup) and self.aggressive_signal_mode:
            setup = self.build_aggressive_fallback_setup(data)
        if not setup:
            return
        side = str(setup.get("side", "LONG"))
        mtf_long_ok = float(d15.iloc[-1]["close"]) > float(d15.iloc[-1]["ema50"]) and float(d1h.iloc[-1]["close"]) > float(d1h.iloc[-1]["ema50"])
        mtf_short_ok = float(d15.iloc[-1]["close"]) < float(d15.iloc[-1]["ema50"]) and float(d1h.iloc[-1]["close"]) < float(d1h.iloc[-1]["ema50"])
        if (side == "LONG" and not mtf_long_ok) or (side == "SHORT" and not mtf_short_ok):
            print(f"[QUALITY] skipped {exchange_name} {symbol} (MTF trend not aligned)")
            return
        tf_key = str(setup.get("timeframe", "15m"))
        penalty = int(self.exchange_score_penalty.get(exchange_name, 0)) + int(
            self.timeframe_score_penalty.get(tf_key, 0)
        )
        if penalty > 0:
            setup["score"] = max(1, int(setup.get("score", 0)) - penalty)
        setup_score = int(setup.get("score", 0))
        strict_alignment = bool(setup.get("vip_plus_ok", False)) or (
            bool(setup.get("vip_ok", False))
            and float(setup.get("rr", 0.0)) >= 2.5
            and float(setup.get("rel_volume", 0.0)) >= 1.6
            and float(setup.get("adx", 0.0)) >= 20.0
        )
        if setup_score < self.strict_quality_score and (not strict_alignment):
            print(
                f"[QUALITY] skipped {exchange_name} {symbol} "
                f"(score={setup_score}/10 strict={self.strict_quality_score}/10 align={strict_alignment})"
            )
            return
        # Adaptive floor: when daily flow is below target, allow slightly lower score floor.
        self._refresh_daily_flow_state()
        adaptive_floor = self.min_score_threshold
        if int(self.daily_flow.get("chat_sent", 0)) < 10:
            adaptive_floor = max(5, self.min_score_threshold - 1)
        if int(setup["score"]) < adaptive_floor:
            return
        entry_val = float(setup.get("entry", 0.0) or 0.0)
        tp1_val = float(setup.get("tp1", 0.0) or 0.0)
        if entry_val > 0 and tp1_val > 0:
            tp1_distance_pct = abs((tp1_val - entry_val) / entry_val) * 100.0
            if tp1_distance_pct < self.min_tp1_distance_pct:
                print(
                    f"[QUALITY] skipped {exchange_name} {symbol} "
                    f"(tp1 distance {tp1_distance_pct:.2f}% < min {self.min_tp1_distance_pct:.2f}%)"
                )
                return
        entry = float(setup.get("entry", 0.0) or 0.0)
        tp1 = float(setup.get("tp1", 0.0) or 0.0)
        tp3 = float(setup.get("tp3", 0.0) or 0.0)
        if entry > 0 and tp1 > 0 and tp3 > 0:
            tp1_move_pct = abs((tp1 - entry) / entry) * 100.0
            tp3_move_pct = abs((tp3 - entry) / entry) * 100.0
            if tp1_move_pct < self.min_tp1_pct or tp3_move_pct < self.min_tp3_pct:
                print(
                    f"[RISK] skipped {symbol}: tiny targets "
                    f"(tp1={tp1_move_pct:.2f}% tp3={tp3_move_pct:.2f}%) "
                    f"< min(tp1={self.min_tp1_pct:.2f}% tp3={self.min_tp3_pct:.2f}%)"
                )
                return

        if self.has_recent_signal(symbol, minutes=30):
            print(f"[SIGNAL] skip | duplicate in last 30m symbol={symbol}")
            return
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        total_today, lowest_pending = self._run_async_task(
            self.trade_manager.get_daily_count_and_lowest_pending(today)
        )
        if total_today >= self.global_daily_signal_limit:
            if (not lowest_pending) or setup_score <= int(lowest_pending.get("score") or 0):
                print(
                    f"[CAP] skipped {exchange_name} {symbol}: cap reached "
                    f"({total_today}/{self.global_daily_signal_limit})"
                )
                return
            replace_id = int(lowest_pending["id"])
            replace_last = float(lowest_pending.get("last_price") or lowest_pending.get("entry_price") or 0.0)
            self._run_async_task(
                self.trade_manager.update_signal_status(
                    signal_id=replace_id,
                    status="Replaced",
                    last_price=replace_last,
                )
            )
            print(
                f"[CAP] replaced pending signal id={replace_id} "
                f"score={int(lowest_pending.get('score') or 0)} with {symbol} score={setup_score}"
            )

        signal_id = self.save_signal(exchange_name, symbol, setup)
        setup["signal_id"] = int(signal_id)
        self._run_async_task(self.trade_manager.cache_layer_diagnostics(signal_id, setup))
        self.send_telegram(exchange_name, symbol, setup)

    def build_aggressive_fallback_setup(self, dfs: Dict[str, pd.DataFrame]) -> Optional[dict]:
        try:
            d1h = self.add_indicators(dfs["1h"])
            d15 = self.add_indicators(dfs["15m"])
            d5 = self.add_indicators(dfs["5m"])
            if d1h.iloc[-1].isna().any() or d15.iloc[-1].isna().any() or d5.iloc[-1].isna().any():
                return None
            last_1h = d1h.iloc[-1]
            last_15 = d15.iloc[-1]
            last_5 = d5.iloc[-1]
            price = float(last_5["close"])
            atr = float(last_5["atr"])
            if atr <= 0:
                return None

            trend_up = float(last_1h["close"]) >= float(last_1h["ema50"])
            trend_down = float(last_1h["close"]) < float(last_1h["ema50"])
            if not (trend_up or trend_down):
                return None
            side = "LONG" if trend_up else "SHORT"

            sl = price - (1.3 * atr) if side == "LONG" else price + (1.3 * atr)
            risk = abs(price - sl)
            tp1 = price + (1.2 * risk) if side == "LONG" else price - (1.2 * risk)
            tp2 = price + (2.0 * risk) if side == "LONG" else price - (2.0 * risk)
            tp3 = price + (3.0 * risk) if side == "LONG" else price - (3.0 * risk)
            # Ensure fallback targets are meaningful in percentage terms.
            if side == "LONG":
                tp1 = max(tp1, price * (1 + self.min_tp1_pct / 100.0))
                tp2 = max(tp2, price * (1 + max(1.3, self.min_tp1_pct * 1.6) / 100.0))
                tp3 = max(tp3, price * (1 + self.min_tp3_pct / 100.0))
            else:
                tp1 = min(tp1, price * (1 - self.min_tp1_pct / 100.0))
                tp2 = min(tp2, price * (1 - max(1.3, self.min_tp1_pct * 1.6) / 100.0))
                tp3 = min(tp3, price * (1 - self.min_tp3_pct / 100.0))
            potential_gain_pct = abs((tp3 - price) / max(price, 1e-9)) * 100
            potential_loss_pct = abs((price - sl) / max(price, 1e-9)) * 100
            rr = potential_gain_pct / max(potential_loss_pct, 1e-9)
            risk_pct = max(0.1, min(self.max_risk_per_trade_pct, self.risk_per_trade_pct))
            pos_size = self.compute_position_size(entry=price, sl=sl, equity=self.virtual_balance, risk_pct=risk_pct)

            return {
                "side": side,
                "entry": price,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "score": 8,
                "breakdown": "Aggressive momentum fallback",
                "potential_gain_pct": potential_gain_pct,
                "potential_loss_pct": potential_loss_pct,
                "rr": rr,
                "adx": float(last_1h.get("adx", 20.0)),
                "rsi": float(last_15.get("rsi", 50.0)),
                "rel_volume": float(last_5.get("rel_volume", 1.0)),
                "liquidity_status": "Aggressive Momentum Flow",
                "liquidity_confirmation": "Fallback route enabled",
                "sync_text": "Bypassed (aggressive fallback)",
                "beta_vs_btc": 0.0,
                "dxy_status": "Bypassed",
                "usdt_dom": 0.0,
                "capital_flow_text": "Bypassed",
                "sector_status_text": "Bypassed",
                "order_book_depth": "Bypassed",
                "vip_ok": True,
                "vip_plus_ok": False,
                "vip_soft_ok": True,
                "vip_plus_soft_ok": False,
                "vip_layer_lines": ["Aggressive fallback setup (forced throughput)"],
                "vip_plus_layer_lines": ["Aggressive fallback setup (forced throughput)"],
                "social_text": "Bypassed",
                "whale_text": "Bypassed",
                "analysis_text": "Aggressive fallback setup",
                "context_detailed": "Momentum continuation structure with throughput fallback enabled.",
                "levels_detailed": "Entry/SL/TP derived from ATR dynamics on live 5m-1h structure.",
                "indicators_detailed": "EMA50 trend alignment with RSI/volume baseline checks.",
                "risk_detailed": "Reduced strict gating mode: manage size conservatively.",
                "timeframe": "15m",
                "position_size": float(pos_size),
                "risk_pct": float(risk_pct),
            }
        except Exception:
            return None

    def _refresh_daily_flow_state(self) -> None:
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        if self.daily_flow.get("date") != today:
            self.daily_flow = {"date": today, "chat_sent": 0, "vip_plus_sent": 0, "global_sent": 0}

    def scan_exchange(self, exchange_name: str, ex: ccxt.Exchange) -> None:
        symbols = self.top_usdt_symbols(exchange_name, ex)
        if not symbols:
            return
        worker_failures = 0
        first_error: Optional[str] = None
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(self.process_symbol, exchange_name, ex, symbol) for symbol in symbols]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as exc:
                    worker_failures += 1
                    if first_error is None:
                        first_error = str(exc)
                    print(f"[SCAN] {exchange_name} | symbol task error: {exc}")
        if worker_failures > 0:
            self.notify_worker_failure_once(
                exchange_name=exchange_name,
                failed=worker_failures,
                total=len(symbols),
                first_error=(first_error or ""),
            )

    async def _process_symbol_async(
        self,
        exchange_name: str,
        ex: ccxt.Exchange,
        symbol: str,
        sem: asyncio.Semaphore,
    ) -> Optional[Exception]:
        async with sem:
            try:
                await asyncio.to_thread(self.process_symbol, exchange_name, ex, symbol)
                return None
            except Exception as exc:
                return exc

    async def scan_exchange_async(self, exchange_name: str, ex: ccxt.Exchange, sem: asyncio.Semaphore) -> None:
        symbols = await asyncio.to_thread(self.top_usdt_symbols, exchange_name, ex)
        if not symbols:
            return
        tasks = [
            asyncio.create_task(self._process_symbol_async(exchange_name, ex, symbol, sem))
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        worker_failures = 0
        first_error: Optional[str] = None
        for r in results:
            if r is None:
                continue
            worker_failures += 1
            if first_error is None:
                first_error = str(r)
            print(f"[SCAN] {exchange_name} | symbol task error: {r}")
        if worker_failures > 0:
            self.notify_worker_failure_once(
                exchange_name=exchange_name,
                failed=worker_failures,
                total=len(symbols),
                first_error=(first_error or ""),
            )

    def run_cycle(self) -> None:
        if not self._run_async_task(self.trade_manager.is_bot_active()):
            print("[SCAN] cycle skipped | BOT_ACTIVE=false")
            return
        with self.signal_pacing_lock:
            self.cycle_signal_sent_count = 0
        print("\n[SCAN] === cycle start (sync path) ===")
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(self.scan_exchange, name, ex) for name, ex in self.exchanges.items()]
            for job in as_completed(jobs):
                try:
                    job.result()
                except Exception as exc:
                    print(f"[SCAN] exchange batch thread failed: {exc}")
                    self.send_admin_notification(
                        f"Scanner exchange batch failed: {exc}",
                        loud=True,
                    )
        print("[SCAN] === cycle complete (sync path) ===")

    async def run_cycle_async(self) -> None:
        if not await self.trade_manager.is_bot_active():
            print("[SCAN] cycle skipped | BOT_ACTIVE=false")
            return
        with self.signal_pacing_lock:
            self.cycle_signal_sent_count = 0
        print("\n[SCAN] === cycle start ===")
        sem = asyncio.Semaphore(max(4, self.scan_semaphore_size))
        jobs = [
            asyncio.create_task(self.scan_exchange_async(name, ex, sem))
            for name, ex in self.exchanges.items()
        ]
        results = await asyncio.gather(*jobs, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                print(f"[SCAN] exchange async batch failed: {res}")
                self.send_admin_notification(
                    f"Scanner exchange batch failed: {res}",
                    loud=True,
                )
        print("[SCAN] === cycle complete ===")

    def run_forever(self) -> None:
        while True:
            try:
                self._run_async_task(self.run_cycle_async())
            except Exception as exc:
                print(f"[FATAL] scan loop error: {exc}")
                self._notify_if_infra_error(exc, "run_forever")
                if not self._infra_component_from_error(str(exc)):
                    self.send_admin_notification(f"FATAL cycle error: {exc}", loud=True)
            print(f"[SCAN] sleep {self.scan_interval_seconds}s until next cycle\n")
            time.sleep(self.scan_interval_seconds)


if __name__ == "__main__":
    scanner = FortressScanner()
    scanner.run_forever()
