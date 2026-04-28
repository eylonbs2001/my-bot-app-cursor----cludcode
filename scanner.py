import os
import json
import sqlite3
import threading
import time
import io
import statistics
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, UTC
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import ccxt
import matplotlib.pyplot as plt
import pandas as pd
import requests
import asyncpg
import redis.asyncio as redis_async
from dotenv import load_dotenv

try:
    import pandas_ta as ta
except Exception:
    import pandas_ta_classic as ta

load_dotenv()


class TradeManager:
    """Async Postgres + Redis trade persistence layer."""

    def __init__(self) -> None:
        self.db_url = (
            os.getenv("DATABASE_URL", "").strip()
            or os.getenv("DB_URL", "").strip()
        )
        self.pg_host = os.getenv("POSTGRES_HOST", "localhost")
        self.pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.pg_user = os.getenv("POSTGRES_USER", "fortress")
        self.pg_password = os.getenv("POSTGRES_PASSWORD", "fortress")
        self.pg_database = os.getenv("POSTGRES_DB", "trading_db")
        self.redis_url = (
            os.getenv("REDIS_URL")
            or os.getenv("REDIS_PRIVATE_URL")
            or os.getenv("REDIS_PUBLIC_URL")
            or "redis://localhost:6379/0"
        )
        self.pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[redis_async.Redis] = None

    async def startup(self) -> List[dict]:
        if os.getenv("DATABASE_URL"):
            print(f"DEBUG: Connecting to DB using URL: {os.getenv('DATABASE_URL')[:20]}...")
        if self.db_url:
            self.pool = await asyncpg.create_pool(
                dsn=self.db_url,
                min_size=1,
                max_size=10,
            )
        else:
            self.pool = await asyncpg.create_pool(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password,
                database=self.pg_database,
                min_size=1,
                max_size=10,
            )
        # Hard connectivity probe for startup visibility.
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        print("PostgreSQL Connected")
        if os.getenv("REDIS_URL"):
            print(f"DEBUG: Connecting to Redis using URL: {os.getenv('REDIS_URL')[:20]}...")
        self.redis = redis_async.from_url(self.redis_url, decode_responses=True)
        assert self.redis is not None
        await self.redis.ping()
        print("Redis Connected")
        await self._init_schema()
        return await self.recover_active_trades()

    async def _init_schema(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry_price DOUBLE PRECISION NOT NULL,
                    stop_loss DOUBLE PRECISION NOT NULL,
                    tp1 DOUBLE PRECISION NOT NULL,
                    tp2 DOUBLE PRECISION NOT NULL,
                    tp3 DOUBLE PRECISION NOT NULL,
                    score INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'Pending',
                    last_price DOUBLE PRECISION
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signals_symbol_time_pg ON signals(symbol, timestamp)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signals_status_pg ON signals(status)"
            )

    async def recover_active_trades(self) -> List[dict]:
        assert self.pool is not None
        assert self.redis is not None
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
        for trade in active:
            await self.redis.hset(
                f"active_trade:{trade['id']}",
                mapping={k: str(v) for k, v in trade.items()},
            )
        return active

    async def has_recent_signal(self, symbol: str, minutes: int = 30) -> bool:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT 1
                FROM signals
                WHERE symbol = $1
                  AND timestamp >= NOW() - ($2::text || ' minutes')::interval
                LIMIT 1
                """,
                symbol,
                minutes,
            )
        return row is not None

    async def save_signal(self, exchange_name: str, symbol: str, setup: dict) -> int:
        assert self.pool is not None
        assert self.redis is not None
        async with self.pool.acquire() as conn:
            signal_id = await conn.fetchval(
                """
                INSERT INTO signals (
                    exchange, symbol, side, entry_price, stop_loss, tp1, tp2, tp3, score, status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'Pending')
                RETURNING id
                """,
                exchange_name,
                symbol,
                setup["side"],
                float(setup["entry"]),
                float(setup["sl"]),
                float(setup["tp1"]),
                float(setup["tp2"]),
                float(setup["tp3"]),
                int(setup["score"]),
            )
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
            },
        )
        return int(signal_id)

    async def fetch_pending_signals(self) -> List[dict]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, exchange, symbol, side, stop_loss, tp1, tp2, tp3
                FROM signals
                WHERE status = 'Pending'
                ORDER BY id ASC
                """
            )
        return [dict(r) for r in rows]

    async def update_signal_status(self, signal_id: int, status: str, last_price: float) -> None:
        assert self.pool is not None
        assert self.redis is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE signals SET status = $1, last_price = $2 WHERE id = $3",
                status,
                float(last_price),
                int(signal_id),
            )
        await self.redis.delete(f"active_trade:{signal_id}")

    async def sync_active_price(self, exchange_name: str, symbol: str, price: float) -> None:
        assert self.redis is not None
        await self.redis.set(f"price:{exchange_name}:{symbol}", f"{float(price):.10f}", ex=900)

    async def cache_layer_diagnostics(self, signal_id: int, setup: dict) -> None:
        assert self.redis is not None
        payload = {
            "vip_ok": str(bool(setup.get("vip_ok", False))),
            "vip_plus_ok": str(bool(setup.get("vip_plus_ok", False))),
            "vip_layers": json.dumps(setup.get("vip_layer_lines", [])),
            "vip_plus_layers": json.dumps(setup.get("vip_plus_layer_lines", [])),
        }
        await self.redis.hset(f"signal_layers:{signal_id}", mapping=payload)
        await self.redis.expire(f"signal_layers:{signal_id}", 60 * 60 * 24)

    async def fetch_signals_for_day(self, day_utc: str) -> List[tuple]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
                    exchange, symbol, side, entry_price, status
                FROM signals
                WHERE (timestamp AT TIME ZONE 'UTC')::date = $1::date
                ORDER BY timestamp ASC
                """,
                day_utc,
            )
        return [
            (r["timestamp"], r["exchange"], r["symbol"], r["side"], float(r["entry_price"]), r["status"])
            for r in rows
        ]

    async def fetch_recent_closed_statuses(self, limit: int = 40) -> List[str]:
        assert self.pool is not None
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

        self.scan_interval_seconds = 120
        self.max_symbols = 35
        self.alert_cooldown_seconds = 60 * 60
        self.last_alert_at: Dict[str, float] = {}
        # Keep local SQLite only for adaptive learning state.
        self.db_path = "signals_log.db"
        self.db_executor = ThreadPoolExecutor(max_workers=1)

        self.last_daily_report_date = datetime.now(UTC).date()
        self.il_tz = ZoneInfo("Asia/Jerusalem")
        self.last_daily_report_sent_date_il: Optional[str] = None
        self.min_score_threshold = 6
        self.volume_spike_threshold = 2.0
        self.vip_signal_count = 0
        self.chat_signal_count = 0
        self.macro_cache: Dict[str, dict] = {}
        self.macro_cache_ttl_sec = 300
        self.sync_cache: Dict[str, dict] = {}
        self.sync_cache_ttl_sec = 90
        self.ext_cache: Dict[str, dict] = {}
        self.ext_cache_ttl_sec = 120
        self.vip_strict_mode = (os.getenv("VIP_STRICT_MODE", "true").strip().lower() != "false")
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
        }

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
        self.chat_id = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()
        self.vip_plus_chat_id = os.getenv("VIP_PLUS_CHAT_ID", "").strip()
        if self.token and self.chat_id:
            print(f"[TELEGRAM] credentials loaded (chat_id={self.chat_id})")
        else:
            print("[TELEGRAM] missing TELEGRAM_BOT_TOKEN/TELEGRAM_TOKEN or TELEGRAM_CHAT_ID/CHAT_ID in .env")
        if self.vip_plus_chat_id:
            print(f"[TELEGRAM] VIP+ channel loaded (vip_chat_id={self.vip_plus_chat_id})")

        self.trade_manager = TradeManager()
        recovered = self._run_async_task(self.trade_manager.startup())
        print(f"[TRADE-MANAGER] connected | recovered active trades: {len(recovered)}")

        self.init_db()
        self.load_learning_state()
        self.status_thread = threading.Thread(target=self.status_watcher_loop, daemon=True)
        self.status_thread.start()
        self.daily_report_thread = threading.Thread(target=self.daily_report_loop, daemon=True)
        self.daily_report_thread.start()

    # -------------------------
    # 0) Local Learning-State (SQLite)
    # -------------------------
    @staticmethod
    def _run_loop_forever(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def init_db(self) -> None:
        def _create() -> None:
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
        print(f"[LEARNING-DB] ready at {self.db_path}")

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
        print(
            f"[LEARNING] loaded thresholds | min_score={self.min_score_threshold} | rel_vol={self.volume_spike_threshold:.2f}x"
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
        print(f"[DB] saved signal id={signal_id} {exchange_name} {symbol} {setup['side']}")
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
            except Exception:
                continue

            new_status = None
            if side == "LONG":
                if current_price <= float(stop_loss):
                    new_status = "Hit SL"
                elif current_price >= float(tp1) or current_price >= float(tp2) or current_price >= float(tp3):
                    new_status = "Hit TP"
            else:
                if current_price >= float(stop_loss):
                    new_status = "Hit SL"
                elif current_price <= float(tp1) or current_price <= float(tp2) or current_price <= float(tp3):
                    new_status = "Hit TP"

            if not new_status:
                continue

            self._run_async_task(
                self.trade_manager.update_signal_status(
                    signal_id=int(signal_id),
                    status=new_status,
                    last_price=float(current_price),
                )
            )
            print(f"[DB] updated signal id={signal_id} -> {new_status}")

    def adaptive_learning_step(self) -> None:
        """Self-tune thresholds based on recent closed signal performance."""
        statuses = self._run_async_task(self.trade_manager.fetch_recent_closed_statuses(limit=40))
        if len(statuses) < 10:
            return

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
                f"[LEARNING] tuned thresholds | win_rate={win_rate:.2%} | min_score {old_score}->{self.min_score_threshold} | rel_vol {old_vol:.2f}->{self.volume_spike_threshold:.2f}"
            )

    def status_watcher_loop(self) -> None:
        while True:
            try:
                self.update_pending_signal_statuses()
                self.adaptive_learning_step()
            except Exception as exc:
                print(f"[DB] status watcher error: {exc}")
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

        rows = self.fetch_signals_for_day(day_utc)
        if not rows:
            print(f"[REPORT] no signals found for {day_utc}")
            return

        entries: List[str] = []
        for timestamp, exchange_name, symbol, side, entry_price, status in rows:
            result = self.evaluate_signal_result(
                exchange_name=exchange_name,
                symbol=symbol,
                side=side,
                entry_price=float(entry_price),
                status=status,
            )
            if result is None:
                entries.append(f"{symbol} ⚪ | {float(entry_price):.3f} | N/A")
                continue

            move_pct, verdict = result
            entries.append(f"{symbol} {verdict} | {float(entry_price):.3f} | {side} {move_pct:+.2f}%")

        # Add right-side padding inside the pre block so Telegram's copy icon
        # does not visually overlap the end of the content lines.
        content_width = max((len(x) for x in entries), default=0)
        padded_entries = [line.ljust(content_width + 8) for line in entries]
        report_body = "\n".join(padded_entries)
        report_text = (
            "<b>🚀 Daily Performance Report</b>\n"
            f"<i>{day_utc}</i>\n"
            f"<pre>{report_body}</pre>"
        )
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        target_chats = []
        if self.chat_id:
            target_chats.append(self.chat_id)
        if self.vip_plus_chat_id and self.vip_plus_chat_id not in target_chats:
            target_chats.append(self.vip_plus_chat_id)

        if not target_chats:
            print("[REPORT] skipped: no target chat ids configured")
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
                today_il_str = now_il.strftime("%Y-%m-%d")
                if (
                    (now_il.hour > 22 or (now_il.hour == 22 and now_il.minute >= 30))
                    and self.last_daily_report_sent_date_il != today_il_str
                ):
                    self.send_daily_performance_report(today_il_str)
                    self.last_daily_report_sent_date_il = today_il_str
            except Exception as exc:
                print(f"[REPORT] loop error: {exc}")
            time.sleep(60)

    # -------------------------
    # 1) High-Speed Data Engine
    # -------------------------
    def top_usdt_symbols(self, exchange_name: str, ex: ccxt.Exchange) -> List[str]:
        try:
            tickers = ex.fetch_tickers()
            ranked = []
            for symbol, info in tickers.items():
                if "/USDT" not in symbol:
                    continue
                if ":" in symbol:
                    continue
                quote_volume = info.get("quoteVolume") or 0
                if quote_volume and quote_volume > 0:
                    ranked.append((symbol, float(quote_volume)))
            ranked.sort(key=lambda x: x[1], reverse=True)
            symbols = [s for s, _ in ranked[: self.max_symbols]]
            print(f"[{exchange_name}] tracking {len(symbols)} high-volume USDT pairs")
            return symbols
        except Exception as exc:
            print(f"[{exchange_name}] failed to fetch top symbols: {exc}")
            return []

    def fetch_multi_timeframes(self, ex: ccxt.Exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
        timeframes = {"4h": 220, "1h": 220, "15m": 220, "5m": 220}
        out: Dict[str, pd.DataFrame] = {}

        def _retry_ohlcv(tf: str, lim: int, attempts: int = 3) -> Optional[List[list]]:
            for idx in range(attempts):
                try:
                    return ex.fetch_ohlcv(symbol, timeframe=tf, limit=lim)
                except Exception:
                    if idx == attempts - 1:
                        return None
                    time.sleep(0.4 * (idx + 1))
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

    # -------------------------
    # 2) The Intelligence Core
    # -------------------------
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        data = df.copy()
        # pandas-ta VWAP expects a DatetimeIndex
        if "ts" in data.columns:
            data = data.sort_values("ts").set_index("ts", drop=False)
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
            l = float(df_15m.iloc[i]["low"])
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
                    return (l, h)
            else:
                bullish_base = c > o
                downside_bos = next_close < prev_low and next2_low < l
                if bullish_base and downside_bos:
                    return (l, h)

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
                eth_last = eth.iloc[-1]
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
            except Exception:
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
            except Exception:
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
            print(f"[L7-TEST] exchange unavailable: {exchange_name}")
            return
        symbols = self.top_usdt_symbols(exchange_name, ex)[:top_n]
        print(f"[L7-TEST] running on {exchange_name} top {len(symbols)} symbols")
        for symbol in symbols:
            try:
                rows = ex.fetch_ohlcv(symbol, timeframe="5m", limit=80)
                if not rows:
                    print(f"[L7-TEST] {symbol} FAIL | no ohlcv")
                    continue
                d5 = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
                d5["ts"] = pd.to_datetime(d5["ts"], unit="ms", utc=True)
                current_price = float(d5.iloc[-1]["close"])
                atr = float(self.add_indicators(d5).iloc[-1]["atr"] or 0.0)
                if atr <= 0:
                    print(f"[L7-TEST] {symbol} FAIL | ATR unavailable")
                    continue
                entry = current_price
                side = "LONG"
                sl = entry - (1.5 * atr)
                layer6 = self._run_async_task(self._layer6_order_book_multi(symbol, side, entry, sl, current_price))
                layer7 = self._run_async_task(self._layer7_whale_flow(symbol, side, layer6))
                status = "PASS" if layer7.get("ok") else "FAIL"
                print(
                    f"[L7-TEST] {symbol} {status} | L6={layer6.get('text')} | L7={layer7.get('text')}"
                )
            except Exception as exc:
                print(f"[L7-TEST] {symbol} FAIL | error={exc}")

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

    def run_vip_plus_layer_test(self, symbols: List[str], exchange_name: str = "Binance") -> None:
        ex = self.exchanges.get(exchange_name)
        if not ex:
            print(f"[VIP+-TEST] exchange unavailable: {exchange_name}")
            return
        for symbol in symbols:
            print(f"\n[VIP+-TEST] {symbol} ({exchange_name})")
            try:
                dfs = self.fetch_multi_timeframes(ex, symbol)
                if not dfs:
                    print("[VIP+-TEST] FAIL | data fetch failed")
                    continue
                d4h = self.add_indicators(dfs["4h"])
                d1h = self.add_indicators(dfs["1h"])
                d15 = self.add_indicators(dfs["15m"])
                d5 = self.add_indicators(dfs["5m"])
                if d4h.iloc[-1].isna().any() or d1h.iloc[-1].isna().any() or d15.iloc[-1].isna().any() or d5.iloc[-1].isna().any():
                    print("[VIP+-TEST] FAIL | indicator NaN")
                    continue
                price = float(d5.iloc[-1]["close"])
                trend_up = float(d4h.iloc[-1]["close"]) > float(d4h.iloc[-1]["ema200"])
                trend_down = float(d4h.iloc[-1]["close"]) < float(d4h.iloc[-1]["ema200"])
                bos_up = float(d15.iloc[-1]["close"]) > max(float(d15.iloc[-2]["high"]), float(d15.iloc[-3]["high"]))
                bos_down = float(d15.iloc[-1]["close"]) < min(float(d15.iloc[-2]["low"]), float(d15.iloc[-3]["low"]))
                side = "LONG" if (trend_up or bos_up) else "SHORT"
                if trend_down and bos_down:
                    side = "SHORT"
                demand_zone = self.find_last_order_block_before_bos(d15, "LONG")
                supply_zone = self.find_last_order_block_before_bos(d15, "SHORT")
                in_demand_zone = bool(demand_zone and demand_zone[0] <= price <= demand_zone[1] * 1.002)
                in_supply_zone = bool(supply_zone and supply_zone[0] * 0.998 <= price <= supply_zone[1])
                long_fvg = self.find_latest_fvg(d15, "LONG")
                short_fvg = self.find_latest_fvg(d15, "SHORT")
                long_fvg_eq_retested = bool(long_fvg and long_fvg[0] <= price <= long_fvg[1] and price <= long_fvg[2])
                short_fvg_eq_retested = bool(short_fvg and short_fvg[0] <= price <= short_fvg[1] and price >= short_fvg[2])
                avg_volume = float(d5.iloc[-1]["vol_sma20"]) if float(d5.iloc[-1]["vol_sma20"]) > 0 else 0.0
                current_volume = float(d5.iloc[-1]["volume"])
                volume_spike = avg_volume > 0 and current_volume > (avg_volume * 1.5)
                atr = float(d5.iloc[-1]["atr"])
                if atr <= 0:
                    print("[VIP+-TEST] FAIL | ATR unavailable")
                    continue
                entry = price
                sl = entry - (1.5 * atr) if side == "LONG" else entry + (1.5 * atr)
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
                lines = layered.get("vip_plus_layer_lines", [])
                for ln in lines:
                    print(f"  {ln}")
                if layered.get("vip_plus_ok"):
                    print("  => VIP+ PASS (all layers 1-8)")
                else:
                    fail_line = next((ln for ln in lines if "FAIL" in ln), "Unknown fail layer")
                    print(f"  => VIP+ REJECTED at: {fail_line}")
            except Exception as exc:
                print(f"[VIP+-TEST] FAIL | error={repr(exc)}")

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
        return future.result()

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
                except Exception:
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

        # Layer 4 hard volume gate: current volume > 150% of volume SMA20.
        avg_volume = float(last_5["vol_sma20"]) if float(last_5["vol_sma20"]) > 0 else 0.0
        current_volume = float(last_5["volume"])
        volume_spike = avg_volume > 0 and current_volume > (avg_volume * 1.5)

        # Layer 3 trigger components
        long_osc = (last_15["rsi"] < 30) and (last_15["stoch_k"] > last_15["stoch_d"]) and (last_15["stoch_k"] < 25)
        short_osc = (last_15["rsi"] > 70) and (last_15["stoch_k"] < last_15["stoch_d"]) and (last_15["stoch_k"] > 75)
        rsi_recover_long = (float(prev_15["rsi"]) < 30) and (float(last_15["rsi"]) > float(prev_15["rsi"]))
        rsi_recover_short = (float(prev_15["rsi"]) > 70) and (float(last_15["rsi"]) < float(prev_15["rsi"]))
        at_support = price <= pivots["s1"] * 1.005
        at_resistance = price >= pivots["r1"] * 0.995
        macd_cross_up = (prev_5["macd"] <= prev_5["macd_signal"]) and (last_5["macd"] > last_5["macd_signal"])
        macd_cross_down = (prev_5["macd"] >= prev_5["macd_signal"]) and (last_5["macd"] < last_5["macd_signal"])
        vwap_long = float(last_15["close"]) > float(last_15["vwap"]) and float(last_1h["close"]) > float(last_1h["vwap"])
        vwap_short = float(last_15["close"]) < float(last_15["vwap"]) and float(last_1h["close"]) < float(last_1h["vwap"])

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
        if rr < 2.5:
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
        if side == "LONG":
            # Block longs in dollar bullish regime and risk-off stablecoin flow.
            if macro["dxy_rsi_1h"] > 60:
                return None
            if not macro["usdt_dominance_risk_on"]:
                return None
            if not macro["sector_strength_ok"]:
                return None

        # Beta filter and score boost.
        beta = macro["beta_vs_btc"]
        btc_change = macro["btc_change_pct"]
        asset_change = macro["asset_change_pct"]
        if side == "LONG":
            # Negative behavior in corrections: reject if asset drops notably more than BTC.
            if btc_change < 0 and asset_change < (btc_change * 1.2):
                return None
            if macro["relative_strength"]:
                score_1_10 = min(10, score_1_10 + 1)
        else:
            # For shorts, avoid names that are too positively resilient while BTC weakens.
            if btc_change < 0 and asset_change > btc_change * 0.5:
                return None

        # -------------------------
        # VIP / VIP+ Layered Gate (strict institutional filter stack)
        # -------------------------
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
        if self.vip_strict_mode and not layered.get("vip_ok", False):
            print(f"[LAYER-GATE] {symbol} rejected VIP stack | " + " | ".join(layered.get("vip_layer_lines", [])))
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
            "liquidity_status": liquidity_status,
            "liquidity_confirmation": liquidity_confirmation,
            "order_book_depth": order_book_depth,
            "macro_dxy_status": macro["dxy_status_text"],
            "macro_sector_status": macro["sector_status_text"],
            "macro_capital_flow": macro["capital_flow_text"],
            "sync_mtf_status": sync.get("mtf_text", "N/A"),
            "sync_exchange_status": sync.get("exchange_text", "N/A"),
            "sync_oi_status": sync.get("oi_text", "N/A"),
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
            except Exception:
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
        asset_rows = ex.fetch_ohlcv(symbol, timeframe="15m", limit=120)
        btc_rows = ex.fetch_ohlcv("BTC/USDT", timeframe="15m", limit=120)
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

    def build_chart_image_bytes(self, exchange_name: str, symbol: str, setup: Optional[dict] = None) -> Optional[bytes]:
        """Build a black-background candlestick chart image from live OHLCV data."""
        ex = self.exchanges.get(exchange_name)
        if not ex:
            return None
        try:
            rows = ex.fetch_ohlcv(symbol, timeframe="15m", limit=48)
            if not rows:
                return None

            ohlc = [
                {
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
                l = candle["low"]
                c = candle["close"]
                color = bull_color if c >= o else bear_color
                ax.plot([i, i], [l, h], color=color, linewidth=1.25, solid_capstyle="round")
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

            ax.set_title(f"{symbol} • 15m Candles", color="#e5e7eb", fontsize=14, fontweight="bold")
            ax.tick_params(axis="x", colors="#9ca3af", labelsize=8)
            ax.tick_params(axis="y", colors="#d1d5db", labelsize=9)
            for spine in ax.spines.values():
                spine.set_color("#374151")
            ax.grid(color="#1f2937", linestyle="--", linewidth=0.5, alpha=0.6)
            ax.set_xlim(-1, len(ohlc))
            ax.set_xticks([])

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

            # Liquidity sweep marker: long wick extremes near the edge.
            sweep_idx = max(range(len(ohlc) - 12, len(ohlc)), key=lambda i: abs(ohlc[i]["high"] - ohlc[i]["low"]))
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

            fig.tight_layout()

            buffer = io.BytesIO()
            fig.savefig(buffer, format="png", dpi=150, facecolor=fig.get_facecolor())
            plt.close(fig)
            buffer.seek(0)
            return buffer.getvalue()
        except Exception:
            return None

    def send_telegram(self, exchange_name: str, symbol: str, setup: dict) -> None:
        if not self.token:
            print(f"[TELEGRAM] SKIPPED (missing credentials) | {exchange_name} {symbol}")
            return

        key = f"{exchange_name}:{symbol}:{setup['side']}"
        now = time.time()
        if key in self.last_alert_at and (now - self.last_alert_at[key]) < self.alert_cooldown_seconds:
            print(f"[ANTI-SPAM] skipped {key} (cooldown 60m)")
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            self._refresh_daily_flow_state()
            side_caps = "LONG" if setup["side"] == "LONG" else "SHORT"
            side_emoji = "📈" if setup["side"] == "LONG" else "📉"
            stop_pct = abs((setup["entry"] - setup["sl"]) / max(setup["entry"], 1e-9)) * 100
            signal_strength = max(80, min(99, int(setup["score"] * 10)))
            rr_text = f"1:{setup['rr']:.2f}"
            rel_vol = float(setup["rel_volume"])
            adx = float(setup["adx"])
            rsi = float(setup["rsi"])
            rsi_zone = (
                "oversold zone (rebound potential)"
                if rsi < 30
                else "overbought zone (pullback potential)"
                if rsi > 70
                else "neutral momentum zone"
            )
            trend_strength = (
                "strong directional trend" if adx >= 25 else "weak / sideways trend"
            )
            volume_impact = (
                "volume confirms participation and improves follow-through odds"
                if rel_vol >= 1.0
                else "volume is soft, so follow-through quality can be weaker"
            )
            short_liquidity = (
                "Strong"
                if rel_vol >= 1.8 and abs(rsi - 50) >= 5
                else "Moderate"
                if rel_vol >= 1.2
                else "Weak"
            )
            medium_liquidity = (
                "Strong" if adx >= 25 and rel_vol >= 1.3 else "Moderate" if adx >= 20 else "Weak"
            )
            long_liquidity = (
                "Strong"
                if ("High Volume Confirmation" in setup.get("liquidity_status", "")) and adx >= 25
                else "Moderate"
                if adx >= 20
                else "Weak"
            )
            technical_support = (
                "Trend direction and volume support continuation."
                if side_caps == "LONG"
                else "Trend pressure and momentum support downside continuation."
            )
            technical_risk = (
                "Counter-trend spikes and sudden volume fade can invalidate setup."
                if side_caps == "LONG"
                else "Short squeeze bursts and volume reversal can invalidate setup."
            )
            # Keep technical text concise so final sections (especially Volume Snapshot)
            # always remain visible within Telegram caption limits.
            technical_support = technical_support[:90]
            technical_risk = technical_risk[:90]
            driver_text = str(setup["breakdown"])[:85]
            volume_state = "above average" if rel_vol >= 1.0 else "below average"
            vip_layer_block = "\n".join(setup.get("vip_layer_lines", [])[:5]) or "Layer diagnostics unavailable"
            vip_plus_layer_block = "\n".join(setup.get("vip_plus_layer_lines", [])[:8]) or vip_layer_block
            chat_base_caption = (
                "FORTRESS VIP SIGNAL\n"
                f"Symbol: {symbol}\n"
                f"Side: {side_caps}\n"
                f"Entry: ${setup['entry']:,.2f}\n"
                f"TP1: ${setup['tp1']:,.2f}\n"
                f"TP2: ${setup['tp2']:,.2f}\n"
                f"TP3: ${setup['tp3']:,.2f}\n"
                f"SL: ${setup['sl']:,.2f}"
            )
            technical_short = (
                "Liquidity sweep + Whale wall confirmed"
                if ("PASS" in " ".join(setup.get("vip_plus_layer_lines", [])) and "L6_OrderBookDepth: PASS" in " ".join(setup.get("vip_plus_layer_lines", [])))
                else "Momentum + structure alignment"
            )
            cmc_meta = self._fetch_cmc_snapshot(symbol)
            vip_base_caption = (
                "⚡️🚨 <b>ELITE SIGNAL</b> 🚨⚡️\n\n"
                f"{symbol}\n"
                f"Position: {side_caps} {side_emoji}\n"
                "Leverage: x10\n"
                "━━━━━━━━━━━━━━\n\n"
                f"📥 Entry: ${setup['entry']:,.2f}\n"
                f"🛑 Stop Loss: ${setup['sl']:,.2f} ({stop_pct:.2f}%)\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "Targets\n"
                f"🎯 T1: ${setup['tp1']:,.2f}\n"
                f"🎯 T2: ${setup['tp2']:,.2f}\n"
                f"🎯 T3: ${setup['tp3']:,.2f}"
            )
            vip_caption = (
                f"{vip_base_caption}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "       ✨ <b>VIP+ EXTENSIONS</b> ✨\n"
                f"       🔥 <b>Confidence Level:</b> {signal_strength}% 🔥\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "📊 <b>Technical Analysis</b>\n"
                f"Context: this setup triggers because momentum, trend and volume align for a {side_caps} continuation.\n"
                f"Support: {technical_support}\n"
                f"Risk: {technical_risk}\n"
                f"Indicators: RSI {rsi:.2f} ({rsi_zone}) | ADX {adx:.2f} ({trend_strength}) | Rel.Volume {rel_vol:.2f}x\n"
                f"Indicator impact: {volume_impact}. ADX and RSI together shape timing quality and trend confidence.\n"
                f"Driver: {driver_text}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "💧 <b>Liquidity (Multi-Horizon)</b>\n"
                f"Short-Term: {short_liquidity}\n"
                f"Mid-Term: {medium_liquidity}\n"
                f"Long-Term: {long_liquidity}\n"
                f"Status: {setup.get('liquidity_status', 'Liquidity Neutral')}\n"
                f"💧 <b>Liquidity Confirmation:</b> {setup.get('liquidity_confirmation', 'No Liquidity Confirmation')}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "⚖️ <b>Risk / Reward</b>\n"
                f"Ratio: {rr_text}\n"
                f"Risk to SL: {stop_pct:.2f}%\n"
                f"TP Ladder: T1 ${setup['tp1']:,.2f} | T2 ${setup['tp2']:,.2f} | T3 ${setup['tp3']:,.2f}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "📈 <b>Volume Snapshot</b>\n"
                f"At alert time: {rel_vol:.2f}x ({volume_state})\n"
                f"🧱 <b>Order Book Depth:</b> {setup.get('order_book_depth', 'N/A')}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "🌐 <b>Macro Environment</b>\n"
                f"DXY Correlation: {setup.get('macro_dxy_status', 'N/A')}\n"
                f"Sector Strength: {setup.get('macro_sector_status', 'N/A')}\n"
                f"Capital Flow: {setup.get('macro_capital_flow', 'N/A')}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "🔄 <b>Synchronicity Check</b>\n"
                f"MTF Alignment: {setup.get('sync_mtf_status', 'N/A')}\n"
                f"Exchange Sync: {setup.get('sync_exchange_status', 'N/A')}\n"
                f"OI Flow: {setup.get('sync_oi_status', 'N/A')}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "🧪 <b>VIP+ Layer Audit (1-8)</b>\n"
                f"{vip_plus_layer_block}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "🌍 <b>Social Pulse</b>\n"
                f"Galaxy Score: {float(setup.get('social_galaxy_score', 0.0)):.1f}\n"
                f"AltRank: {int(setup.get('social_alt_rank', 0))}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "🧠 <b>Technical Edge</b>\n"
                f"{technical_short}\n\n"
                "━━━━━━━━━━━━━━\n\n"
                "🏛️ <b>CMC Market Context (Advisory)</b>\n"
                f"{cmc_meta.get('text', 'CMC unavailable')}"
            )
            vip_chart_image_bytes = self.build_chart_image_bytes(exchange_name, symbol, setup=setup)

            def _send(chat_id: str, caption: str, include_live_button: bool = False, force_text: bool = False) -> bool:
                live_symbol = symbol.replace("/", "")
                live_url = (
                    f"https://www.binance.com/en/trade/{live_symbol}"
                    if exchange_name.lower() == "binance"
                    else f"https://www.bybit.com/trade/usdt/{live_symbol}"
                )
                max_caption_len = 1000
                if len(caption) > max_caption_len:
                    # Keep both Risk/Reward and Volume Snapshot visible by trimming
                    # earlier sections first (mainly technical analysis details).
                    tail_anchor = "\n\n━━━━━━━━━━━━━━\n\n⚖️ <b>Risk / Reward</b>"
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
                    data_payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
                    if reply_markup:
                        data_payload["reply_markup"] = json.dumps(reply_markup)
                    resp = requests.post(
                        f"https://api.telegram.org/bot{self.token}/sendPhoto",
                        data=data_payload,
                        files={"photo": ("fortress_chart.png", vip_chart_image_bytes, "image/png")},
                        timeout=30,
                    )
                else:
                    json_payload = {"chat_id": chat_id, "text": caption}
                    if reply_markup:
                        json_payload["reply_markup"] = reply_markup
                    resp = requests.post(
                        url,
                        json=json_payload,
                        timeout=20,
                    )
                if not resp.ok:
                    print(f"[TELEGRAM] send failed ({resp.status_code}) | chat={chat_id} | {exchange_name} {symbol} | {resp.text}")
                    return False
                return True

            sent_any = False

            vip_plus_quality = bool(setup.get("vip_plus_ok", False))
            vip_plus_soft = bool(setup.get("vip_plus_soft_ok", False))
            vip_quality = bool(setup.get("vip_ok", False))
            vip_soft = bool(setup.get("vip_soft_ok", False))

            # Dynamic daily target windows:
            # chat: 10-20/day, vip+: 5-10/day
            vip_plus_sent_today = int(self.daily_flow.get("vip_plus_sent", 0))
            chat_sent_today = int(self.daily_flow.get("chat_sent", 0))

            allow_vip_plus = False
            if vip_plus_sent_today < 5:
                allow_vip_plus = vip_plus_soft
            elif vip_plus_sent_today < 10:
                allow_vip_plus = vip_plus_quality
            else:
                allow_vip_plus = vip_plus_quality and int(setup.get("score", 0)) >= 9

            allow_vip_chat = False
            if chat_sent_today < 10:
                allow_vip_chat = vip_soft
            elif chat_sent_today < 20:
                allow_vip_chat = vip_quality
            else:
                allow_vip_chat = vip_quality and int(setup.get("score", 0)) >= 9

            # VIP+ gets only 99%-tier confirmations (layers 1-8 all pass).
            if self.vip_plus_chat_id and allow_vip_plus:
                if _send(self.vip_plus_chat_id, vip_caption, include_live_button=True):
                    self.vip_signal_count += 1
                    self.daily_flow["vip_plus_sent"] = vip_plus_sent_today + 1
                    sent_any = True

            # Regular VIP gets text-only fast signal (never chart).
            if self.chat_id and allow_vip_chat:
                if _send(self.chat_id, chat_base_caption, force_text=True):
                    self.chat_signal_count += 1
                    self.daily_flow["chat_sent"] = chat_sent_today + 1
                    sent_any = True

            if sent_any:
                self.last_alert_at[key] = now
                print(
                    f"[TELEGRAM] SENT routed signal | {exchange_name} {symbol} | score={setup['score']}/10 | vip={self.vip_signal_count} chat={self.chat_signal_count}"
                )
            else:
                print(
                    f"[TELEGRAM] routed signal skipped (no successful sends) | {exchange_name} {symbol}"
                )
        except Exception as exc:
            print(f"[TELEGRAM] ERROR while sending | {exchange_name} {symbol} | {exc}")

    # -------------------------
    # 5) Technical Optimization + DB integration
    # -------------------------
    def process_symbol(self, exchange_name: str, ex: ccxt.Exchange, symbol: str) -> None:
        data = self.fetch_multi_timeframes(ex, symbol)
        if not data:
            return
        setup = self.detect_setup(ex, symbol, data)
        if not setup:
            return
        # Adaptive floor: when daily flow is below target, allow slightly lower score floor.
        self._refresh_daily_flow_state()
        adaptive_floor = self.min_score_threshold
        if int(self.daily_flow.get("chat_sent", 0)) < 10:
            adaptive_floor = max(5, self.min_score_threshold - 1)
        if int(setup["score"]) < adaptive_floor:
            return

        if self.has_recent_signal(symbol, minutes=30):
            print(f"[DB-MEMORY] skipped {symbol} (signal exists in last 30m)")
            return

        signal_id = self.save_signal(exchange_name, symbol, setup)
        self._run_async_task(self.trade_manager.cache_layer_diagnostics(signal_id, setup))
        self.send_telegram(exchange_name, symbol, setup)

    def _refresh_daily_flow_state(self) -> None:
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        if self.daily_flow.get("date") != today:
            self.daily_flow = {"date": today, "chat_sent": 0, "vip_plus_sent": 0}

    def scan_exchange(self, exchange_name: str, ex: ccxt.Exchange) -> None:
        symbols = self.top_usdt_symbols(exchange_name, ex)
        if not symbols:
            return
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(self.process_symbol, exchange_name, ex, symbol) for symbol in symbols]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as exc:
                    print(f"[{exchange_name}] symbol worker failed: {exc}")

    def run_cycle(self) -> None:
        print("\n=== Fortress Scanner v1.2 cycle started ===")
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(self.scan_exchange, name, ex) for name, ex in self.exchanges.items()]
            for job in as_completed(jobs):
                try:
                    job.result()
                except Exception as exc:
                    print(f"[ENGINE] exchange worker failed: {exc}")
        print("=== Fortress Scanner cycle completed ===")

    def run_forever(self) -> None:
        while True:
            try:
                self.run_cycle()
            except Exception as exc:
                print(f"[FATAL] cycle error: {exc}")
            print(f"Sleeping {self.scan_interval_seconds}s...\n")
            time.sleep(self.scan_interval_seconds)


if __name__ == "__main__":
    scanner = FortressScanner()
    scanner.run_forever()