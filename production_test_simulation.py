import argparse
import asyncio
import io
import os
from datetime import UTC, datetime
from typing import Any, Dict
from urllib.parse import parse_qs, unquote, urlparse

import asyncpg
import matplotlib.pyplot as plt
import requests
from dotenv import load_dotenv


def _urlparse_database_url(db_url: str) -> Any:
    s = db_url.strip()
    if s.startswith("postgres://"):
        s = "postgresql://" + s[len("postgres://") :]
    return urlparse(s)


def _asyncpg_ssl_kwarg(parsed: Any) -> Any:
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
    parsed = _urlparse_database_url(db_url)
    password = (pg_password_env or "").strip() or unquote(parsed.password or "")
    if not password:
        raise RuntimeError(
            "Postgres: DATABASE_URL username is placeholder 'user'. Set POSTGRES_PASSWORD "
            "or fix DATABASE_URL with the correct user."
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


def mdv2_code(value: str) -> str:
    text = str(value).replace("\\", "\\\\").replace("`", "\\`")
    return f"`{text}`"


def esc_mdv2(value: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    out = []
    for ch in str(value):
        if ch in escape_chars:
            out.append("\\")
        out.append(ch)
    return "".join(out)


def format_elapsed(ts: datetime, now: datetime | None = None) -> str:
    ref = now or datetime.now(UTC)
    sec = max(0, int((ref - ts).total_seconds()))
    days = sec // 86400
    hours = (sec % 86400) // 3600
    minutes = (sec % 3600) // 60
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


async def open_db() -> asyncpg.Connection:
    db_url = (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip()
    pg_user = os.getenv("POSTGRES_USER", "falcon_admin")
    pg_password = (os.getenv("POSTGRES_PASSWORD") or "").strip()
    if db_url:
        parsed = _urlparse_database_url(db_url)
        url_user = unquote(parsed.username or "")
        if url_user == "user" and pg_user != "user":
            kw = _pg_connect_kwargs_placeholder_user_dsn(db_url, pg_user, pg_password)
            return await asyncpg.connect(**kw)
        return await asyncpg.connect(dsn=db_url)
    password = pg_password
    if not password:
        raise RuntimeError(
            "Set DATABASE_URL/DB_URL or POSTGRES_PASSWORD when using discrete POSTGRES_*."
        )
    return await asyncpg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=pg_user,
        password=password,
        database=os.getenv("POSTGRES_DB", "trading_db"),
    )


def send_message(token: str, payload: dict) -> requests.Response:
    return requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=payload,
        timeout=20,
    )


def build_test_chart_image(symbol: str) -> bytes:
    x_vals = list(range(20))
    y_vals = [
        68200, 68250, 68320, 68410, 68380, 68490, 68530, 68620, 68580, 68690,
        68740, 68810, 68760, 68890, 68980, 69060, 69120, 69240, 69310, 69400,
    ]
    fig, ax = plt.subplots(figsize=(8, 4), dpi=170)
    ax.plot(x_vals, y_vals, linewidth=2.2, color="#2E86DE")
    ax.fill_between(x_vals, y_vals, min(y_vals) - 120, alpha=0.15, color="#2E86DE")
    ax.set_title(f"{symbol} 1H Simulation Chart")
    ax.set_xlabel("Recent candles")
    ax.set_ylabel("Price")
    ax.grid(alpha=0.25)
    ax.text(
        x_vals[-1],
        y_vals[-1],
        f"  {y_vals[-1]:,.0f}",
        fontsize=9,
        va="center",
        color="#1B4F72",
    )
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


def send_photo(token: str, chat_id: str, caption: str, image_bytes: bytes, parse_mode: str = "MarkdownV2") -> requests.Response:
    return requests.post(
        f"https://api.telegram.org/bot{token}/sendPhoto",
        data={"chat_id": chat_id, "caption": caption, "parse_mode": parse_mode},
        files={"photo": ("chart.png", image_bytes, "image/png")},
        timeout=30,
    )


async def main(keep_row: bool) -> int:
    load_dotenv(dotenv_path=".env")

    token = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN") or "").strip()
    vip_chat_id = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()
    vip_plus_chat_id = (os.getenv("VIP_PLUS_CHAT_ID") or "").strip()
    if not token:
        print("[FAIL] Missing TELEGRAM_BOT_TOKEN/TELEGRAM_TOKEN")
        return 1
    if not vip_chat_id or not vip_plus_chat_id:
        print("[FAIL] Missing TELEGRAM_CHAT_ID/CHAT_ID or VIP_PLUS_CHAT_ID")
        return 1

    conn = await open_db()
    inserted_id = None
    try:
        print("[0/5] Ensuring DB schema for simulation...")
        await conn.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS original_message_id BIGINT")
        await conn.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS original_chat_id TEXT")
        await conn.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS chat_id TEXT")
        await conn.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS last_target_hit INTEGER NOT NULL DEFAULT 0")
        print("  -> schema check/migration complete")

        print("[1/5] Creating fake trade row in DB...")
        inserted = await conn.fetchrow(
            """
            INSERT INTO signals (
                exchange, symbol, side, entry_price, stop_loss, tp1, tp2, tp3, score, status, last_target_hit
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            RETURNING id, timestamp
            """,
            "SIMULATION",
            "TESTBTC/USDT",
            "LONG",
            68450.50,
            67800.00,
            69200.00,
            69850.00,
            70400.00,
            9,
            "Pending",
            0,
        )
        inserted_id = int(inserted["id"])
        signal_ts = inserted["timestamp"]
        print(f"  -> created signal id={inserted_id} timestamp={signal_ts}")

        print("[2/5] Sending fake signals with fixed VIP/VIP+ formats...")
        vip_plus_signal_text = (
            "⚡️🚨 *ELITE SIGNAL* 🚨⚡️\n\n"
            f"*{mdv2_code('TESTBTC/USDT')}*\n"
            f"Position: *{mdv2_code('LONG')}* 📈\n"
            f"Leverage: *{mdv2_code('x10')}*\n"
            "━━━━━━━━━━━━━━\n"
            f"📥 *Entry:* {mdv2_code('$68,450.50')}\n"
            f"🛑 *Stop Loss:* {mdv2_code('$67,800.00')} \\({mdv2_code('0.95%')}\\)\n"
            "━━━━━━━━━━━━━━\n"
            "*Targets*\n"
            f"🎯 T1: {mdv2_code('$69,200.00')}\n"
            f"🎯 T2: {mdv2_code('$69,850.00')}\n"
            f"🎯 T3: {mdv2_code('$70,400.00')}\n"
            "━━━━━━━━━━━━━━\n"
            "📊 *Technical Analysis*\n"
            f"🔹 *Context:* {mdv2_code('Bullish continuation structure after 4H compression break')}\n"
            f"🔹 *Support/Resistance:* {mdv2_code('EMA20/50 aligned below price; local ceiling near 69,850 now acting as pivot')}\n"
            f"🔹 *Indicators:* {mdv2_code('RSI 63 rising, MACD positive expansion, volume confirms breakout participation')}\n"
            f"🔹 *Risk:* {mdv2_code('Invalidation on sustained trade below 67,800 and weakening follow-through volume')}\n"
            "━━━━━━━━━━━━━━\n"
            "💧 *Liquidity*\n"
            f"🔹 Status: {mdv2_code('Bid-side absorption')}\n"
            f"🔹 Confirmation: {mdv2_code('Sweep-and-reclaim above intraday liquidity pocket')}\n"
            "━━━━━━━━━━━━━━\n"
            "⚖️ *Risk / Reward*\n"
            f"🔹 Ratio: {mdv2_code('1:2.45')}\n"
            f"🔹 Risk to SL: {mdv2_code('0.95%')}\n"
            "━━━━━━━━━━━━━━\n"
            "✨ *VIP\\+ EXTENSIONS*\n"
            f"🔥 Confidence Level: {mdv2_code('89%')}\n"
            "━━━━━━━━━━━━━━"
        )
        vip_signal_text = (
            "💎🦅 *FALCON ALERT* 🦅💎\n\n"
            f"*{mdv2_code('TESTBTC/USDT')}*\n"
            f"Position: *{mdv2_code('LONG')}* 📈\n"
            f"Leverage: *{mdv2_code('x10')}*\n"
            "━━━━━━━━━━━━━━\n"
            f"📥 *Entry:* {mdv2_code('$68,450.50')}\n"
            f"🛑 *Stop Loss:* {mdv2_code('$67,800.00')} \\({mdv2_code('0.95%')}\\)\n"
            "━━━━━━━━━━━━━━\n"
            "*Targets*\n"
            f"🎯 T1: {mdv2_code('$69,200.00')}\n"
            f"🎯 T2: {mdv2_code('$69,850.00')}\n"
            f"🎯 T3: {mdv2_code('$70,400.00')}"
        )
        chart_bytes = build_test_chart_image("TESTBTC/USDT")
        res_vip = send_photo(
            token=token,
            chat_id=vip_chat_id,
            caption=vip_signal_text,
            image_bytes=chart_bytes,
            parse_mode="MarkdownV2",
        )
        res_vip_plus = send_photo(
            token=token,
            chat_id=vip_plus_chat_id,
            caption=vip_plus_signal_text,
            image_bytes=chart_bytes,
            parse_mode="MarkdownV2",
        )
        print(f"  -> VIP send status={res_vip.status_code} ok={res_vip.ok}")
        print(f"  -> VIP+ send status={res_vip_plus.status_code} ok={res_vip_plus.ok}")
        if (not res_vip.ok) or (not res_vip_plus.ok):
            print("  -> send errors:", res_vip.text, res_vip_plus.text)
            return 1

        vip_message_id = int(res_vip.json()["result"]["message_id"])
        vip_plus_message_id = int(res_vip_plus.json()["result"]["message_id"])
        print(f"  -> captured message_ids VIP={vip_message_id}, VIP+={vip_plus_message_id}")

        # Track original message in DB (simulate production capture)
        await conn.execute(
            """
            UPDATE signals
            SET original_message_id = $1,
                original_chat_id = $2,
                chat_id = $2
            WHERE id = $3
            """,
            vip_chat_id and vip_message_id,
            vip_chat_id,
            inserted_id,
        )
        print("  -> DB updated with original_message_id/chat_id for reply logic")

        print("[3/5] Triggering fake Target Hit (T1) and sending reply...")
        elapsed = format_elapsed(signal_ts if signal_ts.tzinfo else signal_ts.replace(tzinfo=UTC))
        target_text = (
            f"🎯 *{esc_mdv2('TESTBTC/USDT')} TARGET 1 HIT\\!* 📈\n"
            "━━━━━━━━━━━━━━\n"
            f"💰 *Profit:* {mdv2_code('+2.35')}% \\(with {esc_mdv2('x10')}\\)\n"
            f"⏳ *Time Elapsed:* {mdv2_code(elapsed)}\n"
            f"📥 *Original Entry:* {mdv2_code('$68,450.50')}\n"
            "━━━━━━━━━━━━━━\n"
            "✅ *Status:* Goal 1 reached successfully\\."
        )
        t1_vip = send_message(
            token,
            {
                "chat_id": vip_chat_id,
                "text": target_text,
                "parse_mode": "MarkdownV2",
                "reply_to_message_id": vip_message_id,
                "allow_sending_without_reply": True,
            },
        )
        t1_vip_plus = send_message(
            token,
            {
                "chat_id": vip_plus_chat_id,
                "text": target_text,
                "parse_mode": "MarkdownV2",
                "reply_to_message_id": vip_plus_message_id,
                "allow_sending_without_reply": True,
            },
        )
        print(f"  -> VIP T1 reply status={t1_vip.status_code} ok={t1_vip.ok}")
        print(f"  -> VIP+ T1 reply status={t1_vip_plus.status_code} ok={t1_vip_plus.ok}")
        if (not t1_vip.ok) or (not t1_vip_plus.ok):
            print("  -> reply errors:", t1_vip.text, t1_vip_plus.text)
            return 1

        await conn.execute(
            "UPDATE signals SET last_target_hit = 1, status = 'T1_DONE' WHERE id = $1",
            inserted_id,
        )
        print("  -> DB updated: last_target_hit=1, status='T1_DONE'")

        print("[4/5] Building and sending Daily Summary to both groups...")
        daily_rows = await conn.fetch(
            """
            SELECT symbol, entry_price, status, COALESCE(last_target_hit, 0) AS last_target_hit
            FROM signals
            WHERE (timestamp AT TIME ZONE 'UTC')::date = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY id DESC
            LIMIT 20
            """
        )
        lines = []
        hits = 0
        missed = 0
        for row in daily_rows:
            symbol = str(row["symbol"]).replace("/USDT", "")
            price = float(row["entry_price"])
            # synthetic 1h change for simulation visibility
            change = "+2.35%" if int(row["last_target_hit"]) >= 1 else "-0.80%"
            emoji = "✅" if int(row["last_target_hit"]) >= 1 else "❌"
            lines.append(f"{symbol} | {price:.3f} | {change} {emoji}")
            if emoji == "✅":
                hits += 1
            else:
                missed += 1
        total = hits + missed
        win_rate = (hits / total * 100.0) if total else 0.0
        summary_text = (
            "📊 <b>DAILY SUMMARY</b>\n"
            f"<b>{datetime.now(UTC).strftime('%Y-%m-%d')} (SIMULATION)</b>\n"
            "<pre>"
            + "\n".join(lines)
            + "\n</pre>\n"
            "Summary:\n"
            f"✅ Hits: {hits}\n"
            f"❌ Missed: {missed}\n"
            f"💰 Win Rate: {win_rate:.2f}%"
        )
        ds_vip = send_message(token, {"chat_id": vip_chat_id, "text": summary_text, "parse_mode": "HTML"})
        ds_vip_plus = send_message(token, {"chat_id": vip_plus_chat_id, "text": summary_text, "parse_mode": "HTML"})
        print(f"  -> VIP summary status={ds_vip.status_code} ok={ds_vip.ok}")
        print(f"  -> VIP+ summary status={ds_vip_plus.status_code} ok={ds_vip_plus.ok}")
        if (not ds_vip.ok) or (not ds_vip_plus.ok):
            print("  -> summary errors:", ds_vip.text, ds_vip_plus.text)
            return 1

        print("[5/5] Simulation completed successfully.")
        if keep_row:
            print(f"  -> keeping test row in DB (id={inserted_id})")
        else:
            await conn.execute("DELETE FROM signals WHERE id = $1", inserted_id)
            print(f"  -> cleaned test row from DB (id={inserted_id})")
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Production test simulation for signal/target/daily summary flow.")
    parser.add_argument("--keep-row", action="store_true", help="Keep inserted simulation row in DB.")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(keep_row=args.keep_row)))
