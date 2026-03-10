"""
bot.py  –  TennisEdge Telegram Bot
Commands:
    /start       – register user
    /balance     – check credits
    /signals     – view recent signals
    /buy         – credit packages info
    /help        – usage guide
    /scan        – admin: run pipeline manually
    /addcredits  – admin: top up credits
"""

import sys
import os
import time
import traceback
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Health-check HTTP server (must start before heavy imports) ───────────────

_startup_status = "starting..."


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(_startup_status.encode())

    def log_message(self, *args):
        pass


def _start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"Health server listening on :{port}", flush=True)


# Start health server immediately
_start_health_server()

print(f"Python {sys.version}", flush=True)
print(f"DATABASE_URL set: {bool(os.environ.get('DATABASE_URL'))}", flush=True)
print(f"BOT_TOKEN set: {bool(os.environ.get('TELEGRAM_BOT_TOKEN'))}", flush=True)

# ── Heavy imports (one-by-one for diagnostics) ──────────────────────────────
try:
    _startup_status = "import: asyncio"
    import asyncio
    print("  asyncio OK", flush=True)

    _startup_status = "import: logging"
    import logging
    print("  logging OK", flush=True)

    _startup_status = "import: telegram"
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application, CommandHandler, CallbackQueryHandler,
        ContextTypes, MessageHandler, filters
    )
    print("  telegram OK", flush=True)

    _startup_status = "import: config"
    from config import (
        TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID,
        CREDIT_PACKAGES, MOCK_MODE
    )
    print("  config OK", flush=True)

    _startup_status = "import: database.db"
    from database.db import (
        init_schema, get_or_create_user, get_user,
        add_credits_manual, get_recent_signals
    )
    print("  database.db OK", flush=True)

    _startup_status = "import: signals.formatter"
    from signals.formatter import format_signal_list
    print("  signals.formatter OK", flush=True)

    _startup_status = "import: scheduler.job"
    from scheduler.job import start_scheduler, run_pipeline, set_bot, set_loop
    print("  scheduler.job OK", flush=True)

    _startup_status = "import: models.elo_model"
    from models.elo_model import seed_top_players
    print("  models.elo_model OK", flush=True)

    _startup_status = "import: ingestion.fetch_odds"
    from ingestion.fetch_odds import fetch_odds
    print("  ingestion.fetch_odds OK", flush=True)

    _startup_status = "imports OK"
    print("All imports succeeded.", flush=True)
except Exception as e:
    tb = traceback.format_exc()
    _startup_status = f"IMPORT ERROR at [{_startup_status}]:\n{tb}"
    print(f"IMPORT ERROR:\n{tb}", flush=True)
    while True:
        time.sleep(60)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ── post_init hook ────────────────────────────────────────────────────────────

async def post_init(app: Application) -> None:
    """
    Called by python-telegram-bot AFTER it has started its own event loop
    via asyncio.run(). This is the ONLY safe place to capture the live loop.

    BUG FIX: Capturing asyncio.get_event_loop() before run_polling() gives
    a dead/wrong loop. PTB v20 creates a fresh loop inside asyncio.run().
    Using post_init guarantees we get the actual running loop.
    """
    loop = asyncio.get_running_loop()
    set_loop(loop)
    logger.info("post_init: live event loop captured for scheduler.")


# ── /start ────────────────────────────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    db_user = get_or_create_user(user.id, user.username)

    welcome = (
        f"🎾 *Welcome to TennisEdge\\!*\n\n"
        f"I detect *value betting edges* in tennis matches using a "
        f"surface\\-adjusted Elo model\\.\n\n"
        f"💳 Your credits: *{db_user['credits']}*\n\n"
        f"*Commands:*\n"
        f"/signals \\– Latest edge signals\n"
        f"/matches \\– Upcoming matches & odds\n"
        f"/balance \\– Check your credits\n"
        f"/buy \\– Purchase credits\n"
        f"/help \\– How it works\n\n"
        f"_Each signal costs 1 credit\\._"
    )
    await update.message.reply_text(welcome, parse_mode="Markdown")


# ── /balance ──────────────────────────────────────────────────────────────────

async def balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    db_user = get_or_create_user(user.id, user.username)
    await update.message.reply_text(
        f"💳 *Your Balance*\n\n"
        f"Credits remaining: *{db_user['credits']}*\n\n"
        f"Use /buy to top up.",
        parse_mode="Markdown"
    )


# ── /buy ──────────────────────────────────────────────────────────────────────

async def buy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    keyboard = []
    for key, pkg in CREDIT_PACKAGES.items():
        label = f"{key.upper()} – {pkg['credits']} credits @ ₹{pkg['price_inr']}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"buy_{key}")])

    await update.message.reply_text(
        "💳 *Purchase Credits*\n\n"
        "Choose a package below.\n"
        "After selecting, you'll receive UPI payment instructions.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def buy_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    key = query.data.replace("buy_", "")
    pkg = CREDIT_PACKAGES.get(key)
    if not pkg:
        return

    # BUG FIX: admin_username was read from bot_data which was never populated,
    # always showing "@admin". Now read from config.
    admin_id = TELEGRAM_ADMIN_ID
    try:
        admin_info = await ctx.bot.get_chat(admin_id)
        admin_handle = f"@{admin_info.username}" if admin_info.username else f"Admin (ID: {admin_id})"
    except Exception:
        admin_handle = f"Admin (ID: {admin_id})"

    await query.edit_message_text(
        f"💳 *{key.upper()} Package*\n\n"
        f"Credits: *{pkg['credits']}*\n"
        f"Price: *₹{pkg['price_inr']}*\n\n"
        f"*Payment Instructions:*\n"
        f"1. Send ₹{pkg['price_inr']} via UPI\n"
        f"2. UPI ID: `your-upi-id@bank`\n"
        f"3. Screenshot the payment\n"
        f"4. Send screenshot to {admin_handle} with your Telegram ID\n\n"
        f"Credits will be added within 1 hour.\n\n"
        f"_Your Telegram ID: `{query.from_user.id}`_",
        parse_mode="Markdown"
    )


# ── /signals ──────────────────────────────────────────────────────────────────

async def signals(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    recent = get_recent_signals(limit=5)
    if not recent:
        # Run pipeline on-demand if no signals in DB yet
        await update.message.reply_text("⏳ Scanning for edges...")
        try:
            from signals.edge_detector import detect_edges
            match_list = fetch_odds()
            detected = detect_edges(match_list)
            if detected:
                recent = get_recent_signals(limit=5)
        except Exception as e:
            logger.error(f"On-demand pipeline error: {e}")
    msg = format_signal_list(recent)
    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /matches ─────────────────────────────────────────────────────────────────

async def matches(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Fetching live matches...")
    try:
        match_list = fetch_odds()
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to fetch matches: {e}")
        return

    if not match_list:
        await update.message.reply_text("📭 No upcoming matches found right now. Check back later!")
        return

    try:
        from signals.edge_detector import odds_to_prob
        from models.elo_model import predict
        from config import EDGE_THRESHOLD
    except ImportError:
        pass

    lines = ["🎾 *Upcoming Matches*\n"]
    for i, m in enumerate(match_list[:15], 1):
        edge_text = ""
        odds_text = ""
        if m.get("odds_a") and m.get("odds_b") and m["odds_a"] > 0 and m["odds_b"] > 0:
            odds_text = f"   📊 Odds: {m['odds_a']:.2f} / {m['odds_b']:.2f}\n"
            try:
                model = predict(m["player_a"], m["player_b"], m.get("surface", "hard"))
                market_a = odds_to_prob(m["odds_a"])
                market_b = odds_to_prob(m["odds_b"])
                edge_a = model["prob_a"] - market_a
                edge_b = model["prob_b"] - market_b
                if edge_a >= EDGE_THRESHOLD:
                    edge_text = f"   🔥 Edge: {m['player_a']} +{edge_a:.1%}\n"
                elif edge_b >= EDGE_THRESHOLD:
                    edge_text = f"   🔥 Edge: {m['player_b']} +{edge_b:.1%}\n"
            except Exception:
                pass

        time_str = m.get("event_time", "")
        status = m.get("status", "")
        status_text = f"🔴 {status}" if status and status != "Finished" else f"🕐 {time_str}" if time_str else ""

        lines.append(
            f"{i}. *{m['player_a']}* vs *{m['player_b']}*\n"
            f"   🏟 {m['tournament']}  |  🌍 {m['surface'].title()}\n"
            f"   {status_text}\n"
            f"{odds_text}"
            f"{edge_text}"
        )
    if len(match_list) > 15:
        lines.append(f"\n_...and {len(match_list) - 15} more matches_")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /help ─────────────────────────────────────────────────────────────────────

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎾 *TennisEdge – How It Works*\n\n"
        "*What is an Edge?*\n"
        "Edge = Model Probability – Market Probability\n\n"
        "When our Elo model says a player has a 58% chance of winning, "
        "but the bookmaker's odds imply only 47%, that's an 11% edge — "
        "a potentially mispriced bet.\n\n"
        "*Surface Adjustment*\n"
        "We track separate Elo ratings for Clay, Hard, and Grass courts. "
        "A player's clay record is weighted more heavily on clay matches.\n\n"
        "*Signal Threshold*\n"
        "We only send signals when edge > 7%.\n\n"
        "*Credits*\n"
        "Each signal costs 1 credit. Use /buy to top up.\n\n"
        "*Commands*\n"
        "/start – Register\n"
        "/balance – Check credits\n"
        "/signals – Latest signals\n"
        "/matches – Upcoming matches & odds\n"
        "/buy – Purchase credits",
        parse_mode="Markdown"
    )


# ── Admin commands ────────────────────────────────────────────────────────────

def is_admin(user_id: int) -> bool:
    return TELEGRAM_ADMIN_ID != 0 and user_id == TELEGRAM_ADMIN_ID


async def scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: manually trigger the full pipeline."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    await update.message.reply_text("🔄 Running pipeline scan...")
    try:
        # BUG FIX: run_pipeline() is blocking (HTTP + DB). Running it directly
        # in an async handler freezes the event loop for all users.
        # run_in_executor() offloads it to a thread pool.
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, run_pipeline)
        await update.message.reply_text("✅ Scan complete. Check /signals.")
    except Exception as e:
        logger.exception("Pipeline scan error")
        await update.message.reply_text(f"❌ Error: {e}")


async def addcredits(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /addcredits <telegram_id> <amount>"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    try:
        target_tid = int(ctx.args[0])
        amount     = int(ctx.args[1])
        if amount <= 0:
            raise ValueError("Amount must be positive")
    except (IndexError, ValueError) as e:
        await update.message.reply_text(f"Usage: /addcredits <telegram_id> <amount>\nError: {e}")
        return

    db_user = get_user(target_tid)
    if not db_user:
        await update.message.reply_text(f"❌ User {target_tid} not found. They must /start first.")
        return

    # BUG FIX: add_credits_manual now returns the new balance directly,
    # so we show the correct post-update value instead of stale pre-update data.
    new_balance = add_credits_manual(db_user["id"], amount)
    await update.message.reply_text(
        f"✅ Added {amount} credits to "
        f"{db_user['username'] or target_tid}.\n"
        f"New balance: *{new_balance}*",
        parse_mode="Markdown"
    )


# ── Global error handler ──────────────────────────────────────────────────────

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    BUG FIX: Without a global error handler, any unhandled exception in a
    handler is silently swallowed by PTB — the user gets no response and
    the error only appears in logs (if you're watching).
    Now errors are logged and the user gets a friendly message.
    """
    logger.error("Unhandled exception in handler:", exc_info=ctx.error)
    if isinstance(update, Update) and update.message:
        try:
            await update.message.reply_text(
                "⚠️ Something went wrong. Please try again in a moment."
            )
        except Exception:
            pass


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global _startup_status

    try:
        _startup_status = "connecting to database..."
        print("Connecting to database...", flush=True)
        init_schema()
        _startup_status = "database ready, seeding elo..."
        print("Database schema ready.", flush=True)
        seed_top_players()
        _startup_status = "elo done, building bot..."
        print("Elo seeding done.", flush=True)
    except Exception as e:
        tb = traceback.format_exc()
        _startup_status = f"FATAL: {tb}"
        print(f"FATAL: Database initialization failed:\n{tb}", flush=True)
        while True:
            time.sleep(60)

    if MOCK_MODE:
        print("⚠️  MOCK_MODE=true — using fake match data.")

    if not TELEGRAM_BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN not set in .env")

    if TELEGRAM_ADMIN_ID == 0:
        print("⚠️  TELEGRAM_ADMIN_ID not set — admin commands disabled.")

    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)   # captures live loop for scheduler
        .build()
    )

    # User commands
    app.add_handler(CommandHandler("start",      start))
    app.add_handler(CommandHandler("balance",    balance))
    app.add_handler(CommandHandler("buy",        buy))
    app.add_handler(CommandHandler("signals",    signals))
    app.add_handler(CommandHandler("matches",    matches))
    app.add_handler(CommandHandler("help",       help_cmd))
    # Admin commands
    app.add_handler(CommandHandler("scan",       scan))
    app.add_handler(CommandHandler("addcredits", addcredits))
    # Callbacks
    app.add_handler(CallbackQueryHandler(buy_callback, pattern="^buy_"))
    # Global error handler
    app.add_error_handler(error_handler)

    set_bot(app)
    start_scheduler()

    _startup_status = "running"
    print("🎾 TennisEdge bot is running...", flush=True)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
