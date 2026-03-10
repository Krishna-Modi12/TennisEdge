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

import asyncio
import logging
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)

from config import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID,
    CREDIT_PACKAGES, MOCK_MODE
)
from database.db import (
    init_schema, get_or_create_user, get_user,
    add_credits_manual, get_recent_signals
)
from signals.formatter import format_signal_list
from scheduler.job import start_scheduler, run_pipeline, set_bot, set_loop
from models.elo_model import seed_top_players

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
    msg    = format_signal_list(recent)
    await update.message.reply_text(msg, parse_mode="Markdown")


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
    # Start health check HTTP server for Render
    _start_health_server()

    init_schema()
    seed_top_players()

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

    print("🎾 TennisEdge bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()


# ── Health-check HTTP server (keeps Render web service alive) ────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass  # suppress request logs


def _start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"Health server listening on :{port}")
