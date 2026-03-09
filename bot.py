"""
bot.py  –  TennisEdge Telegram Bot
Commands:
    /start    – register user
    /balance  – check credits
    /signals  – view recent signals
    /buy      – credit packages info
    /help     – usage guide
    /scan     – admin: run pipeline manually
    /addcredits <telegram_id> <amount>  – admin: top up credits
    /result <signal_id> <winner_name>   – admin: record match outcome
    /accuracy – admin: view signal accuracy stats
    /alias <old_name> → <canonical_name> – admin: add player alias
    /buildelo – admin: build Elo from tennis-data.co.uk historical data
    /updateelo – admin: manually trigger daily Elo update + result tracking
    /stats    – admin: system stats with surface breakdown
"""

import asyncio
import logging
import os
from functools import wraps
from logging.handlers import RotatingFileHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID, CREDIT_PACKAGES, MOCK_MODE, UPI_ID
from database.db import (
    init_schema, get_or_create_user, get_user, add_credits_manual,
    get_recent_signals, record_signal_result, get_signal_accuracy,
    get_signal_by_id, add_player_alias, get_player_aliases,
    get_elo_player_count, get_stats_by_surface, get_user_credits,
)
from signals.formatter import format_signal_list
from scheduler.job import start_scheduler, run_pipeline, set_bot, set_loop
from scheduler.elo_update_job import run_daily_elo_update
from models.elo_model import seed_top_players, update_elo_after_match


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logging():
    """Configure console + rotating file loggers."""
    log_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(log_dir, "tennisedge.log")
    err_file = os.path.join(log_dir, "error.log")

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)

    file_h = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
    file_h.setLevel(logging.INFO)
    file_h.setFormatter(fmt)
    root.addHandler(file_h)

    err_h = RotatingFileHandler(err_file, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
    err_h.setLevel(logging.ERROR)
    err_h.setFormatter(fmt)
    root.addHandler(err_h)


setup_logging()
logger = logging.getLogger(__name__)


# ── Error handling decorator ──────────────────────────────────────────────────

def safe_handler(func):
    """Wrap command handlers with try/except for user-friendly error messages."""
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        try:
            return await func(update, ctx)
        except Exception as e:
            logger.error(f"Error in /{func.__name__}: {e}", exc_info=True)
            try:
                await update.message.reply_text(
                    "⚠️ Something went wrong. Please try again later."
                )
            except Exception as reply_err:
                logger.error(f"Failed to send error reply: {reply_err}")
    return wrapper


# ── /start ────────────────────────────────────────────────────────────────────

@safe_handler
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = get_or_create_user(user.id, user.username)

    welcome = (
        f"🎾 *Welcome to TennisEdge!*\n\n"
        f"I detect *value betting edges* in tennis matches using a "
        f"surface-adjusted Elo model.\n\n"
        f"💳 Your credits: *{db_user['credits']}*\n\n"
        f"*Commands:*\n"
        f"/signals – Latest edge signals\n"
        f"/matches – Today's upcoming matches\n"
        f"/balance – Check your credits\n"
        f"/buy – Purchase credits\n"
        f"/help – How it works\n\n"
        f"_Each signal costs 1 credit._"
    )
    await update.message.reply_text(welcome, parse_mode="Markdown")


# ── /balance ──────────────────────────────────────────────────────────────────

@safe_handler
async def balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = get_or_create_user(user.id, user.username)
    await update.message.reply_text(
        f"💳 *Your Balance*\n\n"
        f"Credits remaining: *{db_user['credits']}*\n\n"
        f"Use /buy to top up.",
        parse_mode="Markdown"
    )


# ── /buy ──────────────────────────────────────────────────────────────────────

@safe_handler
async def buy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    keyboard = []
    for key, pkg in CREDIT_PACKAGES.items():
        label = f"{key.upper()} – {pkg['credits']} credits @ ₹{pkg['price_inr']}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"buy_{key}")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "💳 *Purchase Credits*\n\n"
        "Choose a package below.\n"
        "After selecting, you'll receive UPI payment instructions.",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )


@safe_handler
async def buy_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    key = query.data.replace("buy_", "")
    pkg = CREDIT_PACKAGES.get(key)
    if not pkg:
        return

    admin_handle = f"@{ctx.bot_data.get('admin_username', 'admin')}"
    upi_display = f"`{UPI_ID}`" if UPI_ID else "_UPI ID not configured – contact admin_"

    await query.edit_message_text(
        f"💳 *{key.upper()} Package*\n\n"
        f"Credits: *{pkg['credits']}*\n"
        f"Price: *₹{pkg['price_inr']}*\n\n"
        f"*Payment Instructions:*\n"
        f"1. Send ₹{pkg['price_inr']} via UPI\n"
        f"2. UPI ID: {upi_display}\n"
        f"3. Screenshot the payment\n"
        f"4. Send screenshot to {admin_handle} with your Telegram ID\n\n"
        f"Credits will be added within 1 hour.\n\n"
        f"_Your Telegram ID: `{query.from_user.id}`_",
        parse_mode="Markdown"
    )


# ── /signals ──────────────────────────────────────────────────────────────────

@safe_handler
async def signals(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    recent = get_recent_signals(limit=5)
    msg = format_signal_list(recent)
    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /matches ─────────────────────────────────────────────────────────────────

SURFACE_EMOJI = {"clay": "🟤", "hard": "🔵", "grass": "🟢"}

@safe_handler
async def matches_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show tournament picker, then matches for selected tournament."""
    await update.message.reply_text("⏳ Fetching today's tournaments...")

    from ingestion.fetch_odds import fetch_odds
    matches = fetch_odds()
    if not matches:
        await update.message.reply_text("No upcoming matches found for today.")
        return

    # Group by tournament
    tournaments = {}
    for m in matches:
        t = m.get("tournament", "Unknown")
        tournaments.setdefault(t, []).append(m)

    # Build inline keyboard — one button per tournament + "All"
    keyboard = []
    for t_name in sorted(tournaments.keys()):
        count = len(tournaments[t_name])
        surf = tournaments[t_name][0].get("surface", "hard")
        emoji = SURFACE_EMOJI.get(surf, "⚪")
        label = f"{emoji} {t_name} ({count})"
        # Use short hash to keep callback_data under 64 bytes
        callback_id = str(abs(hash(t_name)) % 100000)
        keyboard.append([InlineKeyboardButton(label, callback_data=f"match_{callback_id}")])

    keyboard.append([InlineKeyboardButton(f"🎾 All Matches ({len(matches)})", callback_data="match_all")])

    # Store tournament map in bot_data so callback can look it up
    ctx.bot_data["_match_tournaments"] = tournaments
    ctx.bot_data["_match_callback_map"] = {
        str(abs(hash(t)) % 100000): t for t in tournaments
    }

    await update.message.reply_text(
        f"🎾 *Today's Tournaments* ({len(tournaments)})\n\nSelect a tournament:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def _format_matches(matches: list, title: str) -> str:
    """Format a list of matches into a readable message."""
    from models.elo_model import predict
    from models.model_components import final_probability
    from database.db import get_conn

    matches.sort(key=lambda m: (m.get("tournament", ""), m["odds_a"]))
    lines = [f"🎾 *{title}*\n"]
    current_tournament = None

    for m in matches:
        tourn = m.get("tournament", "Unknown")
        if tourn != current_tournament:
            current_tournament = tourn
            surf = m.get("surface", "hard")
            emoji = SURFACE_EMOJI.get(surf, "⚪")
            lines.append(f"\n{emoji} *{tourn}* ({surf})")

        pa = m["player_a"]
        pb = m["player_b"]
        oa = m["odds_a"]
        ob = m["odds_b"]

        try:
            elo = predict(pa, pb, m.get("surface", "hard"))
            with get_conn() as conn:
                prob_a = final_probability(pa, pb, m.get("surface", "hard"), conn, elo["prob_a"])
            try:
                from models.calibration import calibrate
                prob_a = calibrate(prob_a)
            except Exception:
                pass
            prob_b = 1.0 - prob_a
            model_str = f"  📊 Model: {prob_a:.0%} / {prob_b:.0%}"
        except Exception:
            model_str = ""

        lines.append(f"  ▸ {pa} vs {pb}")
        lines.append(f"    💰 Odds: {oa:.2f} / {ob:.2f}{model_str}")

    lines.append(f"\n_{len(matches)} matches_")
    return "\n".join(lines)


async def matches_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle tournament selection from /matches."""
    query = update.callback_query
    await query.answer()

    data = query.data  # "match_all" or "match_<id>"
    tournaments = ctx.bot_data.get("_match_tournaments", {})
    callback_map = ctx.bot_data.get("_match_callback_map", {})

    if not tournaments:
        await query.edit_message_text("⚠️ Session expired. Use /matches again.")
        return

    if data == "match_all":
        all_matches = [m for ms in tournaments.values() for m in ms]
        msg = await _format_matches(all_matches, "All Matches")
    else:
        t_id = data.replace("match_", "")
        t_name = callback_map.get(t_id)
        if not t_name or t_name not in tournaments:
            await query.edit_message_text("⚠️ Tournament not found. Use /matches again.")
            return
        msg = await _format_matches(tournaments[t_name], t_name)

    if len(msg) > 4000:
        await query.edit_message_text("Loading...")
        parts = msg.split("\n\n")
        chunk = ""
        for part in parts:
            if len(chunk) + len(part) > 3900:
                await query.message.reply_text(chunk, parse_mode="Markdown")
                chunk = ""
            chunk += part + "\n\n"
        if chunk.strip():
            await query.message.reply_text(chunk, parse_mode="Markdown")
    else:
        await query.edit_message_text(msg, parse_mode="Markdown")


# ── /help ─────────────────────────────────────────────────────────────────────

@safe_handler
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
        "/matches – Today's upcoming matches\n"
        "/buy – Purchase credits",
        parse_mode="Markdown"
    )


# ── Admin commands ────────────────────────────────────────────────────────────

def is_admin(user_id: int) -> bool:
    return user_id == TELEGRAM_ADMIN_ID


@safe_handler
async def scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: manually trigger the pipeline."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    await update.message.reply_text("🔄 Running pipeline scan...")
    try:
        await asyncio.to_thread(run_pipeline)
        await update.message.reply_text("✅ Pipeline scan complete. Check /signals.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


@safe_handler
async def addcredits(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /addcredits <telegram_id> <amount>"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    try:
        target_tid = int(ctx.args[0])
        amount     = int(ctx.args[1])
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /addcredits <telegram_id> <amount>")
        return

    db_user = get_user(target_tid)
    if not db_user:
        await update.message.reply_text(f"User {target_tid} not found.")
        return

    add_credits_manual(db_user["id"], amount)
    await update.message.reply_text(
        f"✅ Added {amount} credits to {db_user['username'] or target_tid}.\n"
        f"New balance: {db_user['credits'] + amount}"
    )


@safe_handler
async def result(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /result <signal_id> <winner_name> — record match outcome + update Elo."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    try:
        signal_id = int(ctx.args[0])
        winner_name = " ".join(ctx.args[1:])
        if not winner_name:
            raise ValueError
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /result <signal\\_id> <winner name>")
        return

    signal = get_signal_by_id(signal_id)
    if not signal:
        await update.message.reply_text(f"Signal #{signal_id} not found.")
        return

    # Validate winner actually played in this match
    from utils import normalize_player_name
    norm_winner = normalize_player_name(winner_name)
    norm_a = normalize_player_name(signal["player_a"])
    norm_b = normalize_player_name(signal["player_b"])
    if norm_winner != norm_a and norm_winner != norm_b:
        await update.message.reply_text(
            f"⚠️ '{winner_name}' didn't play in this match.\n"
            f"Players: {signal['player_a']} vs {signal['player_b']}"
        )
        return

    res = record_signal_result(signal_id, winner_name)
    if not res:
        await update.message.reply_text(f"Could not record result for signal #{signal_id}.")
        return

    # Update Elo ratings with actual match outcome
    loser_name = signal["player_b"] if norm_winner == norm_a else signal["player_a"]
    try:
        update_elo_after_match(winner_name, loser_name, signal["surface"])
        elo_msg = "Elo ratings updated."
    except Exception as e:
        elo_msg = f"Elo update failed: {e}"

    icon = "✅" if res["is_correct"] else "❌"
    await update.message.reply_text(
        f"{icon} *Result Recorded*\n\n"
        f"Signal #{signal_id}\n"
        f"Bet on: {res['bet_on']}\n"
        f"Winner: {res['actual_winner']}\n"
        f"Correct: {'Yes ✅' if res['is_correct'] else 'No ❌'}\n\n"
        f"_{elo_msg}_",
        parse_mode="Markdown"
    )


@safe_handler
async def accuracy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /accuracy — show signal prediction accuracy."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    stats = get_signal_accuracy()
    await update.message.reply_text(
        f"📊 *Signal Accuracy*\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"Total signals sent: *{stats['total_signals']}*\n"
        f"Results tracked: *{stats['tracked_results']}*\n"
        f"Untracked: *{stats['untracked']}*\n\n"
        f"✅ Wins: *{stats['wins']}*\n"
        f"❌ Losses: *{stats['losses']}*\n"
        f"📈 Accuracy: *{stats['accuracy_pct']}%*",
        parse_mode="Markdown"
    )


@safe_handler
async def alias_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /alias <alt_name> = <canonical_name>  OR  /alias (list all)."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    if not ctx.args:
        aliases = get_player_aliases()
        if not aliases:
            await update.message.reply_text("No aliases configured. Use /alias <alt\\_name> = <canonical>")
            return
        lines = [f"• `{a['alias']}` → `{a['canonical']}`" for a in aliases]
        await update.message.reply_text(
            f"🔗 *Player Aliases* ({len(aliases)})\n\n" + "\n".join(lines),
            parse_mode="Markdown"
        )
        return

    text = " ".join(ctx.args)
    if "=" not in text:
        await update.message.reply_text("Usage: /alias <alt\\_name> = <canonical\\_name>")
        return

    parts = text.split("=", 1)
    alt_name = parts[0].strip()
    canonical = parts[1].strip()
    if not alt_name or not canonical:
        await update.message.reply_text("Usage: /alias <alt\\_name> = <canonical\\_name>")
        return

    add_player_alias(alt_name, canonical)
    from utils import normalize_player_name
    await update.message.reply_text(
        f"✅ Alias added:\n`{normalize_player_name(alt_name)}` → `{normalize_player_name(canonical)}`",
        parse_mode="Markdown"
    )


@safe_handler
async def buildelo_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /buildelo [from_year] [to_year] — build Elo from historical data."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    from_year = 2015
    to_year = 2025
    try:
        if len(ctx.args) >= 1:
            from_year = int(ctx.args[0])
        if len(ctx.args) >= 2:
            to_year = int(ctx.args[1])
    except ValueError:
        await update.message.reply_text("Usage: /buildelo [from\\_year] [to\\_year]")
        return

    await update.message.reply_text(
        f"🔄 Building Elo from tennis-data.co.uk ({from_year}–{to_year})...\n"
        f"This may take a few minutes. You'll be notified when done."
    )

    import threading
    def _run_build():
        try:
            from ingestion.build_elo_from_history import build_elo
            years = list(range(from_year, to_year + 1))
            result = build_elo(atp_years=years, wta_years=years, flush_to_db=True)
            player_count = len(set(p for p, s in result.keys()))
            # Notify admin when done
            if _loop is not None and _loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    update.message.reply_text(
                        f"✅ *Elo Build Complete*\n\n"
                        f"Years: {from_year}–{to_year}\n"
                        f"Elo entries: {len(result)}\n"
                        f"Unique players: {player_count}",
                        parse_mode="Markdown"
                    ),
                    _loop,
                )
        except Exception as e:
            logger.error(f"Elo build failed: {e}", exc_info=True)
            if _loop is not None and _loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    update.message.reply_text(f"❌ Elo build failed: {e}"),
                    _loop,
                )

    # Run from scheduler/job.py's _loop reference
    from scheduler.job import _loop
    thread = threading.Thread(target=_run_build, daemon=True)
    thread.start()


@safe_handler
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Public: /stats — system overview with surface breakdown and ROI."""
    from database.db import get_conn
    conn = get_conn()

    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    signal_count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    delivery_count = conn.execute("SELECT COUNT(*) FROM signal_deliveries").fetchone()[0]
    active_subs = conn.execute("SELECT COUNT(*) FROM users WHERE credits > 0").fetchone()[0]
    total_credits = conn.execute("SELECT COALESCE(SUM(credits_added),0) FROM transactions").fetchone()[0]
    elo_count = get_elo_player_count()
    acc = get_signal_accuracy()

    # Surface breakdown
    surface_stats = get_stats_by_surface()
    surface_lines = ""
    for s in surface_stats:
        wr = round(s["wins"] / s["total"] * 100, 1) if s["total"] > 0 else 0
        avg_w = round(s["avg_edge_wins"] * 100, 1) if s["avg_edge_wins"] else 0
        avg_l = round(s["avg_edge_losses"] * 100, 1) if s["avg_edge_losses"] else 0
        emoji = {"hard": "🏟", "clay": "🟤", "grass": "🟢"}.get(s["surface"], "🎾")
        surface_lines += (
            f"{emoji} *{(s['surface'] or 'Unknown').title()}*: "
            f"{s['wins']}W/{s['losses']}L ({wr}%) "
            f"avg edge +{avg_w}%/−{avg_l}%\n"
        )

    if not surface_lines:
        surface_lines = "_No resolved signals yet_\n"

    await update.message.reply_text(
        f"📊 *TennisEdge Performance System*\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"📈 *ROI:* *{acc.get('roi_pct', 0.0)}%*\n"
        f"🎯 *Accuracy:* *{acc['accuracy_pct']}%* ({acc['wins']}/{acc['tracked_results']})\n"
        f"💰 *Avg Odds:* *{acc.get('avg_odds', 0.0)}*\n\n"
        f"👥 Users: *{user_count}* ({active_subs} active)\n"
        f"📡 Signals sent: *{signal_count}*\n"
        f"📬 Deliveries: *{delivery_count}*\n"
        f"🎾 Players (Elo Database): *{elo_count}*\n\n"
        f"*Surface Breakdown:*\n{surface_lines}\n"
        f"Mock mode: *{'ON' if MOCK_MODE else 'OFF'}*",
        parse_mode="Markdown"
    )


@safe_handler
async def updateelo_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /updateelo — manually trigger daily Elo update + result tracking."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    await update.message.reply_text("⏳ Running daily Elo update...")
    result = await asyncio.to_thread(run_daily_elo_update)
    await update.message.reply_text(
        f"✅ *Elo Update Complete*\n\n"
        f"🎾 Elo updated: *{result['elo_updated']}* matches\n"
        f"📊 Signals resolved: *{result['signals_resolved']}*",
        parse_mode="Markdown"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

async def post_init(application: Application):
    """Called after the Application is initialized and the event loop is running."""
    set_loop(asyncio.get_running_loop())
    start_scheduler()
    logger.info("🎾 Scheduler started via post_init.")


def main():
    # Init DB
    init_schema()

    # Seed top player Elo ratings (no-op if already present from build_elo_from_history)
    seed_top_players()

    if MOCK_MODE:
        logger.warning("⚠️  MOCK_MODE=true — using fake match data. Set MOCK_MODE=false for live data.")

    if not TELEGRAM_BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN not set in .env")

    # Build app with increased timeouts and post_init hook for scheduler
    from telegram.request import HTTPXRequest
    request = HTTPXRequest(connect_timeout=20.0, read_timeout=20.0, write_timeout=20.0)
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(request)
        .post_init(post_init)
        .build()
    )

    # Register handlers
    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("balance",     balance))
    app.add_handler(CommandHandler("buy",         buy))
    app.add_handler(CommandHandler("signals",     signals))
    app.add_handler(CommandHandler("matches",     matches_cmd))
    app.add_handler(CommandHandler("help",        help_cmd))
    app.add_handler(CommandHandler("scan",        scan))
    app.add_handler(CommandHandler("addcredits",  addcredits))
    app.add_handler(CommandHandler("result",      result))
    app.add_handler(CommandHandler("accuracy",    accuracy))
    app.add_handler(CommandHandler("alias",       alias_cmd))
    app.add_handler(CommandHandler("buildelo",    buildelo_cmd))
    app.add_handler(CommandHandler("updateelo",   updateelo_cmd))
    app.add_handler(CommandHandler("stats",       stats_cmd))
    app.add_handler(CallbackQueryHandler(buy_callback, pattern="^buy_"))
    app.add_handler(CallbackQueryHandler(matches_callback, pattern="^match_"))

    # Pass bot reference to scheduler
    set_bot(app)

    logger.info("🎾 TennisEdge bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
