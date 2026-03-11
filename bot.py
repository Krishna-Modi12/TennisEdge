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
        CREDIT_PACKAGES, MOCK_MODE, UPI_ID
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
        f"/predict \\– 🤖 AI match predictions\n"
        f"/portfolio \\– 📈 Live paper trading record\n"
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
        f"2. UPI ID: `{UPI_ID}`\n"
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
            loop = asyncio.get_running_loop()
            match_list = await loop.run_in_executor(None, fetch_odds)
            detected = await loop.run_in_executor(None, detect_edges, match_list)
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

    # Group by tournament
    tournaments = {}
    for m in match_list:
        t = m["tournament"]
        tournaments.setdefault(t, []).append(m)

    # Store in user context for callback
    ctx.user_data["matches_by_tournament"] = tournaments

    # Build inline keyboard with one button per tournament
    buttons = []
    for t_name, t_matches in tournaments.items():
        label = f"🏟 {t_name} ({len(t_matches)} matches)"
        # Use index as callback data to avoid length limits
        buttons.append([InlineKeyboardButton(label, callback_data=f"tourn_{len(buttons)}")])

    # Store tournament order
    ctx.user_data["tournament_order"] = list(tournaments.keys())

    await update.message.reply_text(
        f"🎾 *Select a Tournament*\n\n"
        f"Found *{len(match_list)}* upcoming matches across *{len(tournaments)}* tournaments.",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown",
    )


async def tournament_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    idx = int(query.data.split("_")[1])
    tournament_order = ctx.user_data.get("tournament_order", [])
    matches_by_tournament = ctx.user_data.get("matches_by_tournament", {})

    if idx >= len(tournament_order):
        await query.edit_message_text("❌ Tournament not found. Try /matches again.")
        return

    t_name = tournament_order[idx]
    t_matches = matches_by_tournament.get(t_name, [])

    if not t_matches:
        await query.edit_message_text("📭 No matches found for this tournament.")
        return

    try:
        from signals.edge_detector import odds_to_prob, calculate_true_edge
        from models.advanced_model import advanced_predict as predict
        from signals.formatter import _escape
    except ImportError:
        _escape = lambda x: x

    safe_t_name = _escape(t_name)
    lines = [f"🏟 *{safe_t_name}*\n"]
    for i, m in enumerate(t_matches, 1):
        safe_pa = _escape(m['player_a'])
        safe_pb = _escape(m['player_b'])
        edge_text = ""
        odds_text = ""
        if m.get("odds_a") and m.get("odds_b") and m["odds_a"] > 0 and m["odds_b"] > 0:
            odds_text = f"   📊 Odds: {m['odds_a']:.2f} / {m['odds_b']:.2f}\n"
            try:
                model = predict(m["player_a"], m["player_b"], m.get("surface", "hard"))
                # Dual validation edge for player A
                te_a = calculate_true_edge(model["prob_a"], m["odds_a"])
                te_b = calculate_true_edge(model["prob_b"], m["odds_b"])
                if te_a["signal_valid"]:
                    edge_text = f"   🔥 Edge: {safe_pa} +{te_a['true_edge_score']:.1%} (conf {te_a['confidence']:.1f}x)\n"
                elif te_b["signal_valid"]:
                    edge_text = f"   🔥 Edge: {safe_pb} +{te_b['true_edge_score']:.1%} (conf {te_b['confidence']:.1f}x)\n"
            except Exception:
                pass

        time_str = m.get("event_time", "")
        status = m.get("status", "")
        if status and status != "Finished":
            status_text = f"🔴 LIVE – {status}"
        elif time_str:
            status_text = f"🕐 {time_str}"
        else:
            status_text = ""

        lines.append(
            f"{i}. *{safe_pa}* vs *{safe_pb}*\n"
            f"   🌍 {m['surface'].title()}  |  {status_text}\n"
            f"{odds_text}"
            f"{edge_text}"
        )

    lines.append("\n_Use /matches to see all tournaments_")

    # Telegram has a 4096 char limit per message
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3950] + "\n\n_...message truncated_"

    await query.edit_message_text(text, parse_mode="Markdown")


# ── /predict ──────────────────────────────────────────────────────────────────

async def predict(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Interactive AI prediction: show upcoming matches as buttons."""
    await update.message.reply_text("🤖 Fetching matches for AI prediction...")
    try:
        match_list = fetch_odds()
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to fetch matches: {e}")
        return

    if not match_list:
        await update.message.reply_text("📭 No upcoming matches found right now. Try again later!")
        return

    # Store matches for callback lookup
    ctx.user_data["predict_matches"] = match_list

    # Build buttons — show up to 20 matches
    buttons = []
    for i, m in enumerate(match_list[:20]):
        # Truncate label to avoid Telegram callback_data 64-byte limit
        pa = m['player_a'][:15]
        pb = m['player_b'][:15]
        label = f"🎾 {pa} vs {pb}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"pred_{i}")])

    await update.message.reply_text(
        f"🤖 *Select a Match for AI Prediction*\n\n"
        f"Found *{len(match_list)}* upcoming matches.\n"
        f"Tap a match below to get a DeepSeek AI analysis:\n",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown",
    )


async def predict_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle match selection — run DeepSeek prediction."""
    query = update.callback_query
    await query.answer()

    idx = int(query.data.split("_")[1])
    match_list = ctx.user_data.get("predict_matches", [])

    if idx >= len(match_list):
        await query.edit_message_text("❌ Match not found. Try /predict again.")
        return

    m = match_list[idx]
    await query.edit_message_text(
        f"🤖 Analyzing...\n\n"
        f"⚔️ {m['player_a']} vs {m['player_b']}\n"
        f"🏟 {m.get('tournament', 'Unknown')}\n\n"
        f"Running DeepSeek AI model... please wait 15-30 seconds.",
    )

    try:
        # Get model prediction
        try:
            from models.advanced_model import advanced_predict as model_predict
            result = model_predict(m["player_a"], m["player_b"], m.get("surface", "hard"))
            prob_a = result["prob_a"]
            prob_b = result["prob_b"]
            data_quality = result.get("data_quality", "unknown")
        except Exception as ex:
            logger.warning(f"Model predict fallback: {ex}")
            prob_a = 0.5
            prob_b = 0.5
            data_quality = "elo_only"
            result = {}

        odds_a = m.get("odds_a", 0)
        odds_b = m.get("odds_b", 0)
        surface = m.get("surface", "hard")
        has_odds = bool(odds_a and odds_a > 0 and odds_b and odds_b > 0)

        # Determine who has the edge
        if has_odds:
            implied_a = 1.0 / odds_a
            edge_a = prob_a - implied_a
            implied_b = 1.0 / odds_b
            edge_b = prob_b - implied_b
        else:
            edge_a = 0
            edge_b = 0

        # Pick the player with the larger edge (or higher prob) for AI analysis
        if prob_a >= prob_b:
            focus_player = m["player_a"]
            focus_opponent = m["player_b"]
            focus_prob = prob_a
            focus_odds = odds_a if odds_a else 2.0
            focus_edge = edge_a if has_odds else 0.05
        else:
            focus_player = m["player_b"]
            focus_opponent = m["player_a"]
            focus_prob = prob_b
            focus_odds = odds_b if odds_b else 2.0
            focus_edge = edge_b if has_odds else 0.05

        # Call DeepSeek AI
        ai_text = ""
        try:
            from ai.analyzer import generate_match_analysis
            ai_text = await generate_match_analysis(
                player=focus_player,
                opponent=focus_opponent,
                surface=surface,
                model_prob=focus_prob,
                odds=focus_odds,
                value_edge=focus_edge,
                data_quality=data_quality,
                elo_prob=result.get("elo_prob_a"),
                form_prob=result.get("form_prob_a"),
                surface_prob=result.get("surface_prob_a"),
                h2h_prob=result.get("h2h_prob_a"),
            )
        except Exception as e:
            logger.warning(f"AI prediction error: {e}")
            ai_text = f"AI error: {e}"

        if not ai_text:
            # Distinguish between missing token and other failures
            hf_set = bool(os.environ.get("HF_TOKEN", ""))
            if not hf_set:
                ai_text = "HF_TOKEN not set. Add it in Render Dashboard > Environment to enable AI."
            else:
                ai_text = "AI model did not return a response. The service may be temporarily busy."

        # Format the response
        surface_emoji = {"clay": "🟤", "hard": "🔵", "grass": "🟢"}.get(surface, "🎾")
        prob_a_pct = round(prob_a * 100, 1)
        prob_b_pct = round(prob_b * 100, 1)

        msg = (
            f"🤖 DEEPSEEK AI PREDICTION\n"
            f"━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏆 {m.get('tournament', 'Tournament')}\n"
            f"⚔️ {m['player_a']} vs {m['player_b']}\n"
            f"{surface_emoji} Surface: {surface.title()}\n\n"
            f"📊 Model Probabilities:\n"
            f"  {m['player_a']}: {prob_a_pct}%\n"
            f"  {m['player_b']}: {prob_b_pct}%\n\n"
        )

        if has_odds:
            msg += (
                f"💰 Odds:\n"
                f"  {m['player_a']}: {odds_a:.2f}\n"
                f"  {m['player_b']}: {odds_b:.2f}\n\n"
            )
            edge_pct = round(max(edge_a, edge_b) * 100, 1)
            if edge_pct > 0:
                msg += f"💎 Best Edge: {focus_player} +{edge_pct}%\n\n"
        else:
            msg += "💰 Odds: Not yet available for this match\n\n"

        # Add model breakdown if available
        if data_quality != "elo_only" and result:
            elo_p = round(result.get("elo_prob_a", 0.5) * 100, 1)
            form_p = round(result.get("form_prob_a", 0.5) * 100, 1)
            surf_p = round(result.get("surface_prob_a", 0.5) * 100, 1)
            h2h_p = round(result.get("h2h_prob_a", 0.5) * 100, 1)
            msg += (
                f"📐 Model Breakdown:\n"
                f"  Elo (40%): {elo_p}%\n"
                f"  Form (25%): {form_p}%\n"
                f"  Surface (20%): {surf_p}%\n"
                f"  H2H (15%): {h2h_p}%\n\n"
            )
        elif prob_a == 0.5 and prob_b == 0.5:
            msg += "⚠️ Players not in database — model defaulted to 50/50\n\n"

        msg += (
            f"🤖 AI Analysis:\n"
            f"{ai_text}\n\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Powered by DeepSeek AI via HuggingFace"
        )

        if len(msg) > 4000:
            msg = msg[:3950] + "\n\n...truncated"

        await query.edit_message_text(msg)

    except Exception as e:
        logger.exception(f"predict_callback crashed: {e}")
        try:
            await query.edit_message_text(
                f"❌ Prediction failed: {e}\n\nPlease try /predict again."
            )
        except Exception:
            pass


# ── /portfolio (Paper Trading) ───────────────────────────────────────────────

async def portfolio(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """View the live paper trading track record of the bot."""
    from database.db import get_signal_accuracy
    
    try:
        stats = get_signal_accuracy()
    except Exception as e:
        logger.error(f"Error fetching portfolio stats: {e}")
        await update.message.reply_text("❌ Could not fetch portfolio stats right now.")
        return

    tracked = stats.get('tracked_results', 0)
    
    if tracked == 0:
        await update.message.reply_text(
            "📈 *Live Paper Trading Portfolio*\n\n"
            "The bot has started tracking paper trades, but no match results have been finalized yet. "
            "Check back after the upcoming matches finish!",
            parse_mode="Markdown"
        )
        return

    wins = stats.get('wins', 0)
    losses = stats.get('losses', 0)
    roi = stats.get('roi_pct', 0.0)
    accuracy = stats.get('accuracy_pct', 0.0)
    avg_odds = stats.get('avg_odds', 0.0)
    pending = stats.get('untracked', 0)
    
    profit_emoji = "🟢" if roi > 0 else ("🔴" if roi < 0 else "⚪")
    
    msg = (
        f"📈 *Live Paper Trading Portfolio*\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"The bot automatically places a virtual 1-unit flat bet on every valid signal "
        f"and verifies the result against live data.\n\n"
        f"*{profit_emoji} ROI: {roi}%*\n"
        f"🎯 *Win Rate:* {accuracy}%\n"
        f"⚖️ *Average Odds:* {avg_odds}\n\n"
        f"✅ *Wins:* {wins}\n"
        f"❌ *Losses:* {losses}\n"
        f"⏳ *Pending:* {pending}\n"
        f"🔢 *Total Tracked:* {tracked}\n\n"
        f"_Matches are automatically graded when they finish._"
    )
    
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
        "*AI Predictions*\n"
        "Use /predict to get DeepSeek AI analysis on any upcoming match.\n\n"
        "*Signal Threshold*\n"
        "We send signals ONLY when Value Edge ≥ 4% AND the Model Win Probability ≥ 35%.\n\n"
        "*Credits*\n"
        "Each signal costs 1 credit. Use /buy to top up.\n\n"
        "*Commands*\n"
        "/start – Register\n"
        "/balance – Check credits\n"
        "/signals – Latest signals\n"
        "/matches – Upcoming matches & odds\n"
        "/predict – 🤖 AI match predictions\n"
        "/portfolio – 📈 Live paper trading record\n"
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

    new_balance = add_credits_manual(db_user["id"], amount)
    await update.message.reply_text(
        f"✅ Added {amount} credits to "
        f"{db_user['username'] or target_tid}.\n"
        f"New balance: *{new_balance}*",
        parse_mode="Markdown"
    )


async def backtest_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /backtest [atp_from] [atp_to] — run historical backtest."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    atp_from = int(ctx.args[0]) if len(ctx.args) > 0 else 2023
    atp_to   = int(ctx.args[1]) if len(ctx.args) > 1 else 2024

    await update.message.reply_text(
        f"📊 Running backtest ({atp_from}-{atp_to})...\n"
        f"This may take a few minutes."
    )

    try:
        from backtest.engine import backtest, format_backtest_report
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: backtest(atp_from=atp_from, atp_to=atp_to,
                           wta_from=atp_from, wta_to=atp_to)
        )
        report = format_backtest_report(result)
        # Split if too long
        if len(report) > 4000:
            report = report[:3950] + "\n\n_...truncated_"
        await update.message.reply_text(report, parse_mode="Markdown")
    except Exception as e:
        logger.exception("Backtest error")
        await update.message.reply_text(f"❌ Backtest error: {e}")


# ── Global error handler ──────────────────────────────────────────────────────

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    BUG FIX: Without a global error handler, any unhandled exception in a
    handler is silently swallowed by PTB — the user gets no response and
    the error only appears in logs (if you're watching).
    Now errors are logged and the user gets a friendly message.
    """
    logger.error("Unhandled exception in handler:", exc_info=ctx.error)
    # Try to notify the user regardless of update type
    try:
        if isinstance(update, Update):
            if update.callback_query:
                await update.callback_query.edit_message_text(
                    "⚠️ Something went wrong. Please try again."
                )
            elif update.message:
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
        _startup_status = "elo done, building stats..."
        print("Elo seeding done.", flush=True)

        # Build player stats in background to avoid blocking bot startup
        import threading
        def _build_stats_bg():
            try:
                from ingestion.build_player_stats import build_stats
                print("Building player stats (background)...", flush=True)
                build_stats(atp_from=2020, atp_to=2025, wta_from=2020, wta_to=2025)
            except Exception as e:
                print(f"⚠️ Player stats build failed: {e}", flush=True)
        threading.Thread(target=_build_stats_bg, daemon=True).start()

        _startup_status = "stats building, starting bot..."
        print("Stats build started in background.", flush=True)
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
    app.add_handler(CommandHandler("predict",    predict))
    app.add_handler(CommandHandler("portfolio",  portfolio))
    app.add_handler(CommandHandler("help",       help_cmd))
    # Admin commands
    app.add_handler(CommandHandler("scan",       scan))
    app.add_handler(CommandHandler("addcredits", addcredits))
    app.add_handler(CommandHandler("backtest",   backtest_cmd))
    # Callbacks
    app.add_handler(CallbackQueryHandler(buy_callback, pattern="^buy_"))
    app.add_handler(CallbackQueryHandler(tournament_callback, pattern="^tourn_"))
    app.add_handler(CallbackQueryHandler(predict_callback, pattern="^pred_"))
    # Global error handler
    app.add_error_handler(error_handler)

    set_bot(app)
    start_scheduler()

    _startup_status = "running"
    print("🎾 TennisEdge bot is running...", flush=True)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
