"""
bot.py  –  TennisEdge Telegram Bot
Commands:
    /start       – register user
    /balance     – check credits
    /signals     – view recent signals
    /buy         – credit packages info
    /beta        – join free beta channel
    /help        – usage guide
    /scan        – admin: run pipeline manually
    /addcredits  – admin: top up credits
    /broadcastbeta – admin: invite all users to beta channel
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
        CREDIT_PACKAGES, MOCK_MODE, UPI_ID,
        BETA_CHANNEL_LINK, BETA_CHANNEL_NAME,
    )
    print("  config OK", flush=True)

    _startup_status = "import: database.db"
    from database.db import (
        init_schema, get_or_create_user, get_user,
        add_credits_manual, get_recent_signals,
        get_all_user_telegram_ids, deduct_credit_atomic,
    )
    print("  database.db OK", flush=True)

    _startup_status = "import: signals.formatter"
    from signals.formatter import format_signal_list, format_match_card
    print("  signals.formatter OK", flush=True)

    _startup_status = "import: integrations.tennis_api"
    from integrations.tennis_api import (
        get_player_stats, get_h2h_stats,
        get_current_tournament_form, get_player_ranking,
    )
    print("  integrations.tennis_api OK", flush=True)

    _startup_status = "import: models.match_helpers"
    from models.match_helpers import (
        fair_odds, kelly_stake, confidence_tier,
        total_games_probability, format_prob,
    )
    print("  models.match_helpers OK", flush=True)

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


def _main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎯 Predictions", callback_data="menu_predictions"),
            InlineKeyboardButton("🔥 Value Bets", callback_data="menu_vbets"),
        ],
        [
            InlineKeyboardButton("📊 Total Games", callback_data="menu_tgames"),
            InlineKeyboardButton("💰 My Wallet", callback_data="menu_wallet"),
        ],
        [InlineKeyboardButton("📚 Knowledge Base", callback_data="menu_kb")],
    ])


# ── /start ────────────────────────────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    db_user = get_or_create_user(user.id, user.username)

    welcome = (
        f"🎾 *Welcome to TennisEdge!*\n\n"
        f"I detect *value betting edges* in tennis matches using a "
        f"surface-adjusted Elo model.\n\n"
        f"💳 Your credits: *{db_user['credits']}*\n\n"
        f"*Commands:*\n"
        f"/signals – Latest edge signals\n"
        f"/matches – Upcoming matches & odds\n"
        f"/predict – 🤖 AI match predictions\n"
        f"/portfolio – 📈 Live paper trading record\n"
        f"/balance – Check your credits\n"
        f"/buy – Purchase credits\n"
        f"/beta – Join free beta channel\n"
        f"/help – How it works\n\n"
        f"_Each signal costs 1 credit._"
    )
    await update.message.reply_text(
        welcome,
        parse_mode="Markdown",
        reply_markup=_main_menu_keyboard(),
    )


async def menu_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = get_or_create_user(user.id, user.username)
    await update.message.reply_text(
        f"🎾 *TennisEdge Menu*\n"
        f"Credits: *{db_user['credits']}*\n"
        f"Choose an option:",
        parse_mode="Markdown",
        reply_markup=_main_menu_keyboard(),
    )


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


# ── /beta ─────────────────────────────────────────────────────────────────────

async def beta(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User: get free beta community channel invite."""
    if not BETA_CHANNEL_LINK:
        await update.message.reply_text(
            "📢 *Beta Channel*\n\n"
            "The free beta channel is not configured yet.\n"
            "Please check back shortly.",
            parse_mode="Markdown",
        )
        return

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Join Beta Channel", url=BETA_CHANNEL_LINK)]]
    )
    await update.message.reply_text(
        f"📢 *{BETA_CHANNEL_NAME}*\n\n"
        f"Join our free beta channel for daily updates, performance snapshots, "
        f"and early access alerts.",
        parse_mode="Markdown",
        reply_markup=keyboard,
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
        loop = asyncio.get_running_loop()
        match_list = await loop.run_in_executor(None, fetch_odds)
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
                from signals.edge_detector import _de_vig_probs, calculate_true_edge
                from tennis_backtest.elo_filter import elo_agrees as _elo_agrees
                from models.elo_model import predict as elo_predict
                
                elo_model = elo_predict(m["player_a"], m["player_b"], m.get("surface", "hard"))
                pinny_a = m.get("pinny_odds_a")
                pinny_b = m.get("pinny_odds_b")
                
                prob_a, prob_b = _de_vig_probs(pinny_a, pinny_b)
                if prob_a is None:
                    prob_a, prob_b = _de_vig_probs(m["odds_a"], m["odds_b"])
                
                if prob_a is not None:
                    te_a = calculate_true_edge(prob_a, m["odds_a"])
                    te_b = calculate_true_edge(prob_b, m["odds_b"])
                    
                    elo_agrees_a = _elo_agrees(prob_a, elo_model["prob_a"], max_gap=0.15)
                    elo_agrees_b = _elo_agrees(prob_b, elo_model["prob_b"], max_gap=0.15)
                    
                    if te_a["signal_valid"] and elo_agrees_a:
                        edge_text = f"   🔥 Edge: {safe_pa} +{te_a['true_edge_score']:.1%} (conf {te_a['confidence']:.1f}x)\n"
                    elif te_b["signal_valid"] and elo_agrees_b:
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
        loop = asyncio.get_running_loop()
        match_list = await loop.run_in_executor(None, fetch_odds)
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

    try:
        idx = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.edit_message_text("❌ Selection error. Try /predict again.")
        return

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
        from models.advanced_model import advanced_predict
        from signals.edge_detector import calculate_true_edge

        # Get core model prediction (Unified 4-Factor Balanced Model)
        surface = m.get("surface", "hard")
        res = advanced_predict(m["player_a"], m["player_b"], surface)
        
        prob_a = res["prob_a"]
        prob_b = res["prob_b"]
        data_quality = res.get("data_quality", "partial")

        odds_a = float(m.get("odds_a") or 0)
        odds_b = float(m.get("odds_b") or 0)

        # Determine who to focus on (player with higher prob)
        if prob_a >= prob_b:
            focus_player = m["player_a"]
            focus_opponent = m["player_b"]
            focus_prob = prob_a
            focus_odds = odds_a
            # Probs for Player A
            f_elo = res.get("elo_prob_a", 0.5)
            f_form = res.get("form_prob_a", 0.5)
            f_surf = res.get("surface_prob_a", 0.5)
            f_h2h = res.get("h2h_prob_a", 0.5)
        else:
            focus_player = m["player_b"]
            focus_opponent = m["player_a"]
            focus_prob = prob_b
            focus_odds = odds_b
            # Probs for Player B (inverted)
            f_elo = 1.0 - res.get("elo_prob_a", 0.5)
            f_form = 1.0 - res.get("form_prob_a", 0.5)
            f_surf = 1.0 - res.get("surface_prob_a", 0.5)
            f_h2h = 1.0 - res.get("h2h_prob_a", 0.5)

        # Calculate edge for focus player
        focus_edge = 0.0
        if focus_odds > 0:
            te = calculate_true_edge(focus_prob, focus_odds)
            focus_edge = te["true_edge_score"]

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
                elo_prob=f_elo,
                form_prob=f_form,
                surface_prob=f_surf,
                h2h_prob=f_h2h,
            )
        except Exception as ai_err:
            logger.warning(f"AI error: {ai_err}")
            ai_text = "AI analysis temporarily unavailable."

        if not ai_text:
            hf_set = bool(os.environ.get("HF_TOKEN", ""))
            ai_text = "AI analysis is currently disabled (HF_TOKEN missing)." if not hf_set else "AI service busy."

        # Format Final Result
        surface_emoji = {"clay": "🟤", "hard": "🔵", "grass": "🟢"}.get(surface, "🎾")
        
        msg = (
            f"🤖 *AI Match Prediction*\n"
            f"━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚔️ *{m['player_a']} vs {m['player_b']}*\n"
            f"🏟 {m.get('tournament', 'Unknown')} | {surface_emoji} {surface.title()}\n\n"
            f"📈 *Winning Probabilities:*\n"
            f"• {m['player_a']}: {round(prob_a*100, 1)}%\n"
            f"• {m['player_b']}: {round(prob_b*100, 1)}%\n\n"
            f"🔥 *Focus:* {focus_player}\n"
            f"💰 *Market Odds:* {focus_odds if focus_odds > 0 else 'N/A'}\n"
            f"💎 *Estimated Edge:* {round(focus_edge*100, 1)}%\n\n"
            f"📋 *Model Rationale:*\n"
            f"_{ai_text}_\n\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"_TennisEdge Predictive Model_"
        )

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="menu_home")]])
        )

    except Exception as e:
        logger.exception(f"predict_callback fatal error: {e}")
        await query.edit_message_text("❌ An error occurred during analysis. Try again later.")



# ── /menu callbacks ───────────────────────────────────────────────────────────

async def menu_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "menu_predictions":
        await query.edit_message_text(
            "🎾 Select a tour:",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("ATP", callback_data="tour_sel_atp"),
                    InlineKeyboardButton("WTA", callback_data="tour_sel_wta"),
                ],
                [InlineKeyboardButton("🏠 Home", callback_data="menu_home")],
            ]),
        )

    elif data == "menu_vbets":
        await query.edit_message_text("🔎 Scanning for value bets...")
        loop = asyncio.get_running_loop()
        try:
            match_list = await loop.run_in_executor(None, fetch_odds)
            from signals.edge_detector import detect_edges
            signals = await loop.run_in_executor(None, detect_edges, match_list)
        except Exception as e:
            logger.error(f"Value bets error: {e}")
            signals = []

        if not signals:
            await query.edit_message_text(
                "🔥 *Top Value Bets Today*\n\n"
                "No high-value signals right now.\n"
                "Check back later!",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("🔄 Refresh", callback_data="menu_vbets"),
                        InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
                    ]
                ]),
            )
            return

        ctx.user_data["vbets_signals"] = signals
        top5 = sorted(
            signals,
            key=lambda x: x.get("true_edge_score", x.get("edge", 0)),
            reverse=True,
        )[:5]

        buttons = []
        for i, s in enumerate(top5):
            edge_pct = round(s.get("true_edge_score", s.get("edge", 0)) * 100, 1)
            label = f"{s.get('bet_on', '')[:15]} @ {s.get('odds')} | Edge: {edge_pct}%"
            buttons.append([InlineKeyboardButton(label, callback_data=f"vbets_{i}")])
        buttons.append(
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="menu_vbets"),
                InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
            ]
        )
        await query.edit_message_text(
            "🔥 *Top Value Bets Today*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    elif data == "menu_tgames":
        await query.edit_message_text("📊 Loading total games market...")
        loop = asyncio.get_running_loop()
        try:
            match_list = await loop.run_in_executor(None, fetch_odds)
        except Exception:
            await query.edit_message_text(
                "Failed to load matches.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Home", callback_data="menu_home")]
                ]),
            )
            return

        tg_matches = []
        for m in match_list:
            surface = m.get("surface", "hard")
            stats_a = get_player_stats(m["player_a"], surface)
            stats_b = get_player_stats(m["player_b"], surface)
            if stats_a and stats_b:
                probs = total_games_probability(
                    stats_a.get("first_serve_won_pct"),
                    stats_b.get("first_serve_won_pct"),
                    stats_a.get("second_serve_won_pct"),
                    stats_b.get("second_serve_won_pct"),
                )
                if probs:
                    tg_matches.append({"match": m, "probs": probs})

        tg_matches.sort(key=lambda x: x["probs"]["over"], reverse=True)
        ctx.user_data["tg_matches"] = tg_matches

        if not tg_matches:
            await query.edit_message_text(
                "📊 Serve stats loading.\nCheck back soon!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Home", callback_data="menu_home")]
                ]),
            )
            return

        buttons = []
        for i, item in enumerate(tg_matches[:15]):
            m = item["match"]
            over = round(item["probs"]["over"] * 100, 0)
            label = f"{m['player_a'][:12]} vs {m['player_b'][:12]} | O22.5: {int(over)}%"
            buttons.append([InlineKeyboardButton(label, callback_data=f"tgames_{i}")])
        buttons.append([InlineKeyboardButton("🏠 Home", callback_data="menu_home")])
        await query.edit_message_text(
            "📊 *Total Games Market*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    elif data == "menu_wallet":
        user = update.effective_user
        db_user = get_or_create_user(user.id, user.username)
        await query.edit_message_text(
            f"💳 *Your Credits*\n\n"
            f"Balance: *{db_user['credits']}* credits\n\n"
            f"Usage:\n"
            f"Signal / Analysis = 1 credit\n"
            f"Browsing & lists = free",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Buy Credits", callback_data="menu_buycredits")],
                [InlineKeyboardButton("⬅ Back", callback_data="menu_home")],
            ]),
        )

    elif data == "menu_buycredits":
        keyboard = []
        for key, pkg in CREDIT_PACKAGES.items():
            label = f"{key.upper()} – {pkg['credits']} credits @ ₹{pkg['price_inr']}"
            keyboard.append([InlineKeyboardButton(label, callback_data=f"buy_{key}")])
        keyboard.append([InlineKeyboardButton("⬅ Back", callback_data="menu_wallet")])
        await query.edit_message_text(
            "💳 *Purchase Credits*\n\nChoose a package:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data == "menu_kb":
        await query.edit_message_text(
            "📚 *TennisEdge Knowledge Base*\n\nSelect a topic:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 How Our AI Works", callback_data="kb_how")],
                [InlineKeyboardButton("📈 Understanding Edge & EV", callback_data="kb_edge")],
                [InlineKeyboardButton("💰 Kelly Criterion", callback_data="kb_kelly")],
                [InlineKeyboardButton("⬅ Back to Menu", callback_data="menu_home")],
            ]),
        )

    elif data == "menu_home":
        user = update.effective_user
        db_user = get_or_create_user(user.id, user.username)
        await query.edit_message_text(
            f"🎾 *TennisEdge Menu*\n"
            f"Credits: *{db_user['credits']}*\n"
            f"Choose an option:",
            parse_mode="Markdown",
            reply_markup=_main_menu_keyboard(),
        )


async def tour_sel_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tour = query.data.split("_")[2]  # atp or wta
    ctx.user_data["menu_tour"] = tour
    await query.edit_message_text(f"Loading {tour.upper()} matches...")

    loop = asyncio.get_running_loop()
    try:
        all_matches = await loop.run_in_executor(None, fetch_odds)
    except Exception:
        await query.edit_message_text(
            "Failed to fetch matches. Try again.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Home", callback_data="menu_home")]
            ]),
        )
        return

    filtered = [
        m for m in all_matches
        if tour in str(m.get("sport_key", "")).lower()
        or tour in str(m.get("tournament", "")).lower()
    ]
    if not filtered:
        filtered = all_matches
    ctx.user_data["menu_matches"] = filtered

    if not filtered:
        await query.edit_message_text(
            "📭 No matches available right now.\nCheck back later!",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("⬅ Back", callback_data="menu_predictions"),
                    InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
                ]
            ]),
        )
        return

    buttons = []
    for i, m in enumerate(filtered[:20]):
        label = f"🎾 {m['player_a'][:15]} vs {m['player_b'][:15]}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"match_sel_{i}")])
    buttons.append([
        InlineKeyboardButton("⬅ Back", callback_data="menu_predictions"),
        InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
    ])

    await query.edit_message_text(
        f"🎾 *{tour.upper()} Matches:*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def match_sel_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split("_")[2])
    menu_matches = ctx.user_data.get("menu_matches", [])

    if not menu_matches or idx >= len(menu_matches):
        await query.edit_message_text(
            "Match not found. Please go back and try again.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Home", callback_data="menu_home")]
            ]),
        )
        return

    m = menu_matches[idx]
    await query.edit_message_text("Analysing match...")

    try:
        from models.advanced_model import advanced_predict as model_predict
        surface = m.get("surface", "hard")
        model = model_predict(m["player_a"], m["player_b"], surface)
        
        # Determine bet player based on model probability
        if model.get("prob_a", 0.5) >= model.get("prob_b", 0.5):
            bet_player = m["player_a"]
        else:
            bet_player = m["player_b"]
            
        api_stats = get_player_stats(bet_player, surface)
        card = format_match_card(m, model, api_stats)
    except Exception as e:
        logger.error(f"match_sel_callback error: {e}")
        await query.edit_message_text(
            "Failed to load analysis. Try again.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Home", callback_data="menu_home")]
            ]),
        )
        return

    ctx.user_data["last_match_idx"] = idx
    await query.edit_message_text(
        card,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Full Analysis — 1 credit", callback_data=f"full_ana_{idx}")],
            [
                InlineKeyboardButton(
                    "⬅ Back",
                    callback_data=f"tour_sel_{ctx.user_data.get('menu_tour', 'atp')}",
                ),
                InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
            ],
        ]),
    )


async def full_ana_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split("_")[2])
    menu_matches = ctx.user_data.get("menu_matches", [])

    if not menu_matches or idx >= len(menu_matches):
        await query.edit_message_text(
            "Match not found. Go back and retry.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Home", callback_data="menu_home")]
            ]),
        )
        return

    m = menu_matches[idx]
    user = update.effective_user

    try:
        success = deduct_credit_atomic(user.id)
    except Exception as e:
        logger.error(f"Credit deduction error: {e}")
        await query.edit_message_text(
            "Could not process credits. Try again.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Home", callback_data="menu_home")]
            ]),
        )
        return

    if not success:
        await query.edit_message_text(
            "Not enough credits.\nUse /buy to get more credits.",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("💳 Buy Credits", callback_data="menu_buycredits"),
                    InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
                ]
            ]),
        )
        return

    await query.edit_message_text("Credit deducted. Loading full analysis...")

    try:
        from models.advanced_model import advanced_predict as model_predict
        from signals.formatter import _escape

        surface = m.get("surface", "hard")
        model = model_predict(m["player_a"], m["player_b"], surface)
        
        # Determine bet player and mapping
        if model.get("prob_a", 0.5) >= model.get("prob_b", 0.5):
            bet_player = m["player_a"]
            is_a = True
        else:
            bet_player = m["player_b"]
            is_a = False
            
        api_stats = get_player_stats(bet_player, surface)
        h2h = get_h2h_stats(m["player_a"], m["player_b"])
        form = get_current_tournament_form(bet_player)
        ranking = get_player_ranking(bet_player)
        card = format_match_card(m, model, api_stats)

        lines = [card, "", "📋 *Full Analysis:*"]
        
        # Flip factors if betting on Player B
        if is_a:
            p_elo   = model.get('elo_prob_a', 0.5)
            p_form  = model.get('form_prob_a', 0.5)
            p_surf  = model.get('surface_prob_a', 0.5)
            p_h2h   = model.get('h2h_prob_a', 0.5)
        else:
            p_elo   = 1.0 - model.get('elo_prob_a', 0.5)
            p_form  = 1.0 - model.get('form_prob_a', 0.5)
            p_surf  = 1.0 - model.get('surface_prob_a', 0.5)
            p_h2h   = 1.0 - model.get('h2h_prob_a', 0.5)

        lines += [
            f"Elo Rating (40%): {format_prob(p_elo)}",
            f"Recent Form (25%): {format_prob(p_form)}",
            f"Surface Stat (20%): {format_prob(p_surf)}",
            f"H2H Record (15%): {format_prob(p_h2h)}",
        ]

        if h2h:
            lines += [
                "",
                "🤝 *Head to Head:*",
                f"Overall: {_escape(str(h2h.get('h2h_record', 'N/A')))}",
                f"This surface: {_escape(str(h2h.get('surface_record', 'N/A')))}",
                f"Last 5: {_escape(str(h2h.get('last_5_meetings', 'N/A')))}",
            ]
        if form:
            lines += [
                "",
                "📅 *Tournament Form:*",
                f"Wins this event: {form.get('wins_this_tournament', 'N/A')}",
                f"Days since last match: {form.get('days_since_last_match', 'N/A')}",
            ]
        if ranking:
            rank = ranking.get("current_rank", "N/A")
            change = ranking.get("rank_change_this_week", 0)
            sign = f"+{change}" if isinstance(change, int) and change > 0 else str(change)
            lines += ["", f"🏅 Ranking: #{rank} ({sign} this week)"]

        full_text = "\n".join(lines)
        if len(full_text) > 4000:
            full_text = full_text[:3950] + "\n\n...truncated"
    except Exception as e:
        logger.error(f"full_ana_callback error: {e}")
        full_text = "Partial data only — some stats unavailable."

    await query.edit_message_text(
        full_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Home", callback_data="menu_home")]
        ]),
    )


async def vbets_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split("_")[1])
    signals = ctx.user_data.get("vbets_signals", [])
    top5 = sorted(
        signals,
        key=lambda x: x.get("true_edge_score", x.get("edge", 0)),
        reverse=True,
    )[:5]

    if idx >= len(top5):
        await query.edit_message_text(
            "Signal not found. Try refreshing.",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔄 Refresh", callback_data="menu_vbets"),
                    InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
                ]
            ]),
        )
        return

    s = top5[idx]
    surface = s.get("surface", "hard")
    
    # Construct match dict correctly for format_match_card
    # We must ensure prob_a/odds_a correspond to the same side.
    match = {
        "player_a": s.get("player_a", "Player A"),
        "player_b": s.get("player_b", "Player B"),
        "surface":  surface,
        "tournament": s.get("tournament", "Unknown"),
        "odds_a": s.get("odds", 0) if s.get("bet_on") == s.get("player_a") else 0,
        "odds_b": s.get("odds", 0) if s.get("bet_on") == s.get("player_b") else 0,
    }
    
    # Map the probabilities back to Player A/B for format_match_card
    is_bet_on_a = (s.get("bet_on") == s.get("player_a"))
    model = {
        "prob_a": s.get("model_prob") if is_bet_on_a else (1.0 - s.get("model_prob", 0.5)),
        "prob_b": s.get("model_prob") if not is_bet_on_a else (1.0 - s.get("model_prob", 0.5)),
        "true_edge_score": s.get("true_edge_score", s.get("edge", 0)),
    }
    
    api_stats = get_player_stats(s.get("bet_on", ""), surface)
    card = format_match_card(match, model, api_stats)
    await query.edit_message_text(
        card,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⬅ Back", callback_data="menu_vbets"),
                InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
            ]
        ]),
    )


async def tgames_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split("_")[1])
    tg_matches = ctx.user_data.get("tg_matches", [])

    if idx >= len(tg_matches):
        await query.edit_message_text(
            "Match not found. Go back and retry.",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("⬅ Back", callback_data="menu_tgames"),
                    InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
                ]
            ]),
        )
        return

    item = tg_matches[idx]
    m = item["match"]
    surface = m.get("surface", "hard")
    probs = item["probs"]
    over_pct = round(probs["over"] * 100, 0)
    under_pct = round(probs["under"] * 100, 0)

    if probs["over"] > 0.55:
        rec = "✅ Bet: Over 22.5 Games"
    elif probs["under"] > 0.55:
        rec = "✅ Bet: Under 22.5 Games"
    else:
        rec = "⚪ Skip: No clear edge"

    from signals.formatter import _escape
    text = (
        f"📊 *{_escape(m['player_a'])} vs {_escape(m['player_b'])}*\n\n"
        f"*Total Games Market:*\n"
        f"Over 22.5 Games: {int(over_pct)}%\n"
        f"Under 22.5 Games: {int(under_pct)}%\n\n"
        f"{rec}"
    )
    await query.edit_message_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⬅ Back", callback_data="menu_tgames"),
                InlineKeyboardButton("🏠 Home", callback_data="menu_home"),
            ]
        ]),
    )


async def kb_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    topic = query.data.split("_")[1]
    back_btn = InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back to Help", callback_data="menu_kb")]])

    if topic == "how":
        await query.edit_message_text(
            "🤖 *How Our AI Works*\n\n"
            "Our model combines 4 factors:\n"
            "• Elo Rating (40%) — surface-adjusted career form\n"
            "• Recent Form (25%) — last 10 matches decay-weighted\n"
            "• Surface Win Rate (20%) — last 2 years on this surface\n"
            "• H2H Record (15%) — head-to-head history\n\n"
            "Probabilities are calibrated using isotonic regression trained on historical ATP/WTA data.",
            parse_mode="Markdown",
            reply_markup=back_btn,
        )
    elif topic == "edge":
        await query.edit_message_text(
            "📈 *Understanding Edge & EV*\n\n"
            "Edge = (Model Prob × Odds) - 1\n"
            "Confidence = Model Prob / (1 - Model Prob)\n"
            "True Edge Score = Edge × Confidence\n\n"
            "Signals fire when thresholds are satisfied.\n"
            "A +10% EV means every 100 units staked expects about +10 units long term.",
            parse_mode="Markdown",
            reply_markup=back_btn,
        )
    elif topic == "kelly":
        await query.edit_message_text(
            "💰 *Bankroll & Kelly Stake*\n\n"
            "Quarter Kelly Formula:\n"
            "Stake% = (((prob × odds - 1) / (odds - 1)) × 0.25)\n\n"
            "This gives a bankroll fraction to risk while controlling variance.",
            parse_mode="Markdown",
            reply_markup=back_btn,
        )


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
        "When our AI model says a player has a 58% win chance, "
        "but the bookmaker's odds imply only 47%, that's an 11% edge — "
        "a potentially mispriced bet.\n\n"
        "*Model Architecture*\n"
        "Our logic combines 4 factors: Elo Ratings (40%), Recent Form (25%), "
        "Surface-specific win rates (20%), and Head-to-Head history (15%).\n\n"
        "*AI Predictions*\n"
        "Use /predict to get DeepSeek AI analysis on any upcoming match.\n\n"
        "*Signal Threshold*\n"
        "We send signals ONLY when Value Edge ≥ 4% AND the Model Win Probability ≥ 35%.\n\n"
        "*Paper Trading*\n"
        "The bot tracks every signal with a virtual 1-unit flat bet to validate "
        "the model's live ROI before any real money is suggested.\n\n"
        "*Community*\n"
        "Use /beta to join the free beta Telegram channel.\n\n"
        "*Credits*\n"
        "Each signal costs 1 credit. Use /buy to top up.\n\n"
        "*Commands*\n"
        "/start – Register\n"
        "/balance – Check credits\n"
        "/signals – Latest signals\n"
        "/matches – Upcoming matches & odds\n"
        "/predict – 🤖 AI predictions\n"
        "/portfolio – 📈 Track record\n"
        "/beta – Join channel\n"
        "/buy – Buy credits",
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


async def broadcastbeta(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: broadcast beta-channel invite to all registered users."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    if not BETA_CHANNEL_LINK:
        await update.message.reply_text(
            "❌ BETA_CHANNEL_LINK is not set. Add it in environment variables first."
        )
        return

    custom_text = " ".join(ctx.args).strip()
    if custom_text:
        msg = custom_text
    else:
        msg = (
            f"📢 *{BETA_CHANNEL_NAME}*\n\n"
            f"We've opened a free beta channel.\n"
            f"Join here: {BETA_CHANNEL_LINK}\n\n"
            f"Get daily updates, paper-trading progress, and early feature announcements."
        )

    users = get_all_user_telegram_ids()
    if not users:
        await update.message.reply_text("No users found to broadcast to yet.")
        return

    await update.message.reply_text(f"📨 Broadcasting beta invite to {len(users)} users...")
    sent = 0
    failed = 0
    for tid in users:
        try:
            await ctx.bot.send_message(chat_id=tid, text=msg, parse_mode="Markdown")
            sent += 1
        except Exception:
            failed += 1

    await update.message.reply_text(
        f"✅ Beta broadcast completed.\nSent: {sent}\nFailed: {failed}"
    )


async def backtest_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /backtest [atp_from] [atp_to] — run Pinnacle+Elo backtest."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return

    atp_from = int(ctx.args[0]) if len(ctx.args) > 0 else 2023
    atp_to   = int(ctx.args[1]) if len(ctx.args) > 1 else 2024

    await update.message.reply_text(
        f"📊 Running backtest ({atp_from}-{atp_to})...\n"
        f"Model: Pinnacle implied + Elo confirmation\n"
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
    app.add_handler(CommandHandler("beta",       beta))
    app.add_handler(CommandHandler("help",       help_cmd))
    app.add_handler(CommandHandler("menu",       menu_cmd))
    # Admin commands
    app.add_handler(CommandHandler("scan",       scan))
    app.add_handler(CommandHandler("addcredits", addcredits))
    app.add_handler(CommandHandler("broadcastbeta", broadcastbeta))
    app.add_handler(CommandHandler("backtest",   backtest_cmd))
    # Callbacks
    app.add_handler(CallbackQueryHandler(buy_callback, pattern="^buy_"))
    app.add_handler(CallbackQueryHandler(tournament_callback, pattern="^tourn_"))
    app.add_handler(CallbackQueryHandler(predict_callback, pattern="^pred_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_"))
    app.add_handler(CallbackQueryHandler(tour_sel_callback, pattern="^tour_sel_"))
    app.add_handler(CallbackQueryHandler(match_sel_callback, pattern="^match_sel_"))
    app.add_handler(CallbackQueryHandler(full_ana_callback, pattern="^full_ana_"))
    app.add_handler(CallbackQueryHandler(vbets_callback, pattern="^vbets_"))
    app.add_handler(CallbackQueryHandler(tgames_callback, pattern="^tgames_"))
    app.add_handler(CallbackQueryHandler(kb_callback, pattern="^kb_"))
    # Global error handler
    app.add_error_handler(error_handler)

    set_bot(app)
    start_scheduler()

    _startup_status = "running"
    print("🎾 TennisEdge bot is running...", flush=True)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
