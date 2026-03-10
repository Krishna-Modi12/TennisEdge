"""
signals/formatter.py
Formats signal dicts into Telegram-ready messages.

BUG FIX: Player names and tournament names can contain characters that break
Telegram Markdown parsing (underscores, asterisks, backticks, brackets).
e.g. "ATP 500 – s'Hertogenbosch" or player names with apostrophes.
All dynamic strings are now escaped before insertion into Markdown messages.
"""

SURFACE_EMOJI = {
    "clay":   "🟤",
    "hard":   "🔵",
    "grass":  "🟢",
    "carpet": "⬜",
}

SURFACE_LABEL = {
    "clay":   "Clay",
    "hard":   "Hard",
    "grass":  "Grass",
    "carpet": "Carpet",
}

# Characters that break Telegram Markdown v1 parsing
_MD_SPECIAL = ["_", "*", "`", "["]


def _escape(text: str) -> str:
    """Escape Telegram Markdown v1 special characters in dynamic strings."""
    for ch in _MD_SPECIAL:
        text = text.replace(ch, f"\\{ch}")
    return text


def format_signal(signal: dict) -> str:
    surface_emoji = SURFACE_EMOJI.get(signal["surface"], "🎾")
    surface_label = SURFACE_LABEL.get(signal["surface"], signal["surface"].title())

    model_pct  = round(signal["model_prob"]  * 100, 1)
    market_pct = round(signal["market_prob"] * 100, 1)
    edge_pct   = round(signal["edge"]        * 100, 1)

    # Escape all dynamic strings that go inside Markdown formatting
    tournament = _escape(signal["tournament"])
    player_a   = _escape(signal["player_a"])
    player_b   = _escape(signal["player_b"])
    bet_on     = _escape(signal["bet_on"])

    msg = (
        f"🎾 *TENNISEDGE SIGNAL*\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏆 *Tournament*\n"
        f"{tournament}\n\n"
        f"⚔️ *Match*\n"
        f"{player_a} vs {player_b}\n\n"
        f"{surface_emoji} *Surface:* {surface_label}\n\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ *Bet On:* {bet_on}\n"
        f"💰 *Odds:* {signal['odds']}\n\n"
        f"📊 *Model Probability:* {model_pct}%\n"
        f"📉 *Market Probability:* {market_pct}%\n\n"
        f"🔥 *EDGE: +{edge_pct}%*\n\n"
    )

    # Add model breakdown if advanced model data is present
    if signal.get("data_quality") and signal["data_quality"] != "elo_only":
        elo_p   = round(signal.get("elo_prob_a", 0.5) * 100, 1)
        form_p  = round(signal.get("form_prob_a", 0.5) * 100, 1)
        surf_p  = round(signal.get("surface_prob_a", 0.5) * 100, 1)
        h2h_p   = round(signal.get("h2h_prob_a", 0.5) * 100, 1)
        h2h_wa  = signal.get("h2h_wins_a", 0)
        h2h_wb  = signal.get("h2h_wins_b", 0)
        quality = signal["data_quality"].replace("_", " ").title()

        msg += (
            f"📐 *Model Breakdown ({quality}):*\n"
            f"  Elo Rating (40%): {elo_p}%\n"
            f"  Recent Form (25%): {form_p}%\n"
            f"  Surface WR (20%): {surf_p}%\n"
            f"  H2H (15%): {h2h_p}% ({h2h_wa}-{h2h_wb})\n\n"
        )

    msg += (
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"_1 credit deducted from your balance_"
    )
    return msg


def format_signal_list(signals: list) -> str:
    if not signals:
        return "📭 No recent signals found."

    lines = ["🎾 *Recent TennisEdge Signals*\n━━━━━━━━━━━━━━━━━━━\n"]
    for s in signals:
        edge_pct      = round(s["edge"] * 100, 1)
        surface_emoji = SURFACE_EMOJI.get(s["surface"], "🎾")
        ts            = s["created_at"].strftime("%d %b %H:%M") if hasattr(s["created_at"], "strftime") else ""
        player_a      = _escape(s["player_a"])
        player_b      = _escape(s["player_b"])
        bet_on        = _escape(s["bet_on"])
        lines.append(
            f"{surface_emoji} *{player_a} vs {player_b}*\n"
            f"✅ Bet: {bet_on} @ {s['odds']}\n"
            f"🔥 Edge: +{edge_pct}%  |  {ts}\n"
        )

    return "\n".join(lines)
