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

    # Dual validation metrics
    value_edge_pct = round(signal.get("value_edge", signal.get("edge", 0)) * 100, 1)
    confidence     = signal.get("confidence", 0)
    true_edge_pct  = round(signal.get("true_edge_score", signal.get("edge", 0)) * 100, 1)

    # Escape all dynamic strings that go inside Markdown formatting
    tournament = _escape(signal["tournament"])
    player_a   = _escape(signal["player_a"])
    player_b   = _escape(signal["player_b"])
    bet_on     = _escape(signal["bet_on"])

    ev_score = signal.get("ev_score", 0)
    if ev_score >= 0.06:
        tier_label = "🔥 *Strong Value Bet*"
    elif ev_score >= 0.04:
        tier_label = "⚡️ *Medium Value Bet*"
    else:
        tier_label = "📈 *Small Value Bet*"

    msg = (
        f"{tier_label}\n"
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
        f"📊 *Model Prob:* {model_pct}%\n"
        f"📉 *Market Prob:* {market_pct}%\n\n"
    )

    if signal.get("model_prob") is not None and (
        signal["model_prob"] > 0.90 or signal["model_prob"] < 0.10
    ):
        msg += "⚠️ \\[Suspicious Model Probability\\]\n\n"

    msg += (
        f"💎 *Value Edge:* +{value_edge_pct}%\n"
        f"🧠 *Confidence:* {confidence:.2f}x\n"
        f"🔥 *True Edge Score: +{true_edge_pct}%*\n"
        f"🎯 *EV Score: {ev_score:.3f}*\n\n"
    )

    bet_fraction = signal.get("recommended_bet_fraction")
    bet_size = signal.get("recommended_bet_size")
    bankroll = signal.get("bankroll_assumed")
    if bet_fraction is not None:
        frac_pct = round(float(bet_fraction) * 100, 2)
        if bet_size is not None and bankroll is not None:
            msg += (
                f"💰 *Half-Kelly Stake:* {frac_pct}% "
                f"(≈ {round(float(bet_size), 2)} units on {round(float(bankroll), 2)} bankroll)\n\n"
            )
        else:
            msg += f"💰 *Half-Kelly Stake:* {frac_pct}%\n\n"

    # Add 4-factor model breakdown if available
    if signal.get("data_quality") and signal["data_quality"] != "elo_only":
        elo_p   = round(signal.get("elo_prob", 0.5) * 100, 1)
        form_p  = round(signal.get("form_prob", 0.5) * 100, 1)
        surf_p  = round(signal.get("surface_prob", 0.5) * 100, 1)
        h2h_p   = round(signal.get("h2h_prob", 0.5) * 100, 1)
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



def format_signal_with_ai(signal: dict, ai_text: str) -> str:
    """Format a signal with AI analysis appended. Falls back to plain signal if no AI text."""
    msg = format_signal(signal)
    if ai_text:
        escaped_ai = _escape(ai_text)
        msg = msg.replace(
            "━━━━━━━━━━━━━━━━━━━\n_1 credit deducted from your balance_",
            f"🤖 *AI Analysis:*\n_{escaped_ai}_\n\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"_1 credit deducted from your balance_"
        )
    return msg


def format_signal_list(signals: list) -> str:
    if not signals:
        return "📭 No recent signals found."

    lines = ["🎾 *Recent TennisEdge Signals*\n━━━━━━━━━━━━━━━━━━━\n"]
    for s in signals:
        true_edge = round(s.get("true_edge_score", s.get("edge", 0)) * 100, 1)
        surface_emoji = SURFACE_EMOJI.get(s["surface"], "🎾")
        ts            = s["created_at"].strftime("%d %b %H:%M") if hasattr(s["created_at"], "strftime") else ""
        player_a      = _escape(s["player_a"])
        player_b      = _escape(s["player_b"])
        bet_on        = _escape(s["bet_on"])
        lines.append(
            f"{surface_emoji} *{player_a} vs {player_b}*\n"
            f"✅ Bet: {bet_on} @ {s['odds']}\n"
            f"🔥 True Edge: +{true_edge}%  |  {ts}\n"
        )

    return "\n".join(lines)


def format_match_card(match: dict, model: dict, api_stats: dict = None) -> str:
    """
    match keys: player_a, player_b, surface, tournament, odds_a, odds_b
    model keys: prob_a, prob_b, true_edge_score (or edge), elo_prob_a,
                form_prob_a, surface_prob_a, h2h_prob_a
    api_stats: from get_player_stats(), may be None
    """
    from models.match_helpers import fair_odds, kelly_stake, confidence_tier, format_prob

    # Get bet-on player (higher probability)
    if model.get("prob_a", 0.5) >= model.get("prob_b", 0.5):
        bet_player = match["player_a"]
        bet_prob = model.get("prob_a", 0.5)
        bet_odds = match.get("odds_a", 0)
    else:
        bet_player = match["player_b"]
        bet_prob = model.get("prob_b", 0.5)
        bet_odds = match.get("odds_b", 0)

    true_edge = model.get("true_edge_score", model.get("edge", 0))
    fo = fair_odds(bet_prob)
    kelly = kelly_stake(bet_prob, bet_odds)
    tier = confidence_tier(true_edge, bet_prob)
    edge_pct = round(true_edge * 100, 1)

    lines = [
        f"🎾 *{_escape(match['player_a'])} vs {_escape(match['player_b'])}*",
        f"🏟 {_escape(match.get('tournament', ''))} | {_escape(match.get('surface', '').title())}",
        "",
        "📊 *Stats Breakdown:*",
        f"Bet On: {_escape(str(bet_player))}",
        f"Our Probability: {format_prob(bet_prob)}",
        f"Market Odds: {_escape(str(bet_odds))}",
        f"Fair Odds: {_escape(str(fo)) if fo else 'N/A'}",
        f"Profit Potential: +{_escape(str(edge_pct))}%",
        f"Confidence: {tier}",
        "💰 *Bankroll Strategy:*",
        f"Recommended Stake: {_escape(str(kelly))}% (Quarter Kelly)",
    ]

    # Add API stats only if available
    if api_stats:
        lines.append("📡 *Serve Stats:*")
        fields = [
            ("1st Serve Won", api_stats.get("first_serve_won_pct")),
            ("2nd Serve Won", api_stats.get("second_serve_won_pct")),
            ("BP Saved", api_stats.get("break_points_saved_pct")),
            ("Return Won", api_stats.get("return_points_won_pct")),
        ]
        for label, val in fields:
            if val is not None:
                lines.append(f"{label}: {format_prob(val)}")

    return "\n".join(lines)
