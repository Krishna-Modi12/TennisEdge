"""
signals/formatter.py
Formats signal dicts into Telegram-ready messages.
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


def format_signal(signal: dict) -> str:
    surface_emoji = SURFACE_EMOJI.get(signal["surface"], "🎾")
    surface_label = SURFACE_LABEL.get(signal["surface"], signal["surface"].title())

    model_pct  = round(signal["model_prob"]  * 100, 1)
    market_pct = round(signal["market_prob"] * 100, 1)
    edge_pct   = round(signal["edge"]        * 100, 1)
    
    # Fetch additional stats
    try:
        from database.db import get_conn
        from models.model_components import h2h_edge, surface_winrate, recent_form, NEUTRAL
        
        with get_conn() as conn:
            p_a, p_b, sfc = signal['player_a'], signal['player_b'], signal['surface']
            bet = signal['bet_on']
            opp = p_b if bet == p_a else p_a
            
            # H2H winrate
            h2h = h2h_edge(bet, opp, sfc, conn)
            h2h_str = "No Data" if h2h == NEUTRAL else f"{round(h2h * 100)}%"
            
            # Surface Winrate (2 years)
            swr = surface_winrate(bet, sfc, conn)
            swr_str = "No Data" if swr == NEUTRAL else f"{round(swr * 100)}%"
            
            # Recent Form
            form = recent_form(bet, sfc, conn)
            form_str = "No Data" if form == NEUTRAL else f"{round(form * 100)}%"
            
            stats_block = (
                f"📈 *{bet} Stats:*\n"
                f"• Form (L10): {form_str}\n"
                f"• {surface_label} Win% (2yr): {swr_str}\n"
                f"• H2H on {surface_label}: {h2h_str}\n"
            )
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Error fetching stats for formatting: {e}")
        stats_block = ""

    msg = (
        f"🎾 *TENNISEDGE SIGNAL*\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏆 *Tournament*\n"
        f"{signal['tournament']}\n\n"
        f"⚔️ *Match*\n"
        f"{signal['player_a']} vs {signal['player_b']}\n\n"
        f"{surface_emoji} *Surface:* {surface_label}\n\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ *Bet On:* {signal['bet_on']}\n"
        f"💰 *Odds:* {signal['odds']}\n\n"
        f"{stats_block}\n"
        f"📊 *Model Probability:* {model_pct}%\n"
        f"📉 *Market Probability:* {market_pct}%\n\n"
        f"🔥 *EDGE: +{edge_pct}%*\n\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"_1 credit deducted from your balance_"
    )
    # Remove any double newlines caused by empty stats_block
    msg = msg.replace("\n\n\n", "\n\n")
    return msg


def format_signal_list(signals: list) -> str:
    if not signals:
        return "📭 No recent signals found."

    lines = ["🎾 *Recent TennisEdge Signals*\n━━━━━━━━━━━━━━━━━━━\n"]
    for s in signals:
        edge_pct = round(s["edge"] * 100, 1)
        surface_emoji = SURFACE_EMOJI.get(s["surface"], "🎾")
        ts = s["created_at"].strftime("%d %b %H:%M") if hasattr(s["created_at"], "strftime") else ""
        lines.append(
            f"{surface_emoji} *{s['player_a']} vs {s['player_b']}*\n"
            f"✅ Bet: {s['bet_on']} @ {s['odds']}\n"
            f"🔥 Edge: +{edge_pct}%  |  {ts}\n"
        )

    return "\n".join(lines)
