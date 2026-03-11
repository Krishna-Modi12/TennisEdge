"""
ai/analyzer.py
Generate 1-2 sentence AI match analysis using DeepSeek via HuggingFace.

Requires HF_TOKEN environment variable. Fails gracefully if unavailable.

Usage:
    from ai.analyzer import generate_match_analysis
    text = await generate_match_analysis("Sinner", "Alcaraz", "hard", 0.58, 2.40, 0.12)
"""

import os
import asyncio
import logging
from functools import lru_cache
from config import HF_TOKEN

logger = logging.getLogger(__name__)

MODEL = "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B:fastest"
HF_BASE_URL = "https://router.huggingface.co/v1"
TIMEOUT_SECONDS = 30
MAX_TOKENS = 150


_client_cache = None  # None = not initialized, False = no token/failed


def _get_client():
    """Lazy-init the OpenAI client. Returns None if HF_TOKEN not set."""
    global _client_cache
    if _client_cache is not None:
        return _client_cache if _client_cache else None

    token = HF_TOKEN
    if not token:
        logger.info("[AI] HF_TOKEN not set — AI analysis disabled.")
        _client_cache = False
        return None
    try:
        from openai import OpenAI
        _client_cache = OpenAI(base_url=HF_BASE_URL, api_key=token)
        return _client_cache
    except ImportError:
        logger.warning("[AI] openai package not installed — AI analysis disabled.")
        _client_cache = False
        return None
    except Exception as e:
        logger.warning(f"[AI] Failed to init client: {e}")
        _client_cache = False
        return None


def _build_prompt(player: str, opponent: str, surface: str,
                  model_prob: float, odds: float, value_edge: float,
                  data_quality: str = "unknown",
                  elo_prob: float = None, form_prob: float = None,
                  surface_prob: float = None, h2h_prob: float = None) -> str:
    """Build a concise analysis prompt."""
    edge_pct = round(value_edge * 100, 1)
    prob_pct = round(model_prob * 100, 1)

    breakdown = ""
    if data_quality != "elo_only":
        parts = []
        if elo_prob is not None:
            parts.append(f"Elo: {round(elo_prob*100,1)}%")
        if form_prob is not None:
            parts.append(f"Form: {round(form_prob*100,1)}%")
        if surface_prob is not None:
            parts.append(f"Surface: {round(surface_prob*100,1)}%")
        if h2h_prob is not None:
            parts.append(f"H2H: {round(h2h_prob*100,1)}%")
        if parts:
            breakdown = f"\nModel factors: {', '.join(parts)}"

    return (
        f"You are a tennis betting analyst. Write exactly 1-2 sentences explaining "
        f"why this bet has value. Be specific and confident.\n\n"
        f"Match: {player} vs {opponent} on {surface}\n"
        f"Our model gives {player} a {prob_pct}% win probability.\n"
        f"Bookmaker odds: {odds} (implied {round(100/odds, 1) if odds > 0 else 'N/A'}%)\n"
        f"Value edge: +{edge_pct}%{breakdown}\n\n"
        f"Write a brief, punchy rationale (1-2 sentences only, no intro phrases):"
    )


def _sync_generate(prompt: str) -> str:
    """Synchronous API call to DeepSeek."""
    client = _get_client()
    if client is None:
        return ""

    try:
        completion = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=MAX_TOKENS,
            temperature=0.7,
        )
        text = completion.choices[0].message.content.strip()
        # Clean up any thinking tags from R1 models
        if "<think>" in text:
            # Remove <think>...</think> blocks
            import re
            text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
        return text
    except Exception as e:
        logger.warning(f"[AI] DeepSeek API error: {e}")
        return ""


async def generate_match_analysis(
    player: str, opponent: str, surface: str,
    model_prob: float, odds: float, value_edge: float,
    data_quality: str = "unknown",
    elo_prob: float = None, form_prob: float = None,
    surface_prob: float = None, h2h_prob: float = None,
) -> str:
    """
    Generate AI analysis for a match signal. Returns empty string on failure.

    This is async-safe: runs the blocking API call in a thread executor.
    """
    if _get_client() is None:
        return ""

    prompt = _build_prompt(
        player, opponent, surface, model_prob, odds, value_edge,
        data_quality, elo_prob, form_prob, surface_prob, h2h_prob,
    )

    try:
        loop = asyncio.get_running_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(None, _sync_generate, prompt),
            timeout=TIMEOUT_SECONDS,
        )
        return result
    except asyncio.TimeoutError:
        logger.warning("[AI] DeepSeek request timed out.")
        return ""
    except Exception as e:
        logger.warning(f"[AI] Async generation failed: {e}")
        return ""


def generate_match_analysis_sync(
    player: str, opponent: str, surface: str,
    model_prob: float, odds: float, value_edge: float,
    **kwargs,
) -> str:
    """Synchronous version for non-async contexts."""
    if _get_client() is None:
        return ""

    prompt = _build_prompt(
        player, opponent, surface, model_prob, odds, value_edge,
        **kwargs,
    )
    return _sync_generate(prompt)
