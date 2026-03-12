import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_ID  = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))

ODDS_API_KEY  = os.getenv("ODDS_API_KEY", "")
RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "tennis-api-atp-wta-itf.p.rapidapi.com")
HF_TOKEN      = os.getenv("HF_TOKEN", "")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost:5432/tennisedge"
)

UPI_ID = os.getenv("UPI_ID", "")

# Free beta community channel
BETA_CHANNEL_LINK = os.getenv("BETA_CHANNEL_LINK", "")
BETA_CHANNEL_NAME = os.getenv("BETA_CHANNEL_NAME", "TennisEdge Beta")

EDGE_THRESHOLD = 0.07
MIN_ODDS       = 1.40
MAX_ODDS       = 6.00

# Dual validation thresholds
MIN_VALUE_EDGE  = 0.04   # 4% value edge minimum
MIN_MODEL_PROB  = 0.35   # model must give ≥35% win probability

CREDITS_PER_SIGNAL = 1
DEFAULT_BANKROLL = float(os.getenv("DEFAULT_BANKROLL", "1000"))

CREDIT_PACKAGES = {
    "starter": {"credits": 10,  "price_inr": 199},
    "pro":     {"credits": 50,  "price_inr": 799},
    "vip":     {"credits": 200, "price_inr": 2499},
}

POLL_INTERVAL_MINUTES = 30

ELO_DEFAULT_RATING = 1500.0
ELO_K_FACTOR       = 32
SURFACE_ELO_WEIGHT = 0.60

# Tournament-name based fallback mapping for feeds that omit/garble surface.
SURFACE_MAPPING = {
    # Required canonical mappings
    "Roland Garros": "clay",
    "French Open": "clay",
    "Wimbledon": "grass",
    "Monte Carlo": "clay",
    "Madrid Open": "clay",
    "Madrid": "clay",

    # Additional major events
    "Australian Open": "hard",
    "US Open": "hard",
    "Indian Wells": "hard",
    "Miami": "hard",
    "Cincinnati": "hard",
    "Shanghai": "hard",
    "Paris": "hard",
    "Bercy": "hard",
    "Doha": "hard",
    "Dubai": "hard",
    "Rotterdam": "hard",
    "Tokyo": "hard",
    "Beijing": "hard",
    "Basel": "hard",
    "Vienna": "hard",
    "Montreal": "hard",
    "Toronto": "hard",
    "Wuhan": "hard",
    "Guadalajara": "hard",
    "Ostrava": "hard",
    "Rome": "clay",
    "Barcelona": "clay",
    "Hamburg": "clay",
    "Lyon": "clay",
    "Geneva": "clay",
    "Bastad": "clay",
    "Gstaad": "clay",
    "Kitzbuhel": "clay",
    "Halle": "grass",
    "Queen's": "grass",
    "Queens": "grass",
    "Eastbourne": "grass",
    "Mallorca": "grass",
    "s-Hertogenbosch": "grass",
    "s'Hertogenbosch": "grass",
}

DEFAULT_SURFACE = "hard"


def normalize_tournament_name(name: str) -> str:
    """Normalize tournament names to improve resilient substring matching."""
    if not name:
        return ""
    return " ".join(str(name).lower().replace("-", " ").strip().split())


def get_surface(tournament_name: str) -> str:
    """Resolve likely surface from tournament name with safe default fallback."""
    if not tournament_name:
        return DEFAULT_SURFACE

    normalized = normalize_tournament_name(tournament_name)
    for key, surface in SURFACE_MAPPING.items():
        if normalize_tournament_name(key) in normalized:
            return surface

    return DEFAULT_SURFACE

# Set MOCK_MODE=true in .env to run without real API keys
MOCK_MODE = os.getenv("MOCK_MODE", "true").lower() == "true"
