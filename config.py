import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_ID  = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))

ODDS_API_KEY  = os.getenv("ODDS_API_KEY", "")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost:5432/tennisedge"
)

UPI_ID = os.getenv("UPI_ID", "")

EDGE_THRESHOLD = 0.07
MIN_ODDS       = 1.40
MAX_ODDS       = 6.00

CREDITS_PER_SIGNAL = 1

CREDIT_PACKAGES = {
    "starter": {"credits": 10,  "price_inr": 199},
    "pro":     {"credits": 50,  "price_inr": 799},
    "vip":     {"credits": 200, "price_inr": 2499},
}

POLL_INTERVAL_MINUTES = 30

ELO_DEFAULT_RATING = 1500.0
ELO_K_FACTOR       = 32
SURFACE_ELO_WEIGHT = 0.60

# Set MOCK_MODE=true in .env to run without real API keys
MOCK_MODE = os.getenv("MOCK_MODE", "true").lower() == "true"
