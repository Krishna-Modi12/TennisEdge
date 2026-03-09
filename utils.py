"""
utils.py  –  Shared utilities for player name normalization.
"""

import re


def normalize_player_name(name: str) -> str:
    """
    Normalize a player name for consistent DB matching.

    Handles:
      - Extra whitespace
      - "Last, First" format (some data sources)
      - Title casing with Mc/O' fixes
      - Unicode accent stripping is NOT applied (keeps original diacritics)
    """
    if not name:
        return ""
    name = " ".join(name.split())
    # Handle "Last, First" format → "First Last"
    if "," in name and name.count(",") == 1:
        parts = name.split(",", 1)
        name = f"{parts[1].strip()} {parts[0].strip()}"
    name = name.strip().title()
    # Fix common title-case issues
    name = re.sub(r"\bMc(\w)", lambda m: f"Mc{m.group(1).upper()}", name)
    name = re.sub(r"\bO'(\w)", lambda m: f"O'{m.group(1).upper()}", name)
    name = re.sub(r"\bDe(\s)", r"de\1", name)
    name = re.sub(r"\bVan(\s)", r"van\1", name)
    name = re.sub(r"\bDel(\s)", r"del\1", name)
    return name


def to_db_format(name: str) -> str:
    """
    Convert API name format to DB format used by historical data.

    "C. Gauff"       → "Gauff C."
    "N. Djokovic"    → "Djokovic N."
    "Carlos Alcaraz" → "Alcaraz C."
    "Gauff C."       → "Gauff C."  (already in DB format)
    """
    name = normalize_player_name(name)
    if not name:
        return name

    parts = name.split()
    if len(parts) < 2:
        return name

    # Already in "Last F." format (ends with letter + period)
    if re.match(r'^[A-Z]\.$', parts[-1]):
        return name

    # "F. Last" format (first part is initial)
    if re.match(r'^[A-Z]\.$', parts[0]):
        last = " ".join(parts[1:])
        return f"{last} {parts[0]}"

    # "First Last" format (full first name)
    first_initial = parts[0][0] + "."
    last = " ".join(parts[1:])
    return f"{last} {first_initial}"


def resolve_player_name(name: str) -> str:
    """
    Try to resolve a player name to its DB format.
    Attempts multiple formats and checks the player_elo table.
    Falls back to to_db_format() if no DB match found.
    """
    from database.db import get_elo
    from config import ELO_DEFAULT_RATING

    normalized = normalize_player_name(name)
    db_fmt = to_db_format(name)

    # Try DB format first (most historical data)
    elo = get_elo(db_fmt, "overall")
    if elo != ELO_DEFAULT_RATING:
        return db_fmt

    # Try normalized (seeded players like "Carlos Alcaraz")
    elo = get_elo(normalized, "overall")
    if elo != ELO_DEFAULT_RATING:
        return normalized

    # Default to DB format
    return db_fmt
