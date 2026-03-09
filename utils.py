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
