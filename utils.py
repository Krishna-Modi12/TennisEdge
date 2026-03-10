"""
utils.py  –  Shared utility functions
"""

import re


def normalize_player_name(name: str) -> str:
    """
    Normalize a player name for consistent matching.
    - Strip whitespace
    - Title-case
    - Collapse multiple spaces
    """
    if not name:
        return ""
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = name.title()
    return name
