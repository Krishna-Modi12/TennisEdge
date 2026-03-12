"""
utils.py  –  Shared utility functions
"""

import re


def normalize_player_name(name: str) -> str:
    """
    Normalize a player name for consistent matching.
    - Strip whitespace
    - Title-case (unless already in standard format)
    - Collapse multiple spaces
    - Swap "Djokovic N." → "N. Djokovic"
    """
    if not name:
        return ""
    name = name.strip()
    name = name.replace("-", " ")
    name = name.replace(",", " ")
    name = re.sub(r"\s+", " ", name)
    
    # Don't title case if it's already in a standard format with dots
    if not re.match(r"^[A-Z]\.\s+[A-Z][a-z]+", name):
        name = name.title()
    
    # Swap "Djokovic N." or "Djokovic N" to "N. Djokovic"
    match = re.match(r"^(.+?)\s+([A-Z])\.?$", name)
    if match:
        name = f"{match.group(2)}. {match.group(1)}"
        
    return name.strip()


def extract_last_name(name: str) -> str:
    """Extract the likely last name from a player name string.
    'N. Djokovic' → 'Djokovic'
    'Novak Djokovic' → 'Djokovic'
    'Alex De Minaur' → 'De Minaur'
    'Djokovic N.' → 'Djokovic'
    """
    name = name.strip()
    # "N. Djokovic" or "N Djokovic" pattern
    m = re.match(r"^[A-Z]\.?\s+(.+)$", name)
    if m:
        return m.group(1).strip()
    # "Djokovic N." pattern
    m = re.match(r"^(.+?)\s+[A-Z]\.$", name)
    if m:
        return m.group(1).strip()
    # "Novak Djokovic" → last word(s) after first
    parts = name.split()
    if len(parts) >= 2:
        return " ".join(parts[1:])
    return name


def get_name_variants(name: str) -> list:
    """
    Generate possible name variants for database searching.
    Handles many common tennis API name formats:
      - "N. Djokovic" → ["N. Djokovic", "Djokovic N.", "Novak Djokovic" won't be guessed]
      - "Novak Djokovic" → ["Novak Djokovic", "N. Djokovic", "Djokovic N."]
      - "Alex De Minaur" → ["Alex De Minaur", "A. De Minaur", "De Minaur A."]
      - "Djokovic Novak" → ["Novak Djokovic", "N. Djokovic", "Djokovic N."]
    """
    name = normalize_player_name(name)
    variants = [name]
    
    # Pattern: "N. Djokovic" → also try "Djokovic N."
    m_init = re.match(r"^([A-Z])\.\s+(.+)$", name)
    if m_init:
        initial, rest = m_init.group(1), m_init.group(2)
        v = f"{rest} {initial}."
        if v not in variants:
            variants.append(v)
        # Also try without dot: "N Djokovic"
        v2 = f"{initial} {rest}"
        if v2 not in variants:
            variants.append(v2)
        return variants
    
    # Pattern: "Novak Djokovic" (or multi-word last: "Alex De Minaur")
    m_full = re.match(r"^([A-Z][a-z]+)\s+(.+)$", name)
    if m_full:
        first, last = m_full.group(1), m_full.group(2)
        initial = first[0]
        # "N. Djokovic"
        v1 = f"{initial}. {last}"
        if v1 not in variants:
            variants.append(v1)
        # "Djokovic N."
        v2 = f"{last} {initial}."
        if v2 not in variants:
            variants.append(v2)
        # "Djokovic, Novak" (some DB use this)
        v3 = f"{last}, {first}"
        if v3 not in variants:
            variants.append(v3)
        return variants
    
    return variants
