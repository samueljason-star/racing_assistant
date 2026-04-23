import re


def normalize_horse_name(name: str) -> str:
    """Normalize a horse name for cross-source matching."""
    normalized = name.lower().strip()
    normalized = re.sub(r"[^a-z0-9 ]+", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def horse_names_match(a: str, b: str) -> bool:
    """Return True when two horse names match after normalization."""
    return normalize_horse_name(a) == normalize_horse_name(b)
