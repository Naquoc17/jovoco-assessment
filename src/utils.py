from datetime import datetime


def normalize_name(name: str) -> str:
    """Normalize customer names for matching."""
    parts = name.strip().split()
    return " ".join(sorted(part.lower() for part in parts))


def parse_date(date_str: str) -> str | None:
    """Parse supported date formats into YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None
