"""Shared formatting utilities for slopbox tools."""


# ---------------------------------------------------------------------------
# Byte and duration formatting
# ---------------------------------------------------------------------------

def format_bytes(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def format_duration(days: float) -> str:
    if days < 0:
        return "unknown"
    if days < 1:
        hours = int(days * 24)
        mins = int((days * 24 - hours) * 60)
        if hours == 0:
            return f"{mins}m"
        return f"{hours}h {mins}m"
    d = int(days)
    h = int((days - d) * 24)
    return f"{d}d {h}h" if h else f"{d}d"


# ---------------------------------------------------------------------------
# Rich style helpers
# ---------------------------------------------------------------------------

def phase_style(phase: str) -> str:
    return {
        "hot": "yellow",
        "warm": "blue",
        "cold": "cyan",
        "frozen": "magenta",
        "delete": "red",
    }.get(phase, "white")


def health_style(health: str) -> str:
    return {
        "green": "green",
        "yellow": "yellow",
        "red": "red",
    }.get(health, "white")
