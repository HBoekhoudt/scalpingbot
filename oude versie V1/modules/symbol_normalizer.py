import logging

logger = logging.getLogger("ikbr_scalpingbot")


SYMBOL_MAP = {
    "FDAX1!": "FDXM",
    "FDAX": "FDXM",
    "MES1!": "MES",
    "MNQ1!": "MNQ",
    "M6E1!": "M6E",
}


def normalize_symbol(symbol: str) -> str:
    if symbol is None:
        return symbol

    normalized = symbol.upper().strip()

    if normalized == "EUR/USD":
        return "EURUSD"

    if normalized in SYMBOL_MAP:
        return SYMBOL_MAP[normalized]

    return normalized