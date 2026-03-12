import time
import logging
import threading

logger = logging.getLogger("ikbr_scalpingbot")

# Duplicate window in seconds
DUPLICATE_WINDOW = 30

# In-memory signal storage
_recent_signals = {}

# Thread safety lock
_lock = threading.Lock()


def _cleanup_old_signals():
    """
    Remove signals older than the duplicate window.
    """
    now = time.time()

    expired_keys = [
        key for key, ts in _recent_signals.items()
        if now - ts > DUPLICATE_WINDOW
    ]

    for key in expired_keys:
        del _recent_signals[key]


def is_duplicate_signal(symbol, side, entry_price) -> bool:
    """
    Detect duplicate TradingView signals based on
    symbol, side and entry_price.
    """

    key = f"{symbol}:{side}:{round(float(entry_price), 5)}"
    now = time.time()

    with _lock:

        _cleanup_old_signals()

        if key in _recent_signals:
            logger.warning(f"Duplicate signal blocked: {key}")
            return True

        _recent_signals[key] = now
        logger.info(f"Signal accepted: {key}")

    return False