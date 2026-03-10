import time
import logging

logger = logging.getLogger("ikbr_scalpingbot")

# opslag voor laatste trades
last_trades = {}

# lock duur (seconden)
TRADE_LOCK_SECONDS = 30


def is_duplicate(symbol, side, entry):

    key = f"{symbol}_{side}"

    now = time.time()

    if key in last_trades:

        last_entry, last_time = last_trades[key]

        if abs(entry - last_entry) < 0.0001 and now - last_time < TRADE_LOCK_SECONDS:

            logger.warning(f"Duplicate trade blocked {symbol}")

            return True

    return False


def lock_trade(symbol):

    now = time.time()

    last_trades[symbol] = (0, now)


def is_trade_locked(symbol):

    now = time.time()

    if symbol in last_trades:

        _, last_time = last_trades[symbol]

        if now - last_time < TRADE_LOCK_SECONDS:

            return True

    return False