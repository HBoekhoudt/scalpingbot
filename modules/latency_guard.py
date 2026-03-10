import logging
from modules.ib_watchdog import ensure_connection
from ib_insync import Future

logger = logging.getLogger("ikbr_scalpingbot")

# maximaal toegestane afwijking (ticks)
MAX_ENTRY_SLIPPAGE_TICKS = 3


def is_entry_valid(symbol, entry_price, tick_size):

    try:

        ib = ensure_connection()

        if symbol == "FDXM":
            exchange = "EUREX"
        else:
            exchange = "GLOBEX"

        contract = Future(
            symbol=symbol,
            exchange=exchange
        )

        ticker = ib.reqMktData(contract, "", False, False)

        ib.sleep(1)

        market_price = ticker.last

        if market_price is None:

            logger.warning("No market price available")

            return True

        diff = abs(market_price - entry_price)

        max_slippage = MAX_ENTRY_SLIPPAGE_TICKS * tick_size

        if diff > max_slippage:

            logger.warning(
                f"Latency guard blocked trade "
                f"(market {market_price} vs entry {entry_price})"
            )

            return False

        return True

    except Exception as e:

        logger.error(f"Latency guard error: {e}")

        return True