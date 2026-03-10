import logging
import threading
import time

from modules.ib_watchdog import ensure_connection

logger = logging.getLogger("ikbr_scalpingbot")


def monitor_trade():
    """
    Background monitor that logs fills and basic trade events.
    """

    ib = ensure_connection()

    def worker():

        logger.info("Fill tracker started")

        while True:

            try:

                fills = ib.fills()

                for f in fills:

                    contract = f.contract.symbol
                    side = f.execution.side
                    price = f.execution.price
                    qty = f.execution.shares

                    logger.info(
                        f"FILL -> {contract} {side} {qty} @ {price}"
                    )

            except Exception as e:

                logger.error(f"Fill tracker error: {e}")

            time.sleep(2)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()