import logging
import threading
import time

from modules.ib_watchdog import ensure_connection

logger = logging.getLogger("ikbr_scalpingbot")

# Track processed executions to avoid duplicates
seen_exec_ids = set()


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

                    exec_id = f.execution.execId

                    # Skip already processed executions
                    if exec_id in seen_exec_ids:
                        continue

                    seen_exec_ids.add(exec_id)

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