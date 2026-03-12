import logging
import asyncio

from modules.ib_watchdog import ensure_connection
from modules.execution_queue import execution_queue
import modules.trade_state as trade_state


logger = logging.getLogger("ikbr_scalpingbot")


# ==========================================
# EXECUTION WORKER
# ==========================================

def execution_worker():

    # Create event loop for this thread (required by ib_insync)
    asyncio.set_event_loop(asyncio.new_event_loop())

    logger.info("Execution queue worker started")

    while True:

        job = execution_queue.get()

        # Step A
        logger.info("DEBUG A — Worker pulled job from queue")
        logger.info(f"Worker received job: {job}")

        try:

            # Step B
            logger.info("DEBUG B — Before ensure_connection()")

            ib = ensure_connection()

            # Step C
            logger.info("DEBUG C — After ensure_connection()")

            symbol = job["symbol"]
            side = job["side"]
            entry = job["entry"]
            stop = job["stop"]
            target = job["target"]
            qty = job["qty"]
            contract = job["contract"]

            logger.info(
                f"EXECUTION -> {symbol} {side.upper()} Entry {entry} Stop {stop} Target {target} Qty {qty}"
            )

            # Step F
            logger.info("DEBUG F — Before bracketOrder creation")

            bracket = ib.bracketOrder(
                action="BUY" if side == "long" else "SELL",
                quantity=qty,
                limitPrice=entry,
                takeProfitPrice=target,
                stopLossPrice=stop
            )

            # Step G
            logger.info("DEBUG G — Before sending orders to IBKR")

            for order in bracket:
                ib.placeOrder(contract, order)

            # Step H
            logger.info("DEBUG H — After orders sent")

            trade_state.set_state("IN_TRADE")

            logger.info("Bracket order sent")

        except Exception as e:

            logger.exception("Execution error")
            trade_state.set_state("IDLE")

        finally:

            execution_queue.task_done()