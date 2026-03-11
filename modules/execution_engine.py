import logging
import asyncio

from modules.ib_watchdog import ensure_connection
from modules.execution_queue import execution_queue
from modules.order_id_manager import get_next_order_id
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
        logger.info(f"Worker received job: {job}")

        try:

            ib = ensure_connection()

            symbol = job["symbol"]
            side = job["side"]
            entry = job["entry"]
            stop = job["stop"]
            target = job["target"]
            qty = job["qty"]
            contract = job["contract"]

            # Ensure IBKR contract is fully qualified
            contract = ib.qualifyContracts(contract)[0]

            logger.info(
                f"EXECUTION -> {symbol} {side.upper()} Entry {entry} Stop {stop} Target {target} Qty {qty}"
            )

            bracket = ib.bracketOrder(
                action="BUY" if side == "long" else "SELL",
                quantity=qty,
                limitPrice=entry,
                takeProfitPrice=target,
                stopLossPrice=stop
            )

            # Controlled order IDs
            base_order_id = get_next_order_id()

            bracket[0].orderId = base_order_id
            bracket[1].orderId = base_order_id + 1
            bracket[2].orderId = base_order_id + 2

            for order in bracket:
                ib.placeOrder(contract, order)

            trade_state.set_state("IN_TRADE")

            logger.info("Bracket order sent")

        except Exception as e:

            logger.exception("Execution error")
            trade_state.set_state("IDLE")

        finally:

            execution_queue.task_done()