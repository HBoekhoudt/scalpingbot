import logging
import threading

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from modules.ib_watchdog import ensure_connection, is_ib_connected, watchdog_loop
from modules.session_filter import session_filter
from modules.execution_queue import execution_queue
from modules.position_manager import get_open_positions
from modules.position_sync import has_open_position
from modules.fill_tracker import monitor_trade
from modules.bot_mode import is_live
from modules.contract_resolver import resolve_contract


logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)

app = FastAPI()

trade_state = "IDLE"


# ==========================================
# EXECUTION WORKER
# ==========================================

def execution_worker():

    global trade_state

    logger.info("Execution queue worker started")

    while True:

        job = execution_queue.get()

        try:

            ib = ensure_connection()

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

            bracket = ib.bracketOrder(
                action="BUY" if side == "long" else "SELL",
                quantity=qty,
                limitPrice=entry,
                takeProfitPrice=target,
                stopLossPrice=stop
            )

            for order in bracket:
                ib.placeOrder(contract, order)

            trade_state = "IN_TRADE"

            logger.info("Bracket order sent")

        except Exception as e:

            logger.error(f"Execution error {e}")
            trade_state = "IDLE"

        finally:

            execution_queue.task_done()


# ==========================================
# STARTUP
# ==========================================

@app.on_event("startup")
def startup():

    logger.info("Scalping bot started")

    # Execution worker
    worker = threading.Thread(target=execution_worker, daemon=True)
    worker.start()

    # IB watchdog
    watchdog = threading.Thread(target=watchdog_loop, daemon=True)
    watchdog.start()

    # Fill tracker
    fill_thread = threading.Thread(target=monitor_trade, daemon=True)
    fill_thread.start()


# ==========================================
# HEALTH
# ==========================================

@app.get("/health")
def health():

    return {"status": "ok"}


# ==========================================
# BOT STATUS
# ==========================================

@app.get("/bot_status")
def bot_status():

    return {
        "ib_connected": is_ib_connected(),
        "session_allowed": session_filter(),
        "trade_state": trade_state,
        "queue_size": execution_queue.qsize(),
        "open_positions": get_open_positions()
    }


# ==========================================
# WEBHOOK
# ==========================================

@app.post("/webhook/tradingview")
async def webhook(request: Request):

    global trade_state

    data = await request.json()

    logger.info(data)

    if data.get("secret") != "FDAX_bot_secure_2026":
        raise HTTPException(status_code=403, detail="Invalid secret")

    if is_live() and not session_filter():
        logger.warning("Session filter blocked trade")
        return JSONResponse({"status": "blocked"})

    if is_live() and has_open_position():
        logger.warning("Position sync blocked trade")
        return JSONResponse({"status": "blocked"})

    if is_live() and trade_state != "IDLE":
        logger.warning("Trade state blocked trade")
        return JSONResponse({"status": "blocked"})

    symbol = data["symbol"]
    side = data["side"]
    entry_price = float(data["entry_price"])

    stop = entry_price - 0.0004
    target = entry_price + 0.0008

    qty = 1

    contract = resolve_contract(symbol)

    job = {
        "symbol": symbol,
        "side": side,
        "entry": entry_price,
        "stop": stop,
        "target": target,
        "qty": qty,
        "contract": contract
    }

    execution_queue.put(job)

    logger.info("Order added to execution queue")

    trade_state = "ENTRY_SENT"

    return {"status": "queued"}