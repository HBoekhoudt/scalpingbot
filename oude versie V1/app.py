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
from modules.execution_engine import execution_worker
from modules.signal_guard import is_duplicate_signal
from modules.symbol_normalizer import normalize_symbol
import modules.trade_state as trade_state


logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)

app = FastAPI()


# ==========================================
# TICK SIZES
# ==========================================

def get_tick_size(symbol):

    symbol = symbol.upper()

    if symbol == "MNQ":
        return 0.25

    if symbol == "MES":
        return 0.25

    if symbol == "M6E":
        return 0.00005

    if symbol == "FDXM":
        return 0.5

    raise ValueError(f"Unknown tick size for {symbol}")


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
        "trade_state": trade_state.get_state(),
        "queue_size": execution_queue.qsize(),
        "open_positions": get_open_positions()
    }


# ==========================================
# WEBHOOK
# ==========================================

@app.post("/webhook/tradingview")
async def webhook(request: Request):

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

    if is_live() and trade_state.get_state() != "IDLE":
        logger.warning("Trade state blocked trade")
        return JSONResponse({"status": "blocked"})

    symbol = data["symbol"]
    side = data["side"]
    entry_price = float(data["entry_price"])

    if is_duplicate_signal(symbol, side, entry_price):
        logger.warning("Duplicate signal blocked")
        return JSONResponse({"status": "duplicate"})

    # ------------------------------------------
    # Tick based stop
    # ------------------------------------------

    symbol_norm = normalize_symbol(symbol)
    tick = get_tick_size(symbol_norm)

    stop_ticks = 8
    target_ticks = 16

    if side == "long":
        stop = entry_price - stop_ticks * tick
    else:
        stop = entry_price + stop_ticks * tick

    # ------------------------------------------
    # Target = 2R
    # ------------------------------------------

    R = abs(entry_price - stop)

    if side == "long":
        target = entry_price + 2 * R
    else:
        target = entry_price - 2 * R

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

    logger.info(f"Queue size after insert: {execution_queue.qsize()}")

    logger.info("Order added to execution queue")

    trade_state.set_state("ENTRY_SENT")

    return {"status": "queued"}