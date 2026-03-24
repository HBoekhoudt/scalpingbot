# ==========================================================
# IKBR SCALPING BOT — P026
# ==========================================================

import logging
import asyncio
import threading
import queue
import time
import decimal

from fastapi import FastAPI, Request, HTTPException
from ib_insync import IB, Future, Forex, LimitOrder, StopOrder

# ==========================================================
# VERSIONING
# ==========================================================

BOT_NAME = "IKBR_SCALPING_BOT"
BOT_VERSION = "v1.6.0"
BOT_PATCH = "P260324026"
BOT_STAGE = "TEST"  # TEST | PAPER | LIVE

logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)

# ==========================================================
# CONFIG
# ==========================================================

SPREADS = {
    "MNQ": 0.25,
    "MES": 0.25,
    "FDXM": 0.5,
    "M6E": 0.00005,
}

TICK_SIZES = {
    "MNQ": 0.25,
    "MES": 0.25,
    "FDXM": 0.5,
    "M6E": 0.00005,
}

# ==========================================================
# BOT
# ==========================================================

class ScalpingBot:

    def __init__(self):

        self.ib = IB()

        self.IB_HOST = "127.0.0.1"
        self.IB_PORT = 7497
        self.IB_CLIENT_ID = 1

        self.contract_cache = {}
        self.execution_queue = queue.Queue()

        self.trade_state = "IDLE"

        self.connection_lock = threading.Lock()

        self.last_signal = None
        self.last_signal_time = 0

        threading.Thread(target=self.execution_worker, daemon=True).start()
        threading.Thread(target=self.ib_watchdog, daemon=True).start()

    # ==========================================================
    # PRICE LOGIC
    # ==========================================================

    def round_to_tick(self, symbol, price):
        tick = decimal.Decimal(str(TICK_SIZES[symbol]))
        price = decimal.Decimal(str(price))
        return float((price / tick).quantize(0) * tick)

    def apply_spread(self, symbol, side, price):
        spread = SPREADS.get(symbol, 0)

        if side == "long":
            price += spread
        else:
            price -= spread

        logger.info(f"SPREAD | {symbol} {side} → {price}")
        return price

    # ==========================================================
    # IB CONNECTION
    # ==========================================================

    def connect_ib(self):

        if self.ib.isConnected():
            return

        with self.connection_lock:

            if self.ib.isConnected():
                return

            logger.info("Connecting to IBKR...")

            self.ib.connect(self.IB_HOST, self.IB_PORT, clientId=self.IB_CLIENT_ID)

            if not self.ib.isConnected():
                raise RuntimeError("IBKR connection failed")

            logger.info("IBKR connected")

            self.attach_ib_events()
            self.qualify_contracts()

    def log_trade_snapshot(self, label, trade):

        try:
            order = trade.order
            status = trade.orderStatus

            logger.info(
                f"{label} → "
                f"orderId={getattr(order, 'orderId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"action={getattr(order, 'action', None)} "
                f"orderType={getattr(order, 'orderType', None)} "
                f"lmtPrice={getattr(order, 'lmtPrice', None)} "
                f"auxPrice={getattr(order, 'auxPrice', None)} "
                f"transmit={getattr(order, 'transmit', None)} "
                f"status={getattr(status, 'status', None)} "
                f"filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} "
                f"avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)} "
                f"permId={getattr(status, 'permId', None)}"
            )

            if getattr(trade, "advancedError", None):
                logger.error(f"{label} ADVANCED ERROR → {trade.advancedError}")

            if getattr(trade, "log", None):
                for entry in trade.log:
                    logger.info(
                        f"{label} TRADE LOG → "
                        f"time={getattr(entry, 'time', None)} "
                        f"status={getattr(entry, 'status', None)} "
                        f"message={getattr(entry, 'message', None)} "
                        f"errorCode={getattr(entry, 'errorCode', None)}"
                    )

        except Exception:
            logger.exception(f"{label} SNAPSHOT FAILED")

    def attach_ib_events(self):

        if hasattr(self.ib, "_events_attached"):
            return

        def on_exec(trade, fill):
            logger.info(f"FILL: {fill}")

            positions = self.ib.positions()
            if not any(p.position != 0 for p in positions):
                self.trade_state = "IDLE"
                logger.info("→ IDLE")

        def on_open_order(trade):
            order = trade.order
            status = trade.orderStatus
            logger.info(
                f"OPEN ORDER EVENT → "
                f"orderId={getattr(order, 'orderId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"action={getattr(order, 'action', None)} "
                f"orderType={getattr(order, 'orderType', None)} "
                f"lmtPrice={getattr(order, 'lmtPrice', None)} "
                f"auxPrice={getattr(order, 'auxPrice', None)} "
                f"transmit={getattr(order, 'transmit', None)} "
                f"status={getattr(status, 'status', None)} "
                f"filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} "
                f"avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)} "
                f"permId={getattr(status, 'permId', None)}"
            )

        def on_order_status(trade):
            order = trade.order
            status = trade.orderStatus
            logger.info(
                f"ORDER STATUS EVENT → "
                f"orderId={getattr(order, 'orderId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"action={getattr(order, 'action', None)} "
                f"orderType={getattr(order, 'orderType', None)} "
                f"status={getattr(status, 'status', None)} "
                f"filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} "
                f"avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)} "
                f"permId={getattr(status, 'permId', None)}"
            )

        def on_error(reqId, errorCode, errorString, contract):
            logger.error(
                f"ERROR EVENT → "
                f"reqId={reqId} "
                f"errorCode={errorCode} "
                f"errorString={errorString} "
                f"contract={contract}"
            )

        self.ib.execDetailsEvent += on_exec
        self.ib.openOrderEvent += on_open_order
        self.ib.orderStatusEvent += on_order_status
        self.ib.errorEvent += on_error

        self.ib._events_attached = True

    # ==========================================================
    # CONTRACTS
    # ==========================================================

    def build_base_contract(self, symbol):

        if symbol in ("MNQ", "MES", "M6E"):
            return Future(symbol=symbol, exchange="CME", currency="USD")

        elif symbol == "FDXM":
            return Future(symbol="FDXM", exchange="EUREX", currency="EUR", tradingClass="FDXM")

        elif symbol == "EURUSD":
            return Forex("EURUSD")

        else:
            raise ValueError(f"Unsupported symbol: {symbol}")

    def select_front_month(self, details):

        sorted_contracts = sorted(
            details,
            key=lambda d: d.contract.lastTradeDateOrContractMonth
        )

        for d in sorted_contracts:
            expiry = d.contract.lastTradeDateOrContractMonth
            if expiry and len(expiry) >= 6:
                return d.contract

        raise RuntimeError("No valid contract")

    def qualify_contracts(self):

        logger.info("QUALIFY CONTRACTS")

        for sym in ["MNQ", "MES", "M6E", "FDXM"]:

            try:
                base = self.build_base_contract(sym)
                details = self.ib.reqContractDetails(base)

                contract = self.select_front_month(details)

                self.contract_cache[sym] = contract

                logger.info(f"{sym} → {contract.lastTradeDateOrContractMonth}")

            except Exception:
                logger.exception(f"FAILED {sym}")

    def get_contract(self, symbol):

        if symbol in self.contract_cache:
            return self.contract_cache[symbol]

        if symbol == "EURUSD":
            return Forex("EURUSD")

        raise ValueError(f"{symbol} not cached")

    # ==========================================================
    # SIGNAL HANDLING
    # ==========================================================

    def handle_webhook_signal(self, data):

        logger.info(f"WEBHOOK RECEIVED: {data}")

        symbol = data["symbol"].upper().replace("1!", "")
        if symbol == "FDAX":
            symbol = "FDXM"

        side = data["side"]
        entry = float(data["entry_price"])

        now = time.time()
        key = f"{symbol}-{side}-{round(entry, 2)}"

        if key == self.last_signal and now - self.last_signal_time < 5:
            logger.info("Duplicate ignored")
            return

        self.last_signal = key
        self.last_signal_time = now

        job = {
            "symbol": symbol,
            "side": side,
            "entry": entry
        }

        logger.info(f"QUEUE PUT: {job}")

        self.execution_queue.put(job)

    # ==========================================================
    # WORKER
    # ==========================================================

    def execution_worker(self):

        asyncio.set_event_loop(asyncio.new_event_loop())

        logger.info("EXECUTION WORKER STARTED")
        logger.info(f"BOT STAGE: {BOT_STAGE}")

        self.connect_ib()

        while True:

            job = self.execution_queue.get()

            logger.info(f"WORKER RECEIVED JOB: {job}")

            try:
                logger.info(f"IB CONNECTED: {self.ib.isConnected()}")
                logger.info("STARTING ORDER EXECUTION")

                self.place_bracket_order(job)

                logger.info("ORDER EXECUTION FINISHED")

            except Exception:
                logger.exception("Execution error")

            self.execution_queue.task_done()

    # ==========================================================
    # 🔥 P026 — OFFICIAL BRACKET MODEL + EXPANDED LOGGING
    # ==========================================================

    def place_bracket_order(self, job):

        symbol = job["symbol"]
        side = job["side"]

        entry = self.apply_spread(symbol, side, job["entry"])
        entry = self.round_to_tick(symbol, entry)

        contract = self.get_contract(symbol)

        if symbol in ("MES", "MNQ"):
            stop_dist = 2
            target_dist = 4
        elif symbol == "M6E":
            stop_dist = 0.002
            target_dist = 0.004
        else:
            stop_dist = 2
            target_dist = 4

        if side == "long":
            stop = entry - stop_dist
            target = entry + target_dist
            parent_action = "BUY"
            child_action = "SELL"
        else:
            stop = entry + stop_dist
            target = entry - target_dist
            parent_action = "SELL"
            child_action = "BUY"

        stop = self.round_to_tick(symbol, stop)
        target = self.round_to_tick(symbol, target)

        logger.info(
            f"OFFICIAL BRACKET | {symbol} {side} entry={entry} stop={stop} target={target}"
        )

        parent_id = self.ib.client.getReqId()
        tp_id = parent_id + 1
        sl_id = parent_id + 2

        parent = LimitOrder(parent_action, 1, entry)
        parent.orderId = parent_id
        parent.transmit = False
        parent.tif = "GTC"

        tp = LimitOrder(child_action, 1, target)
        tp.orderId = tp_id
        tp.parentId = parent_id
        tp.transmit = False
        tp.tif = "GTC"

        sl = StopOrder(child_action, 1, stop)
        sl.orderId = sl_id
        sl.parentId = parent_id
        sl.transmit = True
        sl.tif = "GTC"

        logger.info(
            f"ORDER DEF PARENT → orderId={parent.orderId} parentId={parent.parentId} "
            f"action={parent.action} orderType={parent.orderType} "
            f"lmtPrice={getattr(parent, 'lmtPrice', None)} auxPrice={getattr(parent, 'auxPrice', None)} "
            f"transmit={parent.transmit}"
        )
        logger.info(
            f"ORDER DEF TP → orderId={tp.orderId} parentId={tp.parentId} "
            f"action={tp.action} orderType={tp.orderType} "
            f"lmtPrice={getattr(tp, 'lmtPrice', None)} auxPrice={getattr(tp, 'auxPrice', None)} "
            f"transmit={tp.transmit}"
        )
        logger.info(
            f"ORDER DEF SL → orderId={sl.orderId} parentId={sl.parentId} "
            f"action={sl.action} orderType={sl.orderType} "
            f"lmtPrice={getattr(sl, 'lmtPrice', None)} auxPrice={getattr(sl, 'auxPrice', None)} "
            f"transmit={sl.transmit}"
        )

        parent_trade = self.ib.placeOrder(contract, parent)
        tp_trade = self.ib.placeOrder(contract, tp)
        sl_trade = self.ib.placeOrder(contract, sl)

        self.log_trade_snapshot("POST PLACE PARENT", parent_trade)
        self.log_trade_snapshot("POST PLACE TP", tp_trade)
        self.log_trade_snapshot("POST PLACE SL", sl_trade)

        self.ib.sleep(0.20)

        self.log_trade_snapshot("POST WAIT PARENT", parent_trade)
        self.log_trade_snapshot("POST WAIT TP", tp_trade)
        self.log_trade_snapshot("POST WAIT SL", sl_trade)

        self.ib.reqOpenOrders()
        self.ib.sleep(0.20)

        self.log_trade_snapshot("POST REQOPENORDERS PARENT", parent_trade)
        self.log_trade_snapshot("POST REQOPENORDERS TP", tp_trade)
        self.log_trade_snapshot("POST REQOPENORDERS SL", sl_trade)

        logger.info("BRACKET SUBMITTED (P026 FIXED)")

    # ==========================================================
    # WATCHDOG
    # ==========================================================

    def ib_watchdog(self):

        asyncio.set_event_loop(asyncio.new_event_loop())

        while True:

            if not self.ib.isConnected():
                logger.warning("Reconnecting...")
                try:
                    self.connect_ib()
                except Exception:
                    logger.exception("Reconnect failed")

            time.sleep(10)

# ==========================================================
# API
# ==========================================================

app = FastAPI()
bot = ScalpingBot()

@app.get("/health")
def health():
    return {
        "status": "ok",
        "bot_name": BOT_NAME,
        "bot_version": BOT_VERSION,
        "bot_patch": BOT_PATCH,
        "bot_stage": BOT_STAGE
    }

@app.post("/webhook/tradingview")
async def webhook_handler(request: Request):

    data = await request.json()

    if data.get("secret") != "FDAX_bot_secure_2026":
        raise HTTPException(status_code=403)

    bot.handle_webhook_signal(data)

    return {"status": "queued"}