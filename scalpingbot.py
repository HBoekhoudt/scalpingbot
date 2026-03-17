import logging
import asyncio
import threading
import queue
import time

from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from ib_insync import IB, Future, Forex, LimitOrder, StopOrder

logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)


# ==========================================================
# SCALPING BOT ENGINE
# ==========================================================

class ScalpingBot:

    def __init__(self):

        self.ib = IB()

        self.IB_HOST = "127.0.0.1"
        self.IB_PORT = 7497
        self.IB_CLIENT_ID = 101

        self.contract_cache = {}

        self.execution_queue = queue.Queue()

        self.positions = {}

        self.trade_state = "IDLE"

        self.connection_lock = threading.Lock()
        self.trade_lock = threading.Lock()

        # duplicate signal guard
        self.last_signal = None
        self.last_signal_time = 0

        self.worker_thread = threading.Thread(
            target=self.execution_worker,
            daemon=True
        )
        self.worker_thread.start()

        # IB reconnect watchdog
        self.watchdog_thread = threading.Thread(
            target=self.ib_watchdog,
            daemon=True
        )
        self.watchdog_thread.start()

    # ======================================================
    # IB CONNECTION
    # ======================================================

    def connect_ib(self):

        if self.ib.isConnected():
            return

        with self.connection_lock:

            if self.ib.isConnected():
                return

            logger.info("Connecting to IBKR...")

            self.ib.connect(
                self.IB_HOST,
                self.IB_PORT,
                clientId=self.IB_CLIENT_ID
            )

            if not self.ib.isConnected():
                raise RuntimeError("IBKR connection failed")

            logger.info("IBKR connected")

            self.attach_ib_events()

            positions = self.ib.positions()

            for pos in positions:
                if pos.position != 0:
                    self.trade_state = "IN_TRADE"
                    logger.info(f"Existing position detected: {pos.contract.symbol}")

            self.qualify_contracts()

    # ======================================================
    # IB EVENTS
    # ======================================================

    def attach_ib_events(self):

        if hasattr(self.ib, "_events_attached"):
            return

        def on_order_status(trade):
            logger.info(
                f"ORDER STATUS | "
                f"orderId={trade.order.orderId} "
                f"parentId={getattr(trade.order,'parentId',None)} "
                f"action={trade.order.action} "
                f"orderType={trade.order.orderType} "
                f"status={trade.orderStatus.status} "
                f"filled={trade.orderStatus.filled} "
                f"remaining={trade.orderStatus.remaining} "
                f"avgFillPrice={trade.orderStatus.avgFillPrice}"
            )

            if "Modify" in str(trade.orderStatus.status):
                logger.error("CRITICAL: MODIFY DETECTED → ORDER CHAIN CORRUPTION")

        def on_exec_details(trade, fill):
            logger.info(f"FILL: {fill}")

            positions = self.ib.positions()

            if not any(p.position != 0 for p in positions):
                self.trade_state = "IDLE"
                logger.info("Position closed — trade_state reset to IDLE")

        def on_ib_error(reqId, errorCode, errorString, contract):
            symbol = getattr(contract, "symbol", None) if contract else None
            logger.error(
                f"IB ERROR | reqId={reqId} code={errorCode} symbol={symbol} message={errorString}"
            )

        self.ib.orderStatusEvent += on_order_status
        self.ib.execDetailsEvent += on_exec_details
        self.ib.errorEvent += on_ib_error

        self.ib._events_attached = True

    # ======================================================
    # SYMBOL RESOLUTION
    # ======================================================

    def resolve_symbol(self, tv_symbol):

        symbol = tv_symbol.upper()

        if symbol.endswith("1!"):
            symbol = symbol[:-2]

        if symbol == "FDAX":
            symbol = "FDXM"

        logger.info(f"Resolved TradingView symbol {tv_symbol} → {symbol}")

        return symbol

    # ======================================================
    # CONTRACTS
    # ======================================================

    def qualify_contracts(self):

        logger.info("Qualifying futures contracts...")

        symbols = [
            ("MNQ", "CME", "USD"),
            ("MES", "CME", "USD"),
            ("M6E", "CME", "USD"),
            ("FDXM", "EUREX", "EUR"),
        ]

        for sym, exch, cur in symbols:

            if sym == "FDXM":
                contract = Future(
                    symbol=sym,
                    exchange=exch,
                    currency=cur,
                    tradingClass=sym
                )
            else:
                contract = Future(
                    symbol=sym,
                    exchange=exch,
                    currency=cur
                )

            details = self.ib.reqContractDetails(contract)

            if not details:
                logger.error(f"Contract qualification failed for {sym}")
                continue

            self.contract_cache[sym] = details[0].contract

            logger.info(
                f"Qualified {sym} → {details[0].contract.lastTradeDateOrContractMonth}"
            )

        logger.info(
            f"Contract cache initialized: {list(self.contract_cache.keys())}"
        )

    def get_contract(self, symbol):

        if symbol in self.contract_cache:
            contract = self.contract_cache[symbol]
            logger.info(
                f"Using cached contract {contract.symbol} {contract.lastTradeDateOrContractMonth}"
            )
            return contract

        if symbol == "EURUSD":
            return Forex("EURUSD")

        raise ValueError(f"Contract {symbol} not cached")

    # ======================================================
    # SIGNAL HANDLING
    # ======================================================

    def handle_webhook_signal(self, signal):

        tv_symbol = signal["symbol"]

        symbol = self.resolve_symbol(tv_symbol)

        side = signal["side"]
        entry = float(signal["entry_price"])

        current_time = time.time()

        signal_key = f"{symbol}-{side}-{entry}"

        if signal_key == self.last_signal and current_time - self.last_signal_time < 5:
            logger.info("Duplicate signal ignored")
            return

        self.last_signal = signal_key
        self.last_signal_time = current_time

        job = {
            "symbol": symbol,
            "side": side,
            "entry": entry
        }

        self.enqueue_signal(job)

    # ======================================================
    # QUEUE
    # ======================================================

    def enqueue_signal(self, signal):

        self.execution_queue.put(signal)

        logger.info(
            f"Signal queued. Queue size: {self.execution_queue.qsize()}"
        )

    # ======================================================
    # EXECUTION WORKER
    # ======================================================

    def execution_worker(self):

        asyncio.set_event_loop(asyncio.new_event_loop())

        logger.info("Execution worker started")

        self.connect_ib()

        while True:

            job = self.execution_queue.get()
            logger.info(f"WORKER RECEIVED JOB: {job}")

            try:
                self.connect_ib()
                self.place_bracket_order(job)
            except Exception:
                logger.exception("Execution failure")
            finally:
                self.execution_queue.task_done()

    # ======================================================
    # ORDER EXECUTION
    # ======================================================

    def place_bracket_order(self, job):

        # 🔧 FIX 1 — FORCE IB SYNC
        self.ib.reqOpenOrders()
        self.ib.sleep(0.3)
        open_orders = self.ib.openOrders()

        # 🔧 FIX 2 — HARD BLOCK
        #if open_orders:
        #    logger.error("HARD BLOCK → EXISTING IB ORDERS STILL ACTIVE")
        #
        #    for o in open_orders:
        #        logger.error(
        #            f"ACTIVE ORDER | id={o.orderId} parent={o.parentId} action={o.action}"
        #        )
        #    return

        symbol = job["symbol"]
        side = job["side"]
        entry = job["entry"]

        contract = self.get_contract(symbol)

        if symbol in ("MES", "MNQ"):
            stop_distance = 2
            target_distance = 4
        elif symbol == "M6E":
            stop_distance = 0.002
            target_distance = 0.004
        else:
            stop_distance = 2
            target_distance = 4

        if side == "long":
            stop = entry - stop_distance
            target = entry + target_distance
            action = "BUY"
        else:
            stop = entry + stop_distance
            target = entry - target_distance
            action = "SELL"

        opposite_action = "SELL" if action == "BUY" else "BUY"

        parent_id = self.ib.client.getReqId()

        existing_ids = {o.orderId for o in open_orders}

        if parent_id in existing_ids:
            logger.error("ORDER ID REUSE DETECTED → FORCING NEW ID")
            parent_id = self.ib.client.getReqId()

        tp_id = parent_id + 1
        sl_id = parent_id + 2

        logger.info("ORDER DEBUG →")
        logger.info(f"parent_id={parent_id} tp_id={tp_id} sl_id={sl_id}")

        parent = LimitOrder(action=action, totalQuantity=1, lmtPrice=entry, transmit=False)
        parent.orderId = parent_id
        parent.parentId = 0

        tp = LimitOrder(action=opposite_action, totalQuantity=1, lmtPrice=target, transmit=False)
        tp.orderId = tp_id
        tp.parentId = parent_id

        sl = StopOrder(action=opposite_action, totalQuantity=1, stopPrice=stop, transmit=True)
        sl.orderId = sl_id
        sl.parentId = parent_id

        for o in [parent, tp, sl]:
            logger.info(
                f"ORDER → action={o.action} type={o.orderType} parentId={o.parentId} transmit={o.transmit}"
            )

        self.ib.placeOrder(contract, parent)
        self.ib.placeOrder(contract, tp)
        self.ib.placeOrder(contract, sl)

        trades = self.ib.trades()

        for t in trades:
            logger.info(
                f"POST TRADE | orderId={t.order.orderId} parentId={t.order.parentId} "
                f"permId={t.order.permId} status={t.orderStatus.status}"
            )

    # ======================================================
    # IB WATCHDOG
    # ======================================================

    def ib_watchdog(self):

        asyncio.set_event_loop(asyncio.new_event_loop())

        while True:

            if not self.ib.isConnected():

                logger.warning("IB connection lost — attempting reconnect")

                try:
                    self.connect_ib()
                except Exception:
                    logger.exception("Reconnect attempt failed")

            time.sleep(10)


# ==========================================================
# FASTAPI SERVER
# ==========================================================

app = FastAPI()

bot = ScalpingBot()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/webhook/tradingview")
async def webhook_handler(request: Request):

    data = await request.json()

    if data.get("secret") != "FDAX_bot_secure_2026":
        raise HTTPException(status_code=403, detail="Invalid secret")

    bot.handle_webhook_signal(data)

    return {"status": "queued"}