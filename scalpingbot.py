import logging
import asyncio
import threading
import queue
import time

from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from ib_insync import IB, Future, Forex

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

        self.order_id = None

        self.connection_lock = threading.Lock()
        self.trade_lock = threading.Lock()

        # PATCH 1 — duplicate signal guard
        self.last_signal = None
        self.last_signal_time = 0

        self.worker_thread = threading.Thread(
            target=self.execution_worker,
            daemon=True
        )

        self.worker_thread.start()

        # PATCH 3 — IB reconnect watchdog
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

            asyncio.set_event_loop(asyncio.new_event_loop())

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

            self.order_id = self.ib.client.getReqId()

            logger.info(f"Order ID initialized to {self.order_id}")

            # PATCH 4 — crash recovery position check
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
                f"ORDER STATUS: id={trade.order.orderId} "
                f"status={trade.orderStatus.status} "
                f"filled={trade.orderStatus.filled} "
                f"remaining={trade.orderStatus.remaining}"
            )

        def on_exec_details(trade, fill):
            logger.info(f"FILL: {fill}")

            # PATCH 2 — trade_state reset after position closes
            positions = self.ib.positions()

            if not any(p.position != 0 for p in positions):
                self.trade_state = "IDLE"
                logger.info("Position closed — trade_state reset to IDLE")

        def on_ib_error(reqId, errorCode, errorString, contract):
            logger.error(f"IB ERROR {errorCode}: {errorString}")

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

    def _front_month(self):

        now = datetime.utcnow()

        year = now.year
        month = now.month

        if month <= 3:
            return f"{year}0321"
        elif month <= 6:
            return f"{year}0621"
        elif month <= 9:
            return f"{year}0921"
        else:
            return f"{year}1221"

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
                details = self.ib.reqContractDetails(
                    Future(
                        symbol=sym,
                        exchange=exch,
                        currency=cur,
                        tradingClass=sym
                    )
                )
            else:
                details = self.ib.reqContractDetails(
                    Future(
                        symbol=sym,
                        exchange=exch,
                        currency=cur
                    )
                )

            if not details:
                logger.error(f"Contract qualification failed for {sym}")
                continue

            contract = details[0].contract

            self.contract_cache[sym] = contract

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

        logger.warning(f"Contract {symbol} not cached — attempting dynamic qualification")

        if symbol == "FDXM":
            details = self.ib.reqContractDetails(
                Future(symbol=symbol, exchange="EUREX", currency="EUR", tradingClass=symbol)
            )
        else:
            details = self.ib.reqContractDetails(
                Future(symbol=symbol, exchange="CME", currency="USD")
            )

        if not details:
            raise ValueError(f"Unable to qualify contract {symbol}")

        contract = details[0].contract

        self.contract_cache[symbol] = contract

        return contract

    # ======================================================
    # SIGNAL HANDLING
    # ======================================================

    def handle_webhook_signal(self, signal):

        tv_symbol = signal["symbol"]

        symbol = self.resolve_symbol(tv_symbol)

        side = signal["side"]
        entry = float(signal["entry_price"])

        # PATCH 1 — duplicate TradingView signal guard
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

        # PATCH — atomic trade_state guard
        with self.trade_lock:

            if self.trade_state == "IN_TRADE":
                logger.info("Trade ignored — already in position")
                return

            self.trade_state = "IN_TRADE"

        symbol = job["symbol"]
        side = job["side"]
        entry = job["entry"]

        contract = self.get_contract(symbol)

        logger.info(f"Using contract conId={contract.conId}")

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

        logger.info(
            f"Placing bracket order {symbol} entry={entry} stop={stop} target={target}"
        )

        self.ib.client.setConnectOptions("+PACEAPI")

        bracket = self.ib.bracketOrder(
            action=action,
            quantity=1,
            limitPrice=entry,
            takeProfitPrice=target,
            stopLossPrice=stop
        )

        # PATCH — ensure bracket is transmitted
        bracket[0].transmit = False
        bracket[1].transmit = False
        bracket[2].transmit = True

        for order in bracket:
            self.ib.placeOrder(contract, order)

        self.ib.waitOnUpdate(timeout=1)

        logger.info(f"Bracket order placed for {symbol}")

    # ======================================================
    # IB WATCHDOG
    # ======================================================

    def ib_watchdog(self):

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