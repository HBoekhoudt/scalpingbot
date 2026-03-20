# ==========================================================
# VERSIONING
# ==========================================================

BOT_NAME = "IKBR_SCALPING_BOT"
BOT_VERSION = "v1.5.0"
BOT_PATCH = "P260320012"
BOT_FULL_VERSION = f"{BOT_VERSION}-{BOT_PATCH}"
BOT_STAGE = "TEST"  # TEST | LIVE

"""
PATCH P260320012

* Removed broken FDXM contract (prevents error 200)
"""

import logging
import threading
import queue
import time
import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from ib_insync import IB, Future, Forex, LimitOrder

# ==========================================================
# LOGGING
# ==========================================================

logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)

# ==========================================================
# BOT
# ==========================================================

class ScalpingBot:

    def __init__(self):
        self.ib = IB()

        self.IB_HOST = "127.0.0.1"
        self.IB_PORT = 7497
        self.IB_CLIENT_ID = 101

        self.contract_cache = {}
        self.execution_queue = queue.Queue()

        self.trade_state = "IDLE"

        self.connection_lock = threading.Lock()
        self.trade_lock = threading.Lock()

        self.active_order_ids = set()

        logger.info(f"{BOT_NAME} {BOT_FULL_VERSION} STARTED ({BOT_STAGE})")

        threading.Thread(target=self.execution_worker, daemon=True).start()
        threading.Thread(target=self.ib_watchdog, daemon=True).start()

    # ==========================================================
    # IB CONNECT
    # ==========================================================

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

            self.attach_ib_events()
            self.qualify_contracts()

    # ==========================================================
    # EVENTS
    # ==========================================================

    def attach_ib_events(self):
        if hasattr(self.ib, "_events_attached"):
            return

        def on_order_status(trade):
            if trade.order.clientId != self.IB_CLIENT_ID:
                return

            if trade.order.orderId not in self.active_order_ids:
                return

            logger.info(
                f"ORDER STATUS | {trade.order.orderId} → {trade.orderStatus.status}"
            )

            if trade.orderStatus.status in ("Filled", "Cancelled"):
                self.active_order_ids.discard(trade.order.orderId)

                if not self.active_order_ids:
                    self.trade_state = "IDLE"
                    logger.info("TRADE COMPLETE → IDLE")

        self.ib.orderStatusEvent += on_order_status
        self.ib._events_attached = True

    # ==========================================================
    # CONTRACTS
    # ==========================================================

    def qualify_contracts(self):
        configs = [
            ("MNQ", "CME", "USD"),
            ("MES", "CME", "USD"),
            ("M6E", "CME", "USD"),
        ]

        for sym, exch, cur in configs:
            contract = Future(symbol=sym, exchange=exch, currency=cur)
            details = self.ib.reqContractDetails(contract)
            if details:
                self.contract_cache[sym] = details[0].contract

    def get_contract(self, symbol):
        if symbol in self.contract_cache:
            return self.contract_cache[symbol]

        if symbol == "EURUSD":
            return Forex("EURUSD")

        raise ValueError(f"Unknown contract: {symbol}")

    # ==========================================================
    # MARKET DATA
    # ==========================================================

    def get_market_data(self, contract):
        ticker = self.ib.reqMktData(contract, "", False, False)
        self.ib.sleep(0.5)

        bid = ticker.bid
        ask = ticker.ask

        self.ib.cancelMktData(contract)

        if bid is None or ask is None:
            return None, None, False

        if bid <= 0 or ask <= 0:
            return None, None, False

        return bid, ask, True

    # ==========================================================
    # EXECUTION
    # ==========================================================

    def execution_worker(self):
        while True:
            data = self.execution_queue.get()

            try:
                self.execute_trade(data)
            except Exception as e:
                logger.error(f"EXECUTION ERROR: {e}")

    def execute_trade(self, data):
        with self.trade_lock:

            if self.trade_state != "IDLE":
                logger.warning("BLOCKED: trade already active")
                return

            symbol = data.get("symbol")
            direction = data.get("direction")
            qty = data.get("qty", 1)

            contract = self.get_contract(symbol)

            bid, ask, valid = self.get_market_data(contract)
            if not valid:
                logger.warning("INVALID MARKET DATA")
                return

            price = ask if direction == "LONG" else bid

            order = LimitOrder(
                "BUY" if direction == "LONG" else "SELL",
                qty,
                price
            )

            trade = self.ib.placeOrder(contract, order)

            self.active_order_ids.add(order.orderId)
            self.trade_state = "ACTIVE"

            logger.info(f"ORDER PLACED: {symbol} {direction} @ {price}")

    # ==========================================================
    # WATCHDOG
    # ==========================================================

    def ib_watchdog(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while True:
            try:
                self.connect_ib()
            except Exception as e:
                logger.warning(f"IB reconnect failed: {e}")

            time.sleep(5)

# ==========================================================
# FASTAPI
# ==========================================================

app = FastAPI()
bot = ScalpingBot()

@app.post("/trade")
async def trade_endpoint(request: Request):
    data = await request.json()

    bot.execution_queue.put(data)

    return JSONResponse({"status": "queued"})