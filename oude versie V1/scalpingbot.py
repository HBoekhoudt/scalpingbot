import logging
import asyncio
import threading
import queue

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

        self.worker_thread = threading.Thread(
            target=self.execution_worker,
            daemon=True
        )

        self.worker_thread.start()

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

            self.qualify_contracts()

    # ======================================================
    # IB EVENTS
    # ======================================================

    def attach_ib_events(self):

        if hasattr(self.ib, "_events_attached"):
            return

        def on_order_status(trade):
            logger.info(f"ORDER STATUS: {trade.orderStatus}")

        def on_exec_details(trade, fill):
            logger.info(f"FILL: {fill}")

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
            return f"{year}03"
        elif month <= 6:
            return f"{year}06"
        elif month <= 9:
            return f"{year}09"
        else:
            return f"{year}12"

    def qualify_contracts(self):

        logger.info("Qualifying futures contracts...")

        expiry = self._front_month()

        logger.info(f"Using expiry {expiry}")

        contracts = [

            Future(
                symbol="MNQ",
                exchange="GLOBEX",
                currency="USD",
                tradingClass="MNQ",
                lastTradeDateOrContractMonth=expiry
            ),

            Future(
                symbol="MES",
                exchange="GLOBEX",
                currency="USD",
                tradingClass="MES",
                lastTradeDateOrContractMonth=expiry
            ),

            Future(
                symbol="M6E",
                exchange="GLOBEX",
                currency="USD",
                tradingClass="M6E",
                lastTradeDateOrContractMonth=expiry
            ),

            Future(
                symbol="FDXM",
                exchange="EUREX",
                currency="EUR",
                tradingClass="FDXM",
                lastTradeDateOrContractMonth=expiry
            )
        ]

        qualified = self.ib.qualifyContracts(*contracts)

        for c in qualified:

            if c.conId == 0:
                logger.error(f"Contract qualification failed for {c.symbol}")
                continue

            self.contract_cache[c.symbol] = c

        logger.info(
            f"Contract cache initialized: {list(self.contract_cache.keys())}"
        )

    def get_contract(self, symbol):

        if symbol in self.contract_cache:
            contract = self.contract_cache[symbol]
            logger.info(f"Using cached contract {contract.symbol} {contract.lastTradeDateOrContractMonth}")
            return contract

        if symbol == "EURUSD":
            return Forex("EURUSD")

        # fallback dynamic qualification

        logger.warning(f"Contract {symbol} not cached — attempting dynamic qualification")

        expiry = self._front_month()

        contract = Future(
            symbol=symbol,
            exchange="GLOBEX",
            currency="USD",
            tradingClass=symbol,
            lastTradeDateOrContractMonth=expiry
        )

        qualified = self.ib.qualifyContracts(contract)

        if not qualified:
            raise ValueError(f"Unable to qualify contract {symbol}")

        contract = qualified[0]

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

        contract = self.get_contract(symbol)

        logger.info(
            f"Using contract {contract.symbol} {contract.lastTradeDateOrContractMonth}"
        )

        job = {
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "contract": contract
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

        symbol = job["symbol"]
        side = job["side"]
        entry = job["entry"]
        contract = job["contract"]

        # instrument-specific risk model

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

        bracket = self.ib.bracketOrder(
            action=action,
            quantity=1,
            limitPrice=entry,
            takeProfitPrice=target,
            stopLossPrice=stop
        )

        for order in bracket:
            self.ib.placeOrder(contract, order)

        self.trade_state = "IN_TRADE"

        logger.info(f"Bracket order placed for {symbol}")


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