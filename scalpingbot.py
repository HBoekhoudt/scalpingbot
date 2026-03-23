import logging
import asyncio
import threading
import queue
import time

from fastapi import FastAPI, Request, HTTPException
from ib_insync import IB, Future, Forex

BOT_NAME = "IKBR_SCALPING_BOT"
BOT_VERSION = "v1.6.0"
BOT_PATCH = "P260323008"
BOT_STAGE = "TEST"  # TEST | PAPER | LIVE

logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)


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

        self.last_signal = None
        self.last_signal_time = 0

        threading.Thread(target=self.execution_worker, daemon=True).start()
        threading.Thread(target=self.ib_watchdog, daemon=True).start()

    def round_to_tick(self, symbol, price):
        tick = TICK_SIZES.get(symbol)
        if not tick:
            return price
        return round(price / tick) * tick

    def apply_spread(self, symbol, side, price):
        spread = SPREADS.get(symbol, 0)

        if side == "long":
            price += spread
        else:
            price -= spread

        logger.info(f"SPREAD | {symbol} {side} → {price}")
        return price

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

    def attach_ib_events(self):

        if hasattr(self.ib, "_events_attached"):
            return

        def on_exec(trade, fill):
            logger.info(f"FILL: {fill}")

            positions = self.ib.positions()
            if not any(p.position != 0 for p in positions):
                self.trade_state = "IDLE"
                logger.info("→ IDLE")

        self.ib.execDetailsEvent += on_exec
        self.ib._events_attached = True

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

    def log_trade_snapshot(self, label, trade):

        try:
            order = trade.order
            status = trade.orderStatus

            logger.info(
                f"{label} | "
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
                f"permId={getattr(status, 'permId', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)}"
            )

            if getattr(trade, "advancedError", None):
                logger.warning(f"{label} ADVANCED ERROR | {trade.advancedError}")

            if getattr(trade, "log", None):
                for entry in trade.log:
                    logger.info(
                        f"{label} TRADE LOG | "
                        f"time={getattr(entry, 'time', None)} "
                        f"status={getattr(entry, 'status', None)} "
                        f"message={getattr(entry, 'message', None)} "
                        f"errorCode={getattr(entry, 'errorCode', None)}"
                    )
        except Exception:
            logger.exception(f"{label} | failed to log trade snapshot")

    def place_bracket_order(self, job):

        logger.info("ENTER place_bracket_order")
        logger.info(f"STAGE CHECK → {BOT_STAGE}")

        positions = self.ib.positions()

        if BOT_STAGE in ("PAPER", "LIVE"):
            if any(p.position != 0 for p in positions):
                logger.warning(f"BLOCK → existing position ({BOT_STAGE} mode)")
                return

            if self.trade_state == "IN_TRADE":
                logger.warning(f"BLOCK → in trade ({BOT_STAGE} mode)")
                return
        else:
            if any(p.position != 0 for p in positions):
                logger.info("TEST MODE → existing position ignored")

            if self.trade_state == "IN_TRADE":
                logger.info("TEST MODE → trade_state ignored")

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
            action = "BUY"
        else:
            stop = entry + stop_dist
            target = entry - target_dist
            action = "SELL"

        stop = self.round_to_tick(symbol, stop)
        target = self.round_to_tick(symbol, target)

        logger.info(
            f"BRACKET PREP | stage={BOT_STAGE} {symbol} {side} "
            f"entry={entry} stop={stop} target={target}"
        )

        if BOT_STAGE == "PAPER":
            logger.info(
                f"PAPER MODE → simulated bracket only | "
                f"{symbol} {side} entry={entry} stop={stop} target={target}"
            )
            self.trade_state = "IN_TRADE"
            logger.info("PAPER MODE → trade_state set to IN_TRADE")
            return

        bracket = self.ib.bracketOrder(
            action,
            1,
            entry,
            target,
            stop
        )

        bracket[0].transmit = False
        bracket[1].transmit = False
        bracket[2].transmit = True

        for i, o in enumerate(bracket):
            logger.info(
                f"ORDER DEF {i} | "
                f"orderId={getattr(o, 'orderId', None)} "
                f"parentId={getattr(o, 'parentId', None)} "
                f"action={getattr(o, 'action', None)} "
                f"orderType={getattr(o, 'orderType', None)} "
                f"lmtPrice={getattr(o, 'lmtPrice', None)} "
                f"auxPrice={getattr(o, 'auxPrice', None)} "
                f"transmit={getattr(o, 'transmit', None)}"
            )

        logger.info(
            f"CONTRACT SNAPSHOT | "
            f"symbol={getattr(contract, 'symbol', None)} "
            f"localSymbol={getattr(contract, 'localSymbol', None)} "
            f"expiry={getattr(contract, 'lastTradeDateOrContractMonth', None)} "
            f"exchange={getattr(contract, 'exchange', None)} "
            f"currency={getattr(contract, 'currency', None)} "
            f"conId={getattr(contract, 'conId', None)}"
        )

        logger.info("PLACING BRACKET ORDER (ALL 3 ORDERS)")

        placed_trades = []

        for i, o in enumerate(bracket):
            trade = self.ib.placeOrder(contract, o)
            placed_trades.append(trade)

            self.log_trade_snapshot(f"POST PLACE {i}", trade)

            self.ib.sleep(0.20)

            self.log_trade_snapshot(f"POST WAIT {i}", trade)

        self.ib.sleep(1.00)

        logger.info("FINAL BRACKET SNAPSHOT START")

        for i, trade in enumerate(placed_trades):
            self.log_trade_snapshot(f"FINAL SNAPSHOT {i}", trade)

        logger.info("FINAL BRACKET SNAPSHOT END")

        if BOT_STAGE == "LIVE":
            self.trade_state = "IN_TRADE"
            logger.info("LIVE MODE → trade_state set to IN_TRADE")
        else:
            logger.info("TEST MODE → trade_state not locked")

        logger.info(f"BRACKET ORDER | {symbol} {side} entry={entry} stop={stop} target={target}")

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