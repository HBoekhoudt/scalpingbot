# ibkr_service.py

import asyncio

# Zorg dat er een event loop is vóór de import van ib_insync / eventkit
try:
    asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from ib_insync import IB, Forex, util, LimitOrder, StopOrder

import os
import asyncio
from dotenv import load_dotenv
from ib_insync import IB, Forex, util, LimitOrder, StopOrder
from loguru import logger

load_dotenv()

IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "7497"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1001"))

class IBKRService:
    def __init__(self):
        self.ib = IB()
        self._connected = False
        self._lock = asyncio.Lock()

    async def connect(self):
        async with self._lock:
            if self._connected and self.ib.isConnected():
                return
            logger.info(f"Connecting IBKR {IB_HOST}:{IB_PORT} clientId={IB_CLIENT_ID}")
            await self.ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=5)
            self._connected = True
            logger.info("IBKR connected")

    async def disconnect(self):
        async with self._lock:
            if self.ib.isConnected():
                self.ib.disconnect()
            self._connected = False
            logger.info("IBKR disconnected")

    async def place_fx_bracket(self, symbol: str, action: str, qty: float, entry: float, stop: float, target: float):
        """
        action: BUY or SELL
        qty: base currency amount for FX (bijv 10_000 = 10k EUR in EURUSD)
        """
        await self.connect()

        contract = Forex(symbol)  # "EURUSD" -> Forex("EURUSD")
        qualified = await self.ib.qualifyContractsAsync(contract)
        if not qualified:
            raise RuntimeError(f"Contract niet te kwalificeren: {symbol}")

        # Parent order (entry) - limit
        parent = LimitOrder(action, qty, entry, transmit=False)

        # Child orders: bij BUY is stop = SELL stop, target = SELL limit
        exit_action = "SELL" if action == "BUY" else "BUY"

        take_profit = LimitOrder(exit_action, qty, target, transmit=False)
        stop_loss = StopOrder(exit_action, qty, stop, transmit=True)  # laatste order transmit=True

        # Koppel aan parent
        # IB geeft orderId pas bij placeOrder, dus eerst parent plaatsen
        trade_parent = self.ib.placeOrder(contract, parent)
        await asyncio.sleep(0.2)  # kort om orderId beschikbaar te krijgen
        parent_id = trade_parent.order.orderId

        take_profit.parentId = parent_id
        stop_loss.parentId = parent_id

        # Plaats children
        trade_tp = self.ib.placeOrder(contract, take_profit)
        trade_sl = self.ib.placeOrder(contract, stop_loss)

        logger.info(f"Bracket geplaatst {symbol} {action} qty={qty} entry={entry} stop={stop} target={target} parentId={parent_id}")
        return {
            "parentId": parent_id,
            "status_parent": trade_parent.orderStatus.status,
            "status_tp": trade_tp.orderStatus.status,
            "status_sl": trade_sl.orderStatus.status,
        }
