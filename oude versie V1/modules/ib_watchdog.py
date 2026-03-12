import logging
import time
import threading
from datetime import datetime
from ib_insync import IB, Future
from modules.order_id_manager import set_next_order_id
import asyncio

logger = logging.getLogger("ikbr_scalpingbot")

ib = IB()

IB_HOST = "127.0.0.1"
IB_PORT = 7497
IB_CLIENT_ID = 101

# Prevent multiple threads connecting simultaneously
connection_lock = threading.Lock()

# Contract cache
contract_cache = {}


def _front_month():
    """
    Determine the current front-month futures contract.
    Futures roll quarterly: Mar, Jun, Sep, Dec.
    IBKR qualification expects expiry format YYYYMMDD.
    """

    now = datetime.utcnow()

    year = now.year
    month = now.month

    if month <= 3:
        expiry = f"{year}0320"
    elif month <= 6:
        expiry = f"{year}0620"
    elif month <= 9:
        expiry = f"{year}0920"
    else:
        expiry = f"{year}1220"

    return expiry


def ensure_connection():
    """
    Ensure that the IB connection exists.
    Returns the shared IB instance.
    """

    global ib
    global contract_cache

    if ib.isConnected():
        return ib

    with connection_lock:

        if ib.isConnected():
            return ib

        try:

            # Create event loop for this thread
            asyncio.set_event_loop(asyncio.new_event_loop())

            logger.info("Connecting to IBKR...")

            ib.connect(
                IB_HOST,
                IB_PORT,
                clientId=IB_CLIENT_ID
            )

            if ib.isConnected():

                logger.info("IBKR connected")

                # ------------------------------------------------
                # Attach IBKR logging callbacks (only once)
                # ------------------------------------------------

                if not hasattr(ib, "_logging_attached"):

                    def on_order_status(trade):
                        logger.info(f"ORDER STATUS UPDATE: {trade.orderStatus}")

                    def on_exec_details(trade, fill):
                        logger.info(f"FILL EVENT: {fill}")

                    def on_ib_error(reqId, errorCode, errorString, contract):
                        logger.error(f"IB ERROR {errorCode}: {errorString}")

                    ib.orderStatusEvent += on_order_status
                    ib.execDetailsEvent += on_exec_details
                    ib.errorEvent += on_ib_error

                    ib._logging_attached = True

                # ------------------------------------------------
                # Initialize order ID (only once)
                # ------------------------------------------------

                if not hasattr(ib, "_order_id_initialized"):

                    order_id = ib.client.getReqId()
                    set_next_order_id(order_id)

                    logger.info(f"Order ID initialized to {order_id}")

                    ib._order_id_initialized = True

                # ------------------------------------------------
                # Qualify futures contracts at startup
                # ------------------------------------------------

                logger.info("Qualifying futures contracts...")

                expiry = _front_month()

                mnq = Future(
                    symbol="MNQ",
                    exchange="GLOBEX",
                    currency="USD",
                    tradingClass="MNQ",
                    lastTradeDateOrContractMonth=expiry
                )

                mes = Future(
                    symbol="MES",
                    exchange="GLOBEX",
                    currency="USD",
                    tradingClass="MES",
                    lastTradeDateOrContractMonth=expiry
                )

                m6e = Future(
                    symbol="M6E",
                    exchange="GLOBEX",
                    currency="USD",
                    tradingClass="M6E",
                    lastTradeDateOrContractMonth=expiry
                )

                fdxm = Future(
                    symbol="FDXM",
                    exchange="EUREX",
                    currency="EUR",
                    tradingClass="FDXM",
                    lastTradeDateOrContractMonth=expiry
                )

                qualified = ib.qualifyContracts(
                    mnq,
                    mes,
                    m6e,
                    fdxm
                )

                for contract in qualified:
                    contract_cache[contract.symbol] = contract

                logger.info(
                    f"Contract cache initialized: {list(contract_cache.keys())}"
                )

            else:
                logger.error("IBKR connection failed")

        except Exception as e:

            logger.error(f"IB connection failed: {e}")

    return ib


def is_ib_connected():
    return ib.isConnected()


def watchdog_loop():
    """
    Background watchdog that reconnects if IB disconnects.
    """

    while True:

        try:

            if not ib.isConnected():

                logger.warning("IB disconnected, reconnecting...")

                ensure_connection()

        except Exception as e:

            logger.error(f"Watchdog error: {e}")

        time.sleep(5)