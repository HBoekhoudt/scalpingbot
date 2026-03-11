import logging
import time
import threading
from ib_insync import IB
from modules.order_id_manager import set_next_order_id
import asyncio

logger = logging.getLogger("ikbr_scalpingbot")

ib = IB()

IB_HOST = "127.0.0.1"
IB_PORT = 7497
IB_CLIENT_ID = 101

# Prevent multiple threads connecting simultaneously
connection_lock = threading.Lock()


def ensure_connection():
    """
    Ensure that the IB connection exists.
    Returns the shared IB instance.
    """

    global ib

    if ib.isConnected():
        return ib

    with connection_lock:

        if ib.isConnected():
            return ib

        try:

            # Always create a fresh event loop in this thread
            asyncio.set_event_loop(asyncio.new_event_loop())

            logger.info("Connecting to IBKR...")

            ib.connect(
                IB_HOST,
                IB_PORT,
                clientId=IB_CLIENT_ID
            )

            if ib.isConnected():
                logger.info("IBKR connected")
                set_next_order_id(ib.client.getReqId())
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