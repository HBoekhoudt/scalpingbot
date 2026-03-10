import logging
from ib_insync import IB

logger = logging.getLogger("ikbr_scalpingbot")

ib = IB()

IB_HOST = "127.0.0.1"
IB_PORT = 7497
IB_CLIENT_ID = 101


def ensure_connection():
    """
    Ensure that the IB connection exists.
    Returns the shared IB instance.
    """

    global ib

    if ib.isConnected():
        return ib

    try:
        logger.info("Connecting to IBKR...")

        ib.connect(
            IB_HOST,
            IB_PORT,
            clientId=IB_CLIENT_ID
        )

        logger.info("IBKR connected")

    except Exception as e:
        logger.error(f"IB connection failed: {e}")
        raise

    return ib


def is_ib_connected():
    return ib.isConnected()