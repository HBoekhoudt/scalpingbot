import logging
from ib_insync import IB

logger = logging.getLogger("ikbr_scalpingbot")

# ============================================================
# IBKR CONFIG
# ============================================================

IB_HOST = "127.0.0.1"
IB_PORT = 7497
IB_CLIENT_ID = 101

# ============================================================
# GLOBAL IB INSTANCE
# ============================================================

ib = IB()


# ============================================================
# GET IB CONNECTION
# ============================================================

def get_ib():

    if not ib.isConnected():

        logger.info("Connecting to IBKR...")

        ib.connect(
            IB_HOST,
            IB_PORT,
            clientId=IB_CLIENT_ID
        )

        logger.info("IBKR connected")

    return ib