from ib_insync import Future
from modules.ibkr_connection import get_ib
import logging

logger = logging.getLogger("ikbr_scalpingbot")

EXCHANGE_MAP = {
    "FDXM": "EUREX",
    "MES": "CME",
    "MNQ": "CME",
    "M6E": "CME"
}


def place_bracket_order(
    symbol,
    side,
    quantity,
    entry,
    target,
    stop
):

    ib = get_ib()

    exchange = EXCHANGE_MAP[symbol]

    contract = Future(
        symbol=symbol,
        exchange=exchange
    )

    ib.qualifyContracts(contract)

    action = "BUY" if side == "long" else "SELL"

    parent, tp, sl = ib.bracketOrder(
        action=action,
        quantity=quantity,
        limitPrice=entry,
        takeProfitPrice=target,
        stopLossPrice=stop
    )

    ib.placeOrder(contract, parent)
    ib.placeOrder(contract, tp)
    ib.placeOrder(contract, sl)

    logger.info("Bracket order placed")