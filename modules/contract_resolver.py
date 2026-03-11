from ib_insync import Future, Forex
from datetime import datetime
from modules.symbol_normalizer import normalize_symbol


def _front_month():
    """
    Determine the current front-month futures contract.
    Futures roll quarterly: Mar, Jun, Sep, Dec.
    """

    now = datetime.utcnow()

    year = now.year
    month = now.month

    if month <= 3:
        expiry_month = "03"
    elif month <= 6:
        expiry_month = "06"
    elif month <= 9:
        expiry_month = "09"
    else:
        expiry_month = "12"

    return f"{year}{expiry_month}"


def resolve_contract(symbol):
    """
    Translate TradingView symbol into an IBKR contract object.
    """

    symbol = normalize_symbol(symbol)
    expiry = _front_month()

    # ------------------------------------------------
    # DAX Mini (EUREX)
    # ------------------------------------------------
    if symbol in ["FDXM", "DAX"]:
        return Future(
            symbol="FDXM",
            exchange="EUREX",
            currency="EUR",
            lastTradeDateOrContractMonth=expiry
        )

    # ------------------------------------------------
    # Micro ES
    # ------------------------------------------------
    if symbol == "MES":
        return Future(
            symbol="MES",
            exchange="GLOBEX",
            currency="USD",
            lastTradeDateOrContractMonth=expiry
        )

    # ------------------------------------------------
    # Micro Nasdaq
    # ------------------------------------------------
    if symbol == "MNQ":
        return Future(
            symbol="MNQ",
            exchange="GLOBEX",
            currency="USD",
            lastTradeDateOrContractMonth=expiry
        )

    # ------------------------------------------------
    # Micro EUR/USD
    # ------------------------------------------------
    if symbol == "M6E":
        return Future(
            symbol="M6E",
            exchange="GLOBEX",
            currency="USD",
            lastTradeDateOrContractMonth=expiry
        )

    # ------------------------------------------------
    # Forex EURUSD
    # ------------------------------------------------
    if symbol in ["EURUSD", "EUR/USD"]:
        return Forex("EURUSD")

    # ------------------------------------------------
    # Unsupported symbol
    # ------------------------------------------------
    raise ValueError(f"Unsupported symbol: {symbol}")