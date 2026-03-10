from ib_insync import Future, Forex
from modules.symbol_normalizer import normalize_symbol


def resolve_contract(symbol):
    """
    Translate TradingView symbol into an IBKR contract object.
    """

    symbol = normalize_symbol(symbol)

    # ------------------------------------------------
    # DAX Mini (EUREX)
    # ------------------------------------------------
    if symbol in ["FDXM", "DAX"]:
        return Future(
            symbol="FDXM",
            exchange="EUREX",
            currency="EUR"
        )

    # ------------------------------------------------
    # Micro ES (S&P500)
    # ------------------------------------------------
    if symbol == "MES":
        return Future(
            symbol="MES",
            exchange="GLOBEX",
            currency="USD"
        )

    # ------------------------------------------------
    # Micro Nasdaq
    # ------------------------------------------------
    if symbol == "MNQ":
        return Future(
            symbol="MNQ",
            exchange="GLOBEX",
            currency="USD"
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