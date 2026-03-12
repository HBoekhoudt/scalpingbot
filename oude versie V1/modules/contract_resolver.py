from ib_insync import Future, Forex
from modules.symbol_normalizer import normalize_symbol


def resolve_contract(symbol: str):
    """
    Translate a TradingView symbol into an IBKR contract object.

    Supported TradingView symbols (examples):
        MNQ1!   → MNQ
        MES1!   → MES
        M6E1!   → M6E
        FDAX1!  → FDXM
        EURUSD  → EURUSD
        EUR/USD → EURUSD
    """

    symbol = normalize_symbol(symbol)

    # ------------------------------------------------
    # Micro Nasdaq (CME / GLOBEX)
    # ------------------------------------------------
    if symbol == "MNQ":
        return Future(
            symbol="MNQ",
            lastTradeDateOrContractMonth="20260320",
            exchange="GLOBEX",
            currency="USD",
            tradingClass="MNQ"
        )

    # ------------------------------------------------
    # Micro S&P500 (CME / GLOBEX)
    # ------------------------------------------------
    if symbol == "MES":
        return Future(
            symbol="MES",
            lastTradeDateOrContractMonth="20260320",
            exchange="GLOBEX",
            currency="USD",
            tradingClass="MES"
        )

    # ------------------------------------------------
    # Micro EURUSD Future (CME / GLOBEX)
    # ------------------------------------------------
    if symbol == "M6E":
        return Future(
            symbol="M6E",
            lastTradeDateOrContractMonth="20260316",
            exchange="GLOBEX",
            currency="USD",
            tradingClass="M6E"
        )

    # ------------------------------------------------
    # Mini DAX (EUREX)
    # ------------------------------------------------
    if symbol == "FDXM":
        return Future(
            symbol="FDXM",
            lastTradeDateOrContractMonth="20260320",
            exchange="EUREX",
            currency="EUR",
            tradingClass="FDXM"
        )

    # ------------------------------------------------
    # Forex EURUSD
    # ------------------------------------------------
    if symbol == "EURUSD":
        return Forex("EURUSD")

    # ------------------------------------------------
    # Unsupported symbol
    # ------------------------------------------------
    raise ValueError(f"Unsupported symbol: {symbol}")