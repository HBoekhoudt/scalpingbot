from modules.ib_watchdog import ensure_connection


def get_open_positions():
    """
    Returns a dictionary of open IB positions.

    Example:
    {
        "FDAX": 1,
        "M6E": -1
    }
    """

    ib = ensure_connection()

    positions = ib.positions()

    result = {}

    for p in positions:

        symbol = p.contract.symbol
        size = p.position

        if size != 0:
            result[symbol] = size

    return result