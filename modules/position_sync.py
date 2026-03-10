from modules.position_manager import get_open_positions


def has_open_position():
    """
    Returns True if any position is open in IB.
    """

    positions = get_open_positions()

    return len(positions) > 0


def get_position_size(symbol):
    """
    Returns the position size for a specific symbol.
    """

    positions = get_open_positions()

    if symbol in positions:
        return positions[symbol]

    return 0