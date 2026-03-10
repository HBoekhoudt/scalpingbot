import threading
import logging

logger = logging.getLogger("ikbr_scalpingbot")

_lock = threading.Lock()
_next_order_id = None


def set_next_order_id(order_id: int):
    """
    Set the next valid order ID received from IBKR.
    This is typically called when IBKR sends nextValidId.
    """

    global _next_order_id

    with _lock:
        _next_order_id = order_id
        logger.info(f"Order ID initialized to {order_id}")


def get_next_order_id() -> int:
    """
    Retrieve the next order ID in a thread-safe manner.
    Ensures no duplicate order IDs are issued.
    """

    global _next_order_id

    with _lock:
        if _next_order_id is None:
            raise RuntimeError("Order ID not initialized. Waiting for IBKR nextValidId.")

        order_id = _next_order_id
        _next_order_id += 1

        return order_id


def reset_order_id(order_id: int):
    """
    Reset the order ID after an IBKR reconnect event.
    """

    global _next_order_id

    with _lock:
        _next_order_id = order_id
        logger.info(f"Order ID reset to {order_id}")