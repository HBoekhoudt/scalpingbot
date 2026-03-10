import logging

logger = logging.getLogger("ikbr_scalpingbot")

current_state = "IDLE"


def set_state(state):

    global current_state

    current_state = state

    logger.info(f"Trade state -> {state}")


def get_state():

    return current_state


def is_idle():

    return current_state == "IDLE"


def reset_state():

    global current_state

    current_state = "IDLE"

    logger.info("Trade state reset to IDLE")