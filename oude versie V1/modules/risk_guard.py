import logging

logger = logging.getLogger("ikbr_scalpingbot")

# maximale verliezen per dag
MAX_DAILY_LOSSES = 3

daily_losses = 0


def register_loss():

    global daily_losses

    daily_losses += 1

    logger.warning(f"Daily losses {daily_losses}")


def reset_daily_losses():

    global daily_losses

    daily_losses = 0

    logger.info("Daily loss counter reset")


def is_trading_blocked():

    if daily_losses >= MAX_DAILY_LOSSES:

        logger.error("Daily loss limit reached")

        return True

    return False