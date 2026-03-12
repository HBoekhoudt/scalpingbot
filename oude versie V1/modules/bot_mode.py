BOT_MODE = "PAPER"
# BOT_MODE = "LIVE"


def is_live():
    return BOT_MODE == "LIVE"


def is_paper():
    return BOT_MODE == "PAPER"