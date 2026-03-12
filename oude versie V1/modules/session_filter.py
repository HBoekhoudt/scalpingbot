import datetime
import pytz

UTC = pytz.utc

EU_START = 7
EU_END = 16

US_START = 13
US_END = 21


def session_filter():
    """
    Returns True if trading is allowed.
    Time is evaluated in UTC.
    """

    now = datetime.datetime.now(UTC)
    hour = now.hour

    if EU_START <= hour <= EU_END:
        return True

    if US_START <= hour <= US_END:
        return True

    return False