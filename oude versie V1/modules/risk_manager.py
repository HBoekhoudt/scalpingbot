# Tick sizes
TICK_SIZE = {
    "FDXM": 0.5,
    "MES": 0.25,
    "MNQ": 0.25,
    "M6E": 0.00005
}

# Tick value (money per tick)
TICK_VALUE = {
    "FDXM": 5,
    "MES": 1.25,
    "MNQ": 0.5,
    "M6E": 6.25
}

RR_BY_GRADE = {
    "A": 2.0,
    "A+": 2.5
}


def calculate_position_size(
    symbol,
    side,
    entry,
    account_size,
    risk_pct,
    stop_ticks
):

    tick_size = TICK_SIZE[symbol]
    tick_value = TICK_VALUE[symbol]

    stop_distance = stop_ticks * tick_size

    if side == "long":

        stop_price = entry - stop_distance

    else:

        stop_price = entry + stop_distance

    risk_amount = account_size * risk_pct

    risk_per_contract = stop_ticks * tick_value

    quantity = max(1, int(risk_amount / risk_per_contract))

    rr = RR_BY_GRADE["A"]

    if side == "long":

        target_price = entry + (stop_distance * rr)

    else:

        target_price = entry - (stop_distance * rr)

    return quantity, stop_price, target_price