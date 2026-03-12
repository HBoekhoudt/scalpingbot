from fastapi import HTTPException

SUPPORTED_SYMBOLS = ["FDXM", "MES", "MNQ", "M6E"]


def parse_signal(body):

    symbol = body.get("symbol")
    side = body.get("side")
    grade = body.get("grade")
    entry = body.get("entry_price")

    if not all([symbol, side, grade, entry]):
        raise HTTPException(status_code=400, detail="Missing fields")

    symbol = symbol.upper()

    if symbol.endswith("1!"):
        symbol = symbol[:-2]

    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(status_code=400, detail="Unsupported instrument")

    side = side.lower()

    if side not in ["long", "short"]:
        raise HTTPException(status_code=400, detail="Invalid side")

    entry = float(entry)

    return {
        "symbol": symbol,
        "side": side,
        "grade": grade,
        "entry": entry
    }