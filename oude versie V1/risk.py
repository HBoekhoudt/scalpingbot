from models import TvAlert

def resolve_qty(alert: TvAlert) -> float:
    if alert.qty is None:
        raise ValueError("qty ontbreekt. Stuur qty mee vanuit TradingView alert (aanrader).")
    if alert.qty <= 0:
        raise ValueError("qty moet > 0 zijn.")
    return float(alert.qty)
