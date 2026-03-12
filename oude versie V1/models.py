from pydantic import BaseModel, Field
from typing import Literal, Optional

class TvAlert(BaseModel):
    # TradingView stuurt dit
    symbol: str = Field(..., examples=["EURUSD"])
    action: Literal["BUY", "SELL"]
    setup: Literal["A", "A+"]  # jouw protocol label
    entry: float
    stop: float
    target: float

    # Je kunt qty door Pine laten sturen (aanrader, scheelt pip-value gedoe)
    qty: Optional[float] = None

    # Of risk-based laten berekenen
    risk_eur: Optional[float] = None

    # extra (optioneel)
    comment: Optional[str] = None
