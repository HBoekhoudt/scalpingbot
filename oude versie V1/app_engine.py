"""
Skeleton voor de A/A+ scalping engine.

Rol van dit bestand:
- Ontvangt TradingView webhooks (sensor).
- Bouwt context (sessie, trend, etc.).
- Past jouw Daytrading Expert Training toe (A/A+ / geen trade).
- Maakt een risk- en orderplan.
- (Optioneel) stuurt na human confirm naar IBKR.

Alle "echte" logica is gemarkeerd met TODO.
"""

from __future__ import annotations

import os
from typing import Optional, Literal

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field, validator
from datetime import datetime, timezone


# ============================================================
# 1. Settings / config
# ============================================================

class Settings(BaseModel):
    webhook_secret: str = Field(..., description="Secret die vanuit TradingView wordt meegestuurd")
    environment: Literal["paper", "live"] = "paper"
    account_size_eur: float = 170_000.0   # voorbeeld
    risk_pct_A: float = 0.003             # 0.3%
    risk_pct_A_plus: float = 0.005        # 0.5%

    class Config:
        arbitrary_types_allowed = True


def get_settings() -> Settings:
    # TODO: haal dit desnoods uit .env via python-dotenv
    secret = os.getenv("WEBHOOK_SECRET")
    if not secret:
        raise RuntimeError("WEBHOOK_SECRET is niet gezet in de environment")

    return Settings(webhook_secret=secret)


# ============================================================
# 2. Pydantic modellen (data-structuren)
# ============================================================

class TVSignal(BaseModel):
    """
    Ruwe payload vanuit TradingView.
    Houd dit zo simpel mogelijk; TV is alleen sensor.
    """
    secret: str
    source: Literal["tradingview"] = "tradingview"

    symbol: str                 # "{{ticker}}"
    timeframe: str              # "{{interval}}"
    price: float                # {{close}}
    direction_hint: Literal["LONG", "SHORT"]
    pattern: str                # bijv. "sweep_reject", "vwap_tap", ...

    session: Optional[str] = None   # "EU", "US", ...
    timestamp: Optional[datetime] = None

    @validator("timestamp", pre=True, always=True)
    def default_timestamp(cls, v):
        # Als TradingView geen tijd meestuurt, pakken we nu()
        return v or datetime.now(timezone.utc)


class TradeContext(BaseModel):
    """
    Alle informatie die jouw brein nodig heeft rondom het signaal.
    Dit wordt opgebouwd uit:
    - TVSignal
    - interne state (bijv. sessie-tijd, VWAP-bias, etc.)
    """
    symbol: str
    timeframe: str
    price: float
    direction_hint: Literal["LONG", "SHORT"]
    session: Optional[str]

    # TODO: vul aan met jouw echte context-velden
    vwap_bias: Optional[Literal["UP", "DOWN", "FLAT"]] = None
    trend_5m: Optional[Literal["UP", "DOWN", "RANGE"]] = None
    trend_1m: Optional[Literal["UP", "DOWN", "RANGE"]] = None
    time_in_session_min: Optional[int] = None
    adr_used_pct: Optional[float] = None


class SetupDecision(BaseModel):
    """
    Uitkomst van je A/A+ oordeel.
    """
    allowed: bool
    quality: Optional[Literal["A", "A+"]] = None
    reason: str

    preferred_entry: Optional[Literal["pullback", "breakout"]] = None
    expected_direction: Optional[Literal["LONG", "SHORT"]] = None


class RiskPlan(BaseModel):
    """
    Risk- en position sizing plan.
    """
    risk_eur: float
    risk_pct: float
    stop_distance: float        # in prijs (bijv. 8 punten voor FDAX)
    size: float                 # contracten / lots
    account_size_eur: float


class OrderPlan(BaseModel):
    """
    Concreet ordervoorstel dat aan jou wordt voorgelegd.
    """
    symbol: str
    direction: Literal["LONG", "SHORT"]
    entry_price: float
    stop_price: float
    target_price: float
    size: float

    quality: Optional[Literal["A", "A+"]] = None
    reason: Optional[str] = None


class JournalEntry(BaseModel):
    """
    Alles wat we willen loggen over een (potentiële) trade.
    """
    tv_signal: TVSignal
    context: TradeContext
    setup: SetupDecision
    risk: Optional[RiskPlan] = None
    order_plan: Optional[OrderPlan] = None
    human_decision: Optional[Literal["confirm", "reject"]] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ============================================================
# 3. Core componenten (met TODO's)
# ============================================================

class ContextBuilder:
    """
    Bouwt TradeContext uit het ruwe TV-signaal en eventueel extra data.
    """
    def build(self, signal: TVSignal) -> TradeContext:
        # TODO: zolang we nog geen externe data gebruiken,
        #       vullen we een minimalistische context.
        return TradeContext(
            symbol=signal.symbol,
            timeframe=signal.timeframe,
            price=signal.price,
            direction_hint=signal.direction_hint,
            session=signal.session,
            # vwap_bias=...,
            # trend_5m=...,
            # trend_1m=...,
            # time_in_session_min=...,
            # adr_used_pct=...,
        )


class StrategyEngine:
    """
    Hier landt jouw Daytrading Expert Training.
    TVSignal + TradeContext -> SetupDecision.
    """
    def evaluate(self, signal: TVSignal, ctx: TradeContext) -> SetupDecision:
        # TODO: implementeer jouw echte regels hier.

        # Voor nu: dummy-logica zodat de flow werkt.
        # Bijvoorbeeld: alleen A+ als pattern == "sweep_reject"
        if signal.pattern == "sweep_reject":
            return SetupDecision(
                allowed=True,
                quality="A+",
                reason="Dummy: pattern == sweep_reject",
                preferred_entry="pullback",
                expected_direction=signal.direction_hint,
            )

        # Alles anders: geen trade
        return SetupDecision(
            allowed=False,
            quality=None,
            reason="Dummy: geen setup-criteria gehaald",
            preferred_entry=None,
            expected_direction=None,
        )


class RiskManager:
    """
    Berekent risico en size op basis van A/A+ + stopafstand.
    """
    def __init__(self, settings: Settings):
        self.settings = settings

    def plan_risk(self, setup: SetupDecision, ctx: TradeContext) -> Optional[RiskPlan]:
        if not setup.allowed or setup.quality is None:
            return None

        # TODO: echte stopafstand (bijv. 8 punten FDAX).
        stop_distance = 8.0

        if setup.quality == "A":
            risk_pct = self.settings.risk_pct_A
        else:  # "A+"
            risk_pct = self.settings.risk_pct_A_plus

        risk_eur = self.settings.account_size_eur * risk_pct

        # TODO: echte contractsize berekening per instrument.
        # Voor nu: 1 punt = 25 EUR (FDAX) -> size = risk_eur / (stop_distance * 25)
        point_value = 25.0
        size = risk_eur / (stop_distance * point_value)

        return RiskPlan(
            risk_eur=risk_eur,
            risk_pct=risk_pct,
            stop_distance=stop_distance,
            size=size,
            account_size_eur=self.settings.account_size_eur,
        )


class ExecutionPlanner:
    """
    Zet context + setup + risk om in een ordervoorstel.
    Geen broker-logica hier; puur "wat voor order zouden we willen?".
    """
    def make_order_plan(
        self,
        signal: TVSignal,
        ctx: TradeContext,
        setup: SetupDecision,
        risk: Optional[RiskPlan],
    ) -> Optional[OrderPlan]:
        if not setup.allowed or risk is None:
            return None

        direction = setup.expected_direction or signal.direction_hint

        # TODO: echte entry/stop/target formules (break/pullback etc).
        entry_price = signal.price
        if direction == "LONG":
            stop_price = entry_price - risk.stop_distance
            target_price = entry_price + 2 * risk.stop_distance
        else:
            stop_price = entry_price + risk.stop_distance
            target_price = entry_price - 2 * risk.stop_distance

        return OrderPlan(
            symbol=signal.symbol,
            direction=direction,
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            size=risk.size,
            quality=setup.quality,
            reason=setup.reason,
        )


class BrokerAdapter:
    """
    Wrapper om later IBKR-logica in te steken.
    Nu nog een stub; we houden 'm bewust dom.
    """
    def send_order(self, plan: OrderPlan) -> None:
        # TODO: implementeer IBKR bracket order via ib_insync.
        # Voor nu: alleen printen zodat de flow testbaar is.
        print(f"[BROKER] Would send order: {plan}")


class Journal:
    """
    Simpele journaling laag. Later kun je dit naar een DB of file schrijven.
    """
    def log(self, entry: JournalEntry) -> None:
        # TODO: echte opslag (DB, JSON-lines file, etc.)
        print(f"[JOURNAL] {entry.json()}")


# ============================================================
# 4. FastAPI setup + routes
# ============================================================

app = FastAPI(title="IKBR Scalpingbot A/A+ Engine")


def get_components(settings: Settings = Depends(get_settings)):
    """
    Kleine factory om alle componenten bij elkaar te houden.
    Handig voor dependency injection in FastAPI.
    """
    ctx_builder = ContextBuilder()
    engine = StrategyEngine()
    risk_manager = RiskManager(settings)
    exec_planner = ExecutionPlanner()
    broker = BrokerAdapter()
    journal = Journal()

    return {
        "settings": settings,
        "ctx_builder": ctx_builder,
        "engine": engine,
        "risk_manager": risk_manager,
        "exec_planner": exec_planner,
        "broker": broker,
        "journal": journal,
    }


@app.post("/webhook/tradingview")
def receive_tradingview_webhook(
    payload: TVSignal,
    comps: dict = Depends(get_components),
):
    settings: Settings = comps["settings"]
    ctx_builder: ContextBuilder = comps["ctx_builder"]
    engine: StrategyEngine = comps["engine"]
    risk_manager: RiskManager = comps["risk_manager"]
    exec_planner: ExecutionPlanner = comps["exec_planner"]
    broker: BrokerAdapter = comps["broker"]
    journal: Journal = comps["journal"]

    # 1) Secret check
    if payload.secret != settings.webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    # 2) Context bouwen
    ctx = ctx_builder.build(payload)

    # 3) A/A+ oordeel
    setup = engine.evaluate(payload, ctx)

    # 4) Risk plan
    risk = risk_manager.plan_risk(setup, ctx)

    # 5) Order plan
    order_plan = exec_planner.make_order_plan(payload, ctx, setup, risk)

    # 6) Journal entry (nog vóór human confirm / broker)
    journal_entry = JournalEntry(
        tv_signal=payload,
        context=ctx,
        setup=setup,
        risk=risk,
        order_plan=order_plan,
        human_decision=None,  # later invullen als je een confirm-step maakt
    )
    journal.log(journal_entry)

    # 7) HUMAN-IN-THE-LOOP
    # Voor nu sturen we NIET automatisch naar de broker.
    # Je kunt hier:
    # - alleen het voorstel teruggeven aan bijv. een kleine frontend
    # - of tijdelijk direct broker.send_order(order_plan) doen voor test.

    # if order_plan is not None:
    #     broker.send_order(order_plan)

    return {
        "status": "ok",
        "setup": setup.dict(),
        "risk": risk.dict() if risk else None,
        "order_plan": order_plan.dict() if order_plan else None,
    }