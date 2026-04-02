# IBKR Scalping Bot — Architecture (OOP Version)

## System Overview

The scalping bot is a real-time automated trading engine designed for low-latency execution using Interactive Brokers.

The system receives trading signals from TradingView and executes bracket orders on IBKR.

The architecture uses a **single object-oriented runtime engine** implemented in:

scalpingbot.py

This ensures a **single source of truth for runtime state**.

---

# Execution Flow

TradingView
↓
Webhook
↓
FastAPI endpoint
↓
ScalpingBot.enqueue_signal()
↓
Execution Queue
↓
Worker Thread
↓
IBKR Order Submission

---

# Core Engine

All runtime logic is centralized in the class:

ScalpingBot

This class manages:

• IB connection  
• contract registry  
• order id management  
• signal queue  
• worker thread  
• order execution  
• fill tracking  
• position tracking  

---

# Thread Model

The system uses a single worker thread responsible for order execution.

The worker initializes its own asyncio event loop:

asyncio.set_event_loop(asyncio.new_event_loop())

This ensures compatibility with ib_insync.

---

# Contract Management

Contracts are created through:

bot.get_contract(symbol)

The system maintains a contract cache:

bot.contract_cache

This prevents inconsistent contract construction.

---

# Order Model

Orders are submitted as bracket orders:

Entry
Stop
Target

The final order in the bracket has:

transmit = True

---

# State Management

Runtime state is stored inside the ScalpingBot instance.

Example:

bot.ib  
bot.contract_cache  
bot.execution_queue  
bot.trade_state  
bot.positions

---

# Startup Sequence

1. Start FastAPI server
2. Initialize ScalpingBot
3. Connect to IBKR
4. Qualify futures contracts
5. Start execution worker

---

# Deterministic Execution

The architecture ensures that:

• webhook processing is non-blocking  
• order execution occurs only in the worker thread  
• broker interaction is isolated

==========================================================
BRACKET ORDER MODEL (IBKR — OFFICIËLE IMPLEMENTATIE)
==========================================================
📌 Definitie

De bot gebruikt een IBKR-native bracket order structuur bestaande uit:

1 × Parent (entry)
1 × Take Profit (child)
1 × Stop Loss (child)

Deze vormen samen een OCO-gekoppelde ordergroep.

🔗 ORDER RELATIES

Parent-child relatie:

TP.parentId = parent.orderId
SL.parentId = parent.orderId

OrderId structuur:

parentId = N
TP = N + 1
SL = N + 2

👉 OrderId’s worden deterministisch en handmatig toegewezen

🚀 TRANSMIT CHAIN (KRITISCH)

De bracket wordt alleen correct opgebouwd met:

Parent → transmit = False
TP → transmit = False
SL → transmit = True

👉 De laatste order (SL) triggert verzending van de volledige bracket

🔄 ORDER FLOW (EXPECTED LIFECYCLE)
Fase 1 — Submission
Parent → Submitted
TP → PreSubmitted (whyHeld=child)
SL → PreSubmitted (whyHeld=child,trigger)
Fase 2 — Entry Fill

Wanneer parent gevuld wordt:

TP en SL worden actief
Positie wordt geopend
Fase 3 — Exit (OCO gedrag)

Scenario A — TP hit:

TP → Filled
SL → Cancelled

Scenario B — SL hit:

SL → Filled
TP → Cancelled

👉 IBKR handelt OCO volledig af (geen bot-interventie nodig)

🔍 BEWEZEN GEDRAG (P026 VALIDATIE)

Tijdens live test:

Entry gevuld
Stop Loss gevuld
Take Profit automatisch geannuleerd

👉 Dit bevestigt:

correcte parent-child linking
correcte transmit chain
correcte IBKR OCO handling
⚠️ BELANGRIJKE RANDVOORWAARDEN
1. Geen dynamische contracten

Alle contracts moeten vooraf gekwalificeerd zijn.

2. Geen order zonder volledige bracket

Elke trade = verplicht:

entry
stop-loss
take-profit
3. Worker-only execution

Alle bracket orders worden uitsluitend geplaatst door:

👉 execution worker thread

4. Deterministische orderId generatie

Geen reliance op IBKR auto IDs

5. Volledige event observability

Minimaal vereist:

orderStatusEvent
openOrderEvent
errorEvent
execDetails (fills)
🚫 ANTI-PATTERNS (VERBODEN)
Parent transmit=True
Child orders zonder parentId
Orders buiten execution worker
Partial bracket submission
Async order placement
OrderId afhankelijk van IBKR