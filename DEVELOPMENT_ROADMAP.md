# Scalping Bot Development Roadmap

## Phase 1 — Core Execution Engine
✔ OOP scalping engine (scalpingbot.py)  
✔ IBKR connection  
✔ contract qualification  
✔ TradingView webhook  
✔ execution queue  
✔ worker thread  
✔ bracket order execution  

---

## Phase 2 — Stability

• improved reconnect logic  
• execution logging  
• contract registry improvements  
• duplicate signal protection  

---

## Phase 3 — Observability

Add runtime endpoints:

/bot_status
/contracts
/queue
/open_positions
/last_signal

Add structured logs.

---

## Phase 4 — Risk Engine

• max position size
• max daily loss
• session filter
• duplicate order protection

---

## Phase 5 — Multi-Market Trading

Support:

MES  
MNQ  
M6E  
FDXM

Simultaneous execution.

---

## Phase 6 — Strategy Layer

Strategy modules:

VWAP scalp  
liquidity sweep  
rejection trades

---

## Phase 7 — Production Hardening

• crash recovery
• persistent position tracking
• alert replay protection
• latency monitoring