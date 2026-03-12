# Development Roadmap – IBKR Scalping Bot

This document defines the planned engineering roadmap for building and stabilizing the IBKR scalping bot.

The roadmap divides development into **small, safe engineering sessions**.  
Each session modifies or creates **only one module** to prevent regressions and maintain architecture stability.

All development must follow the engineering protocol defined in:

ENGINEERING_RULES.md


==================================================
PROJECT CONTEXT
==================================================

This repository contains a professional IBKR scalping bot.

Execution pipeline:

TradingView  
→ webhook  
→ resolve_contract  
→ execution_queue  
→ execution_engine  
→ IBKR  

This pipeline must **never change**.

Every engineering change must preserve this execution flow.


==================================================
CURRENT STATE
==================================================

Existing modules:

app.py

modules/

- bot_mode.py  
- contract_resolver.py  
- execution_engine.py  
- execution_queue.py  
- fill_tracker.py  
- ib_watchdog.py  
- position_manager.py  
- position_sync.py  
- session_filter.py  

The system already supports:

- FastAPI webhook for TradingView alerts
- execution queue for deterministic trade processing
- IBKR watchdog connection monitoring
- fill tracking
- PAPER vs LIVE trading modes


==================================================
ROADMAP STRUCTURE
==================================================

Development will proceed through **sequential engineering sessions**.

Each session may:

- modify **one module**
OR
- create **one module**

Never both.

This rule ensures that each change can be tested and validated independently.


==================================================
ENGINEERING SESSIONS
==================================================


--------------------------------------------------
Session 1
--------------------------------------------------

Create module:

symbol_normalizer.py

Purpose:

Normalize TradingView symbols before contract resolution.

Examples:

FDAX1! → FDXM  
MES1! → MES  
MNQ1! → MNQ  


--------------------------------------------------
Session 2
--------------------------------------------------

Integrate symbol_normalizer into:

contract_resolver.py

Purpose:

Ensure all TradingView symbols are normalized before contract creation.


--------------------------------------------------
Session 3
--------------------------------------------------

Create module:

order_manager.py

Purpose:

Track the lifecycle of orders.

Responsibilities:

- entry sent
- entry filled
- TP/SL active
- trade closed


--------------------------------------------------
Session 4
--------------------------------------------------

Create module:

signal_guard.py

Purpose:

Prevent duplicate TradingView signals.

TradingView alerts may fire multiple times during the same market condition.


--------------------------------------------------
Session 5
--------------------------------------------------

Create module:

order_sync.py

Purpose:

Recover order state after IBKR reconnect.

This module prevents:

- duplicate order submission
- ghost positions


--------------------------------------------------
Session 6
--------------------------------------------------

Create module:

risk_manager.py

Purpose:

Apply risk controls in LIVE mode.

Examples:

- maximum open trades
- daily loss limits
- position sizing rules


--------------------------------------------------
Session 7
--------------------------------------------------

Improve module:

fill_tracker.py

Purpose:

Track execution performance.

Examples:

- PnL tracking
- fill latency
- execution statistics


--------------------------------------------------
Session 8
--------------------------------------------------

Add structured trade logging.

Purpose:

Create reproducible trade history for analysis and debugging.

Logs should capture:

- signal time
- entry price
- fill price
- exit price
- latency metrics


--------------------------------------------------
Session 9
--------------------------------------------------

Improve PAPER mode testing capabilities.

Purpose:

Enable stress testing of order execution.

Examples:

- rapid signal testing
- simulated latency
- queue load testing


--------------------------------------------------
Session 10
--------------------------------------------------

Live trading readiness check.

Purpose:

Verify that all failure modes are handled before enabling LIVE trading.

Verification checklist:

- duplicate signal protection
- reconnect recovery
- bracket order reliability
- position synchronization
- risk management enforcement


==================================================
FINAL GOAL
==================================================

The final system should provide a **stable and deterministic IBKR trading engine** capable of operating safely in live markets.

The architecture is designed to prevent the most common automated trading failures:

- duplicate orders
- race conditions
- ghost positions
- reconnect bugs
- symbol mismatches

All future engineering work must follow this roadmap to maintain system stability.