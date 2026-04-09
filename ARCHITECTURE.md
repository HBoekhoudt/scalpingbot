# IBKR Scalping Bot — Architecture (DET Bot-Side OOP Version)

## System Overview

The scalping bot is a real-time automated trading engine designed for safe, deterministic, low-latency execution using Interactive Brokers.

The system receives compact market observations from TradingView/Pine and performs the actual DET interpretation inside the Python bot before any execution is allowed.

The architecture uses a **single object-oriented runtime engine** implemented in:

scalpingbot.py

This remains the **single source of truth for runtime state and execution logic**.

---

## Core Design Principle

**Pine observes.  
Bot decides.**

Pine may send:
- raw market data
- compact observations
- limited diagnostic hints

Pine may **not** act as authority for:
- DET validity decisions
- setup truths
- trigger truths
- blockers
- grades
- execution permission
- DET outcome labels

The Python bot must determine bot-side:
- context quality
- chop / non-chop
- late / non-late
- structural validity
- setup validity
- trigger validity
- REJECT / SHADOW / A / A+
- entry
- stop-loss
- R
- target
- execution yes/no

---

## Execution Flow

TradingView / Pine
↓
Webhook JSON
↓
FastAPI endpoint
↓
Signal normalization
↓
Bot-side DET reconstruction
↓
DET execution gate
↓
Execution Queue
↓
Worker Thread
↓
IBKR Order Submission

This pipeline is mandatory and must not be changed.

---

## Core Engine

All runtime logic is centralized in the class:

ScalpingBot

This class manages:

• IB connection  
• session health  
• contract registry / contract cache  
• order id management  
• signal normalization  
• DET assessment and classification  
• execution queue  
• worker thread  
• order execution  
• fill tracking  
• trade analysis / trade lifecycle  
• position and runtime state  

---

## Thread Model

The system uses a single worker thread responsible for all broker interaction and order execution.

The worker initializes its own asyncio event loop:

asyncio.set_event_loop(asyncio.new_event_loop())

This ensures compatibility with ib_insync.

The FastAPI webhook must remain non-blocking and may never place broker orders directly.

---

## Pine Responsibility

Pine is an **observation source**, not a decision engine.

### Pine must send only:
- required core observation fields
- optional observation fields
- limited diagnostic metadata if needed

### Pine must not send as authority:
- session_valid
- vwap_bias_valid
- structure_valid
- setup_valid
- trigger_valid
- execution_candidate_valid
- body_strength_valid
- blocker
- primary_blocker
- blocker_count
- reason_flags
- score
- grade
- candidate_grade
- tv_candidate_grade
- det_classification
- execution_grade
- A / A+ / SHADOW / REJECT
- semantically heavy DET shortcut qualifiers

---

## Required Core Input From Pine

### Meta / sequencing
- symbol
- timeframe
- bar_time_unix_ms

### Session / timing
- session_name
- minutes_from_open

### Price / VWAP
- price
- vwap_price
- vwap_slope_1m
- vwap_slope_5m
- distance_from_vwap_atr

### Expansion / chop basis
- ema_spread_atr_value
- htf_ema_spread_atr

### 1m trigger quality
- bar_range_1m
- bar_body_1m
- upper_wick_ratio_1m
- lower_wick_ratio_1m
- trigger_bar_high
- trigger_bar_low
- pullback_depth_1m
- trigger_close_location
- trigger_range_expansion

### 5m structure quality
- bar_range_5m
- bar_body_5m
- upper_wick_ratio_5m
- lower_wick_ratio_5m
- structure_anchor_low
- structure_anchor_high

### Sweep / rejection observations
- sweep_detected
- sweep_side
- rejection_detected

---

## Optional Observation Layer

The following may remain as optional, non-authoritative observations:

- above_vwap
- candidate_side
- body_strength_value
- rejection_wick_ratio
- trend_strength_5m
- reacceleration_detected
- htf_trend_up
- htf_trend_down
- htf_vwap_up
- htf_vwap_down
- htf_vwap_not_flat

These may assist diagnostics, but may never independently determine execution.

---

## Bot-Side DET Reconstruction

The bot reconstructs DET internally from Pine observations.

### Context
The bot determines:
- session quality
- opening timing quality
- VWAP bias
- directional pressure
- extension from VWAP
- chop / non-chop

### Structure
The bot determines:
- anchor quality
- sweep significance
- rejection quality
- structural validity

### Trigger
The bot determines:
- trigger quality
- candle conviction
- wick relevance by side
- pullback quality
- impulse / re-acceleration quality

### Classification
The bot determines:
- REJECT
- SHADOW
- EXECUTE_A
- EXECUTE_A_PLUS

### Execution planning
The bot determines:
- entry reference
- executable entry
- stop-loss
- 1R distance
- target
- execution yes/no

---

## Contract Management

Contracts must be created through:

bot.get_contract(symbol)

The system maintains:
- bot.contract_cache
- bot.contract_min_ticks

Contracts must be qualified before execution.
No ad hoc execution may bypass contract readiness checks.

---

## Session Health and Runtime Safety

The bot maintains explicit session health state, including:

- socket connectivity
- initialization completeness
- session health
- reconnect count
- event handler attachment state
- session recovery status

Execution is only allowed when session health and preflight checks pass.

---

## State Management

All runtime state must remain inside the ScalpingBot instance.

Examples include:

bot.ib  
bot.contract_cache  
bot.contract_min_ticks  
bot.execution_queue  
bot.trade_state  
bot.next_order_id  
bot.trade_analysis  
bot.order_to_trade  
bot.aggregate_stats  
bot.session_healthy  

No hidden external runtime state may be introduced.

---

## Deterministic Execution

The architecture ensures:

• webhook processing is non-blocking  
• normalization happens before queueing  
• DET execution gating happens before queueing  
• order execution occurs only in the worker thread  
• broker interaction is isolated  
• queue-worker execution remains deterministic  

---

## Startup and Recovery Model

At runtime, the bot must ensure:

1. FastAPI server is running
2. ScalpingBot is initialized
3. worker thread is running
4. IBKR connection is established
5. event handlers are attached
6. contract cache baseline is ready
7. session health is true before execution is allowed

Reconnect and forced session recovery must preserve this architecture.

---

## BRACKET ORDER MODEL (IBKR — OFFICIAL IMPLEMENTATION)

### Definition

The bot uses an IBKR-native bracket order structure consisting of:

1 × Parent (entry)  
1 × Take Profit (child)  
1 × Stop Loss (child)

Together these form one OCO-linked order group.

### Order Relationships

Parent-child relation:

TP.parentId = parent.orderId  
SL.parentId = parent.orderId

OrderId structure:

parentId = N  
TP = N + 1  
SL = N + 2

OrderIds are assigned deterministically by the bot.

### Transmit Chain (Critical)

The bracket is valid only with:

Parent → transmit = False  
TP → transmit = False  
SL → transmit = True

The final order (SL) transmits the complete bracket.

### Expected Order Lifecycle

#### Phase 1 — Submission
Parent → Submitted  
TP → PreSubmitted (whyHeld=child)  
SL → PreSubmitted (whyHeld=child / trigger)

#### Phase 2 — Entry Fill
When parent fills:
- TP and SL become active
- position becomes live

#### Phase 3 — Exit (OCO behavior)

Scenario A — TP hit:
- TP → Filled
- SL → Cancelled

Scenario B — SL hit:
- SL → Filled
- TP → Cancelled

IBKR handles OCO behavior natively.
The bot should observe and log this lifecycle, not manually emulate OCO logic.

---

## Required Event Observability

Minimum required runtime event coverage:

- orderStatusEvent
- openOrderEvent
- errorEvent
- execDetailsEvent
- commissionReportEvent

The bot must log enough detail to reconstruct:
- submission state
- fill state
- cancellation/rejection state
- trade summary
- execution quality

---

## Mandatory Safety Rules

1. No broker calls outside the execution worker  
2. No direct FastAPI trading logic  
3. No execution without full bracket structure  
4. No execution without session health and preflight success  
5. No Pine DET authority  
6. No architectural split away from single-file OOP without explicit redesign approval  

---

## Anti-Patterns (Forbidden)

- Parent transmit=True
- Child orders without parentId
- Orders outside execution worker
- Partial bracket submission
- Async broker execution outside worker model
- OrderId dependence on uncontrolled IBKR auto sequencing
- Pine-driven execution authority
- Pine-driven DET grades or blockers
- Validity flags from Pine treated as truth

---

## Summary

Pine:
observes and forwards compact market information

Bot:
normalizes, interprets, classifies, plans, and executes

Core principle:
**Pine observes. Bot decides.**