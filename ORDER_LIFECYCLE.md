# Order Lifecycle — IBKR OOP Scalping Bot

This document describes the complete lifecycle of a trade within the scalping bot.

The lifecycle begins when a TradingView signal arrives and ends when the position is closed.

---

# Overview

TradingView
↓
Webhook received
↓
Signal validation
↓
Queue insertion
↓
Execution worker
↓
Bracket order submission
↓
Fill events
↓
Position closed

---

# Step 1 — TradingView Signal

TradingView sends a webhook containing:

• symbol  
• side (long / short)  
• entry price  
• timeframe  
• timestamp  
• grade (A / A+)

Example payload:

{
  "symbol": "MES1!",
  "side": "short",
  "entry_price": 6724.0,
  "grade": "A"
}

---

# Step 2 — Webhook Handler

FastAPI receives the webhook.

Responsibilities:

• validate payload  
• verify secret  
• reject malformed signals  

The webhook must never execute trading logic.

Instead it calls:

bot.enqueue_signal(signal)

---

# Step 3 — Signal Queue

Signals are stored in:

bot.execution_queue

Queue ensures:

• deterministic execution  
• FIFO order processing  
• thread-safe signal handling

---

# Step 4 — Execution Worker

The worker thread processes queued signals.

Responsibilities:

• contract resolution  
• bracket order creation  
• IBKR submission  

Worker thread initializes its own event loop:

asyncio.set_event_loop(asyncio.new_event_loop())

---

# Step 5 — Contract Resolution

Contracts must be retrieved via:

bot.get_contract(symbol)

Example:

Future(
  symbol="MES",
  exchange="GLOBEX",
  currency="USD",
  lastTradeDateOrContractMonth="20260320"
)

Contracts are stored in:

bot.contract_cache

---

# Step 6 — Bracket Order Creation

Three orders are created:

Entry
Stop
Target

Example structure:

Entry order
parentId = 0
transmit = False

Stop order
parentId = entry order
transmit = False

Target order
parentId = entry order
transmit = True

The final order transmits the full bracket.

---

# Step 7 — Order Submission

Orders are sent to IBKR using ib_insync.

Example:

ib.placeOrder(contract, order)

Possible statuses:

PendingSubmit
Submitted
Filled
Cancelled
Rejected

---

# Step 8 — Fill Events

IBKR emits execution events:

orderStatusEvent
execDetailsEvent

These events update:

• position state
• trade statistics
• bot state machine

---

# Step 9 — Position Monitoring

When entry fills:

bot.trade_state → IN_TRADE

The bot monitors:

• stop execution
• target execution

---

# Step 10 — Trade Exit

Two possible outcomes:

Target filled → profitable trade  
Stop filled → losing trade

After exit:

bot.trade_state → IDLE

---

# Error Conditions

Possible failure points:

Signal rejected
Duplicate signal
Contract resolution failure
IBKR rejection
Worker crash

Each failure should return the system safely to:

IDLE

---

# Logging Requirements

Each stage should log:

Signal received
Signal queued
Worker started
Order submitted
Fill event
Trade closed

Example log:

EXECUTION -> MES SHORT Entry 6724 Stop 6726 Target 6720
Bracket order sent
Trade state -> IN_TRADE

---

# Deterministic Execution Guarantee

The lifecycle guarantees:

• one signal → one trade
• no duplicate entries
• predictable order handling