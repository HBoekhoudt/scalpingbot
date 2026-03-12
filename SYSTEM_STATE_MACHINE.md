# Scalping Bot State Machine

This document defines the runtime state machine for the OOP scalping bot implemented in `scalpingbot.py`.

The state machine ensures deterministic execution and prevents inconsistent trading behavior.

---

# Core Principle

At any moment the bot must be in **exactly one state**.

State transitions occur only through defined events.

---

# Runtime States

## IDLE

Bot is connected and waiting for signals.

Conditions:

• IBKR connection active  
• execution queue empty  
• no open trades  

Allowed transitions:

IDLE → SIGNAL_RECEIVED

---

## SIGNAL_RECEIVED

A TradingView alert has been received and validated.

Actions:

• payload validation  
• duplicate signal check  
• enqueue signal  

Allowed transitions:

SIGNAL_RECEIVED → QUEUED

---

## QUEUED

Signal is stored in execution queue.

Actions:

• worker thread retrieves signal  

Allowed transitions:

QUEUED → ENTRY_SENT

---

## ENTRY_SENT

Entry order has been submitted to IBKR.

Actions:

• bracket order created  
• entry order transmitted  

Allowed transitions:

ENTRY_SENT → IN_TRADE  
ENTRY_SENT → ORDER_REJECTED

---

## IN_TRADE

Position is open.

Actions:

• monitor fills  
• track stop and target  

Allowed transitions:

IN_TRADE → TARGET_FILLED  
IN_TRADE → STOP_FILLED

---

## TARGET_FILLED

Trade closed with profit.

Actions:

• update trade statistics  
• reset trade state  

Allowed transitions:

TARGET_FILLED → IDLE

---

## STOP_FILLED

Trade closed with loss.

Actions:

• update trade statistics  
• reset trade state  

Allowed transitions:

STOP_FILLED → IDLE

---

## ORDER_REJECTED

IBKR rejected the order.

Actions:

• log error  
• discard signal  

Allowed transitions:

ORDER_REJECTED → IDLE

---

# State Diagram

IDLE
 ↓
SIGNAL_RECEIVED
 ↓
QUEUED
 ↓
ENTRY_SENT
 ↓
IN_TRADE
 ↙       ↘
STOP     TARGET
 ↓         ↓
IDLE     IDLE

---

# Worker Thread Responsibility

The execution worker controls the state transitions from:

QUEUED → ENTRY_SENT → IN_TRADE

The webhook must never alter trading state directly.

---

# FastAPI Responsibility

The webhook endpoint may only trigger:

IDLE → SIGNAL_RECEIVED

It must never:

• place orders
• modify positions
• interact with IBKR

---

# Error Handling

Unexpected conditions should force a safe transition to:

ORDER_REJECTED

Followed by:

ORDER_REJECTED → IDLE

---

# Deterministic Execution Guarantee

The state machine ensures:

• no duplicate entries  
• no simultaneous trades  
• predictable order lifecycle