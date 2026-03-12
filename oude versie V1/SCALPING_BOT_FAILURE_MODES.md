# Scalping Bot Failure Modes – IBKR Trading Systems

This document describes the most common failure modes found in automated
IBKR trading bots and explains how the architecture of this repository
prevents them.

Understanding these failure modes is critical when modifying the system.
Many architecture rules exist specifically to prevent these problems.

This repository contains a **professional IBKR scalping bot** designed
for safe, deterministic execution.


==================================================
PROJECT EXECUTION PIPELINE
==================================================

All trade execution must follow the strict system pipeline:

TradingView  
→ webhook  
→ resolve_contract  
→ execution_queue  
→ execution_engine  
→ IBKR  

This pipeline ensures that trade signals are validated, normalized,
queued, and executed in a controlled environment.

No module may bypass this pipeline.


==================================================
FAILURE MODE 1 – DUPLICATE ORDERS
==================================================

Cause

TradingView may send multiple alerts for the same signal.

Examples:

- Alert retriggers on the same candle
- Alert fires multiple times during price movement
- Network retries resend the webhook


Risk

If not handled correctly, the bot may open multiple trades for a single
signal.

This can result in unintended position sizing and increased risk.


Prevention

The architecture prevents duplicate trades through:

signal_guard module  
trade_state checks  
execution_queue serialization  

The execution queue ensures that signals are processed sequentially,
while the signal guard module filters duplicate alerts.


==================================================
FAILURE MODE 2 – RACE CONDITIONS
==================================================

Cause

Multiple threads attempt to place orders simultaneously.

Typical causes:

- Webhook threads placing orders directly
- Multiple worker threads calling IBKR API
- Parallel signal processing


Risk

Race conditions may lead to:

- duplicate orders
- inconsistent trade state
- API errors
- unpredictable execution behavior


Prevention

The architecture enforces a **single execution path**:

execution_queue → execution_engine

The queue guarantees that orders are processed sequentially by one
execution worker.

This removes the possibility of concurrent order placement.


==================================================
FAILURE MODE 3 – GHOST POSITIONS
==================================================

Cause

The internal bot state becomes different from the IBKR account state.

Example scenario:

- Bot believes a trade is closed
- IBKR still holds an open position


Risk

This leads to dangerous behavior such as:

- opening a second trade on the same instrument
- reversing a position unintentionally
- loss of risk control


Prevention

The architecture uses synchronization modules:

position_sync module  
order_sync module  

These modules periodically compare the bot state with IBKR and restore
consistency if mismatches occur.


==================================================
FAILURE MODE 4 – RECONNECT ORDER DUPLICATION
==================================================

Cause

IBKR disconnects and reconnects while orders are pending.

When the connection returns, the bot may incorrectly assume that orders
were never submitted and resend them.


Risk

This can create:

- duplicate entry orders
- unexpected additional positions


Prevention

The architecture includes:

order_sync module  
order_manager lifecycle tracking  

These modules track order states and verify existing orders before
submitting new ones.


==================================================
FAILURE MODE 5 – OCO / BRACKET FAILURES
==================================================

Cause

Entry orders fill but protective orders are missing.

Example:

Entry order fills but the StopLoss or TakeProfit was never placed.


Risk

This leaves the trade completely unprotected, exposing the account to
unlimited loss.


Prevention

The execution engine always creates **IBKR bracket orders**.

A bracket order consists of:

Entry  
TakeProfit  
StopLoss  

IBKR manages the OCO relationship between the orders, ensuring that the
remaining order cancels automatically when the other is filled.


==================================================
FAILURE MODE 6 – SYMBOL MISMATCH
==================================================

Cause

TradingView symbols often differ from IBKR symbols.

Example:

TradingView symbol: FDAX1!  
Normalized symbol: FDAX  
IBKR contract: FDXM


Risk

If symbols are not normalized correctly:

- contract resolution fails
- orders are rejected
- incorrect instruments may be traded


Prevention

The architecture separates symbol handling into two modules:

symbol_normalizer module  
contract_resolver module  

symbol_normalizer converts TradingView symbols into normalized internal
symbols.

contract_resolver converts those normalized symbols into valid IBKR
contracts.


==================================================
FAILURE MODE 7 – ORDER ID RACE CONDITIONS
==================================================

Cause

Multiple threads request order IDs from the IBKR API simultaneously.


Risk

This can cause:

- order ID collisions
- IBKR API errors
- rejected orders


Prevention

Only one component is allowed to send orders to IBKR:

execution_engine

Because the execution engine processes jobs from a single queue,
order IDs are requested sequentially and cannot collide.


==================================================
ENGINEERING PRINCIPLE
==================================================

Every module in this repository exists to prevent one or more of the
failure modes described in this document.

Future engineering work must **never introduce changes that re-enable
these failure modes**.

Any architecture change must be evaluated against these risks before it
is implemented.