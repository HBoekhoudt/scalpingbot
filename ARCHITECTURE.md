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