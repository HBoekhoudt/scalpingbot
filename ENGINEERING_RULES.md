# Engineering Rules

The scalping bot follows strict engineering constraints.

---

# Single Source of Truth

All runtime state must exist inside:

ScalpingBot

Do not introduce global state.

---

# Contract Creation

Contracts must only be created using:

bot.get_contract(symbol)

Do not construct Future() objects elsewhere.

---

# IBKR Interaction

IBKR API calls must occur only inside the execution worker.

Never call IBKR inside the FastAPI webhook.

---

# Webhook Rules

Webhook endpoints must:

• validate payload
• enqueue signals
• return immediately

Webhook handlers must never execute trading logic.

---

# Worker Thread

All order execution must occur inside the worker thread.

Worker threads must initialize their own event loop.

---

# Deterministic Execution

Signal → Queue → Worker → Broker

This pipeline must never change.

---

# Module Restrictions

Do not split the trading engine across multiple modules.

All trading runtime logic must remain inside scalpingbot.py.

This prevents state fragmentation.