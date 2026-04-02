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

==========================================================
BRACKET IMPLEMENTATION RULES (FROZEN)
==========================================================
🔒 1. NO BRACKET MODIFICATION

De volgende structuur is frozen:

3 orders (parent + TP + SL)
parentId linking
transmit chain False/False/True

👉 Engineer mag dit NIET wijzigen zonder architect approval

🔒 2. NO ORDER SEPARATION

Verboden:

TP of SL los plaatsen
orders later toevoegen

👉 Bracket moet atomisch worden geplaatst

🔒 3. ORDER PLACEMENT SEQUENCE

Verplicht:

parent
TP
SL (transmit=True)
🔒 4. NO POST-FILL LOGIC

Bot mag:

❌ GEEN TP/SL aanpassen na fill
❌ GEEN OCO zelf beheren

👉 IBKR doet dit

🔒 5. EXECUTION LOCK (VERPLICHT VOLGENDE FASE)

Er mag slechts:

👉 1 actieve trade tegelijk bestaan

Nieuwe signals moeten worden geblokkeerd als:

positie open is
bracket actief is
🔒 6. CLEAN STATE REQUIREMENT

Voor testen:

geen oude open orders
geen orphan brackets
🔒 7. EVENT-DRIVEN VALIDATION

Alle lifecycle beslissingen moeten gebaseerd zijn op:

IBKR events (niet aannames)
🧩 3. WAAR PLAATS JE DIT (BELANGRIJK)
In jouw repo:
📁 ARCHITECTURE.md

👉 Voeg toe onder:

"Order Model" of "Execution Model"
(of onderaan als nieuwe sectie)