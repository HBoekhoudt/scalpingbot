# ENGINEERING RULES – IBKR SCALPING BOT

This repository contains a professional IBKR scalping bot.  
The purpose of this document is to enforce strict engineering discipline and prevent regressions during development.

These rules are mandatory for all future engineering tasks.


==================================================
PROJECT CONTEXT
==================================================

This repository implements an automated trading system connected to Interactive Brokers.

The system follows a strict execution pipeline which must never be bypassed.

TradingView
→ webhook
→ resolve_contract
→ execution_queue
→ execution_engine
→ IBKR

Every order must follow this exact path.


==================================================
CRITICAL INVARIANTS
==================================================

These invariants must never be broken.

Symbol normalization must always work:

FDAX1! → FDAX → FDXM  
MES1! → MES  
MNQ1! → MNQ  


Order safety rules:

Orders may ONLY be sent from execution_engine.

The webhook must NEVER send orders directly.

All trades must pass through execution_queue.


Contract safety:

IBKR contracts must ALWAYS be qualified before order placement.

ib.qualifyContracts()


Order structure:

Every entry must always create a bracket order:

Entry  
StopLoss  
TakeProfit  


==================================================
ENGINEERING PROTOCOL
==================================================

All development must follow strict engineering procedures.


--------------------------------------------------
PATCH PROTOCOL (existing files)
--------------------------------------------------

When modifying an existing file, the following steps are mandatory:

1. Ask for the CURRENT file first.
2. Analyze the file.
3. Describe the minimal change required.
4. Return the FULL updated file.

Never rewrite files without seeing the current version first.


--------------------------------------------------
CREATE PROTOCOL (new modules)
--------------------------------------------------

New modules may be created directly.

Constraints:

- Do NOT modify existing files.
- Return the FULL module code.
- Follow the existing architecture.


==================================================
DEVELOPMENT RULES
==================================================

Each engineering task may perform ONLY ONE of the following:

modify ONE module  
or  
create ONE module  

Never both.

Never redesign the architecture unless explicitly requested.

Never remove existing functionality unless explicitly requested.

Previously fixed bugs must never be reintroduced.


==================================================
ARCHITECTURE SAFETY
==================================================

The execution pipeline must always remain:

TradingView  
→ webhook  
→ resolve_contract  
→ execution_queue  
→ execution_engine  
→ IBKR

Orders must always originate from execution_engine.

No shortcuts are allowed.


==================================================
RETURN FORMAT
==================================================

PATCH:

analysis  
minimal change  
FULL updated file  


CREATE:

FULL file content