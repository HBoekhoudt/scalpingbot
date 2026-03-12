\# IBKR OOP Scalping Bot



This project implements a low latency scalping engine using Interactive Brokers.



Signals originate from TradingView and are executed automatically.



---



\# Core Engine



scalpingbot.py



The entire runtime trading engine is implemented as an object-oriented system.



---



\# Run the Bot



Start the server:



uvicorn scalpingbot:app --host 0.0.0.0 --port 8000



---



\# Trading Flow



TradingView → Webhook → Queue → Worker → IBKR



---



\# Requirements



Python 3.12  

FastAPI  

uvicorn  

ib\_insync  



---



\# Supported Instruments



MES  

MNQ  

M6E  

FDXM  



---



\# Project Structure



scalpingbot.py  

ARCHITECTURE.md  

ENGINEERING\_RULES.md  

SCALPING\_BOT\_FAILURE\_MODES.md  

DEVELOPMENT\_ROADMAP.md

