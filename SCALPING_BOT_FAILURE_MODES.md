\# Failure Modes



The scalping bot must handle the following failure conditions.



---



\# IBKR Disconnect



Symptoms:



IB disconnected

orders rejected



Recovery:



bot.connect\_ib()



Reconnect automatically.



---



\# Contract Qualification Failure



Symptoms:



Error 200

No security definition



Cause:



incorrect contract expiry



Recovery:



validate front month logic.



---



\# Duplicate TradingView Signals



Symptoms:



duplicate orders



Recovery:



signal\_guard logic



Ignore duplicate signals.



---



\# Queue Overload



Symptoms:



queue size grows indefinitely



Recovery:



drop stale signals.



---



\# Order PendingSubmit



Symptoms:



orders never execute



Cause:



transmit flag incorrect



Ensure final bracket order uses:



transmit = True



---



\# Worker Crash



Symptoms:



orders stop executing



Recovery:



restart worker thread.

