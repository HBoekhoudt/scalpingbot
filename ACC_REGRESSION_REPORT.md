# ACC Regression Report

## Scope and Safety Version

This report was produced using the safer v2 regression procedure:

- Validation/reporting only; production code changes are out of scope.
- Updating this report is allowed.
- If a blocking production defect is found, document it and stop for approval before patching.
- Broker-connected and webhook-triggered tests may only run when ACC/paper safety is explicit and the runtime environment can import/start cleanly.
- If IBKR paper connectivity or runtime dependencies are unavailable, broker-dependent tests are marked `NOT EXECUTED`, not `PASS`.

## Run Metadata

- Date/time: 2026-05-23T17:37:56.3200613+02:00
- Bot: IKBR_SCALPING_BOT
- Version/patch/stage: v1.6.0 / P205 / ACC
- Commit/hash: a7aa2cb
- Symbols in scope: MES, MNQ, M6E, FDXM
- Production code changed: No
- Report changed: Yes, this file was created
- PRD/live order submitted: No
- Broker-connected tests executed: No

## Environment Notes

- `python -m py_compile scalpingbot.py` completed successfully with the system Python.
- `.venv\Scripts\python.exe -m py_compile scalpingbot.py` completed successfully.
- Import/runtime checks could not be completed:
  - System Python: `ModuleNotFoundError: No module named 'fastapi'`.
  - `.venv`: `ZoneInfoNotFoundError: 'No time zone found with key Europe/Amsterdam'`; `pip show tzdata` reported package not found.
  - `.venv-1`: has `tzdata==2025.3`, but lacks `fastapi`.
- `requirements.txt` includes `tzdata==2025.3`, so the `.venv` appears out of sync with project requirements.

## Static Validation

| Check | Status | Evidence summary |
|---|---:|---|
| `python -m py_compile scalpingbot.py` | PASS | Command exited 0. |
| BOT_STAGE remains ACC | PASS | `scalpingbot.py:29` has `BOT_STAGE = "ACC"`. |
| RUNTIME_DISABLED_SYMBOLS intended state | PASS | `scalpingbot.py:180` has `RUNTIME_DISABLED_SYMBOLS = set()`, enabling all configured futures. |
| EURUSD execution remains disabled | PASS | `scalpingbot.py:183` defines `EURUSD_EXECUTION_DISABLED_REASON`; gates at `12517` and worker/place-order checks at `17348`, `17490`. |
| QUALIFIED_FUTURE_SYMBOLS includes all futures | PASS | `scalpingbot.py:177` has `("MNQ", "MES", "M6E", "FDXM")`. |
| FDXM spec uses EUREX/EUR/tradingClass FDXM | PASS | `scalpingbot.py:106-122` defines exchange `EUREX`, currency `EUR`, trading_class `FDXM`. |
| No manual futures expiry construction for live qualification | PASS | `Future(**contract_kwargs)` only at `scalpingbot.py:16429`; no `lastTradeDateOrContractMonth = ...` assignment found. Expiry is selected from IBKR contract details. |
| Contract qualification via IBKR details | PASS static | `qualify_contracts()` builds base contract, calls `broker_write_req_contract_details()`, selects front month from returned details, then caches contract at `14891-14921`. |
| broker_io_owner_mode enforcement | PASS static | `scalpingbot.py:510` sets `enforce_single_owner`; `broker_read()` and `broker_write()` both call `assert_broker_io_owner()`. |
| Module import does not create ScalpingBot threads | NOT EXECUTED | Import blocked by missing dependencies/timezone data. Static evidence shows `startup_event()` calls `initialize_bot_once()`, while module top-level only defines `app = FastAPI()` and hooks. |
| Raw webhook secret is never logged | PASS static | Webhook logs use `build_sanitized_payload_summary()` at `16681`, `18951`, `18960`; sensitive keys are redacted via `redact_sensitive_payload()`. Auth failure logs symbol/signal_id only. |

## Symbol Normalization Regression

| TradingView symbol | Expected | Status | Evidence summary |
|---|---:|---:|---|
| MES1! | MES | PASS static | `_normalize_symbol()` uppercases and removes `1!`. |
| MNQ1! | MNQ | PASS static | Same resolver path. |
| M6E1! | M6E | PASS static | Same resolver path. |
| FDAX1! | FDXM | PASS static | `_normalize_symbol()` maps `FDAX` to `FDXM`. |
| FDXM1! | FDXM | PASS static | `1!` removal leaves `FDXM`. |

Executable normalization test was `NOT EXECUTED` because importing `scalpingbot.py` failed in available Python environments.

## Contract Qualification Regression

| Symbol | Status | Evidence / reason |
|---|---:|---|
| MNQ | NOT EXECUTED | Requires IBKR paper connectivity and importable runtime. Environment import failed before safe broker test could start. |
| MES | NOT EXECUTED | Same reason. |
| M6E | NOT EXECUTED | Same reason. |
| FDXM | NOT EXECUTED | Same reason; static spec confirms EUREX/EUR/FDXM. |

No IB Error 200 was observed because no broker qualification call was executed.

## Webhook Regression

| Check | Status | Evidence / reason |
|---|---:|---|
| Valid ACC payloads for each symbol | NOT EXECUTED | Runtime import/start blocked by environment dependency mismatch. |
| Webhook returns quickly | PASS static | Handler validates auth, logs sanitized payload, calls `runtime_bot.handle_webhook_signal(data)`, and returns status. No direct broker calls observed in handler. |
| Secret accepted but not logged | PASS static | `received_secret` is compared with `hmac.compare_digest`; logs use sanitized summaries. |
| Payload normalized | PASS static | `handle_webhook_signal()` calls `normalize_signal(data)` before routing/queue decisions. |
| Signal queued | NOT EXECUTED | Requires executable runtime and valid payload path. Static queue put exists at `17119`. |
| No IBKR call in webhook thread | PASS static | No `broker_read`, `broker_write`, or `ib.` calls found in `webhook_handler`; architecture docs require queue-worker path. |

## Queue / Worker Regression

| Check | Status | Evidence / reason |
|---|---:|---|
| Job enters `execution_queue` | PASS static | `handle_webhook_signal()` puts eligible jobs into `self.execution_queue` at `17119`. |
| Worker picks job up | PASS static | `execution_worker()` calls `self.execution_queue.get(timeout=1.0)` and logs acquisition. |
| Broker owner match true for broker reads/writes | PASS static | Worker registers owner at `17280`; broker wrappers enforce owner before reads/writes. |
| Startup reconciliation completed before execution | PASS static | Worker calls `connect_ib()` then `run_startup_reconciliation()` before entering queue loop. External entries are blocked while reconciliation is incomplete. |

## Risk / Sizing / Containment Regression

| Check | Status | Evidence summary |
|---|---:|---|
| Positive valid sizing | PASS static | `calculate_execution_position_size()` rejects invalid grade, stop distance, raw size, and invalid normalized size. |
| Min size / step constraints | PASS static | Futures have `min_size=1`, `size_step=1`; `validate_position_size()` enforces min and step alignment. |
| ACC containment profile | PASS | `CONTAINMENT_STAGE_PROFILES["ACC"]["profile"] == "ACC_CONSERVATIVE"`. |
| Notional cap and size cap applied | PASS static | `resolve_execution_containment()` applies max size and max notional; final coherence checks fail closed before order creation. |
| No zero/negative order reaches broker | PASS static | Sizing, containment, and final approved-size checks return before bracket order creation/submission. |

## Bracket Regression

| Check | Status | Evidence summary |
|---|---:|---|
| Parent, TP, SL built | PASS static | `place_bracket_order()` builds `LimitOrder` parent, `LimitOrder` TP, `StopOrder` SL. |
| Parent transmit=False | PASS static | `parent.transmit = False`. |
| TP transmit=False | PASS static | `tp.transmit = False`. |
| SL transmit=True | PASS static | `sl.transmit = True`. |
| parentId relationships correct | PASS static | `tp.parentId = parent_id`; `sl.parentId = parent_id`. |
| orderRef includes trade ID / symbol / role | PASS static | All legs call `build_order_ref(trade_id, symbol, role)`. |
| Broker confirmation works or fail-closed activates | PASS static / NOT EXECUTED broker | `assess_broker_bracket_confirmation()` and failure handling paths exist; no live paper bracket was submitted. |

## Failure-Path Regression

| Check | Status | Evidence / reason |
|---|---:|---|
| Duplicate signal ignored | PASS static | Duplicate key/time gate returns `duplicate_ignored`. |
| Startup reconciliation blocks ambiguous broker state | PASS static | Ambiguous startup state keeps entries blocked and sets reconciliation status `blocked`. |
| Partial bracket submission failure fail-closed | PASS static | `handle_bracket_submission_failure()` marks uncertain state, blocks external entries, checks broker reality, and cleanup path. |
| Broker I/O from non-owner thread rejected | PASS static | `assert_broker_io_owner()` raises `RuntimeError` when owner check fails under enforced mode. |
| Missing protective SL triggers emergency protection/flatten or blocks entries | PASS static | Startup reconciliation blocks position without protective SL; emergency flatten routines are present. |
| Session-close cutoff blocks new entries near close | PASS static | Webhook, worker, and place-order paths check `should_block_new_entry_for_session_close()`. |
| Mock/failure injection execution | NOT EXECUTED | Runtime import blocked; no tests/mocks were run. |

## Session-Close Regression

| Check | Status | Evidence summary |
|---|---:|---|
| MES/MNQ/M6E US close behavior | PASS static | `SESSION_FLAT_BY_TIMES` uses `America/Chicago` `16:00` for MES, MNQ, M6E. |
| FDXM Europe/Berlin flat_by 22:00 | PASS static | `SESSION_FLAT_BY_TIMES["FDXM"]` uses `Europe/Berlin`, `22:00`. |
| Monitor only queues system jobs | PASS static | Session monitor queues `SESSION_CLOSE_SWEEP` jobs; system jobs are recognized separately. |
| Worker performs broker actions | PASS static | Worker handles `BROKER_SYSTEM_JOB_TYPES`; broker actions are behind owner-enforced broker wrappers. |

## Logs / Grep Evidence Summary

- No current ACC runtime logs were generated during this regression.
- Existing `Logs/` files are dated 2026-04-10 to 2026-04-11 and were not used as current-pass evidence.
- Grep/static evidence was collected from:
  - `scalpingbot.py`
  - `ARCHITECTURE.md`
  - `ENGINEERING_RULES.md`
  - `ORDER_LIFECYCLE.md`
  - `DEVELOPMENT_ROADMAP.md`

## Blocking Defects

No production-code defect was patched or confirmed through runtime execution.

Environment blocker:

- Available Python environments are inconsistent with `requirements.txt`.
- `.venv` has `fastapi` but does not have `tzdata`, causing `ZoneInfo("Europe/Amsterdam")` import failure on Windows.
- `.venv-1` has `tzdata` but does not have `fastapi`.
- This blocks import-based, webhook, mocked runtime, and broker-dependent regression steps.

## Recommendation

ACC readiness recommendation: **ACC NOT PASS**.

Reason: static/offline safety checks are mostly PASS, but the full ACC regression was not completed. Runtime import checks, executable symbol normalization, webhook tests, contract qualification, paper bracket submission, and IBKR paper broker confirmation were `NOT EXECUTED` due to environment dependency mismatch and lack of verified paper connectivity in this run.

Next safe step: synchronize the active virtualenv with `requirements.txt`, confirm IBKR paper connectivity/account context, then rerun the v2 regression from import/runtime checks onward.
