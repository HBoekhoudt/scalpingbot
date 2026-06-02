# PRD Readiness Checklist

Generated for bot patch: P208

## Safety Statement

- No live PRD order was placed during this validation.
- `BOT_STAGE` was not changed by this patch.
- PRD live order transmission is blocked unless every PRD live-release gate passes.
- Unknown gate state is treated as blocked.

## Required Live Approval

Live PRD order transmission requires all of the following:

- `BOT_STAGE == "PRD"`
- `IKBR_PRD_LIVE_APPROVED=true`
- `EXECUTION_TRANSMISSION_MODE=live`
- `IKBR_ALLOW_LIVE_ORDERS=true`
- `IKBR_PRD_DRY_RUN` is unset or false

## Gate List

| Gate | Required State | Current Static Status |
|---|---|---|
| Stage is PRD | `BOT_STAGE == "PRD"` | FAIL, current code remains ACC |
| PRD release approval | `IKBR_PRD_LIVE_APPROVED=true` | UNKNOWN until runtime env is checked |
| Dry-run disabled | PRD dry-run false | UNKNOWN until runtime env is checked |
| Webhook secret production-ready | env secret present and non-default | UNKNOWN until runtime env is checked |
| Singleton lock active | runtime lock acquired | UNKNOWN until runtime startup |
| Broker I/O owner enforcement | `enforce_single_owner` | PASS static |
| Execution worker owner registered | worker registered owner thread | UNKNOWN until runtime startup |
| Startup reconciliation complete | completed and clear | UNKNOWN until broker-connected startup |
| No startup ambiguity | no ambiguous symbols | UNKNOWN until broker-connected startup |
| No connectivity reconciliation pending | false | UNKNOWN until runtime |
| IBKR session healthy | connected, initialized, healthy | UNKNOWN until runtime |
| Qualified contracts ready | MNQ, MES, M6E, FDXM cached unless disabled | UNKNOWN until IBKR contract qualification |
| Symbol supported/enabled | no unsupported/EURUSD submission | PASS static for configured futures |
| EURUSD disabled | spot FX execution blocked | PASS static |
| PRD containment valid | max size/notional for enabled futures | PASS static |
| Session-close configured | enabled with flat_by per enabled symbol | PASS static |
| PRD daily SL clear | no active daily SL stop | UNKNOWN until runtime |
| No active execution trade | no active trade lock | UNKNOWN until runtime |
| No ambiguous broker reality | reconciliation clear, no external block | UNKNOWN until runtime |
| Raw secret logging disabled | sanitized payload logging | PASS static |
| Explicit live transmission approval | mode live plus approval envs | UNKNOWN until runtime env is checked |

## Required Operator Actions

1. Complete ACC regression with broker-dependent tests.
2. Complete PRD dry-run with `IKBR_PRD_DRY_RUN=true`; confirm no orders are transmitted.
3. Confirm `/health` shows no failed or unknown PRD live gates after switching to the intended PRD runtime context.
4. Only after dry-run evidence is reviewed, set all explicit live approvals for the controlled live release window.
5. Monitor TWS/IB Gateway open orders and positions during first live release.

## Rollback Procedure

- Preferred: set `BOT_STAGE` back to `ACC` in code and restart the bot.
- Immediate live-transmission rollback: unset or set `IKBR_PRD_LIVE_APPROVED=false`.
- Also disable live transmission by setting `EXECUTION_TRANSMISSION_MODE=dry_run` or unsetting `IKBR_ALLOW_LIVE_ORDERS`.
- Restart the bot and confirm `/health` reports PRD live gates blocked.

## Emergency Procedure

- If bot state, broker state, or reconciliation status is ambiguous, do not rely on automated PRD entry logic.
- Manually inspect and flatten positions in TWS/IB Gateway if needed.
- Cancel unexpected open orders manually in TWS/IB Gateway if bot ownership is unclear.
- Keep PRD live approvals disabled until broker reality is reconciled and documented.
