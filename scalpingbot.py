# ==========================================================
# IKBR SCALPING BOT — P034
# ==========================================================

import logging
import asyncio
import threading
import queue
import time
import decimal
import json
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from ib_insync import IB, Future, Forex, LimitOrder, StopOrder

# ==========================================================
# VERSIONING
# ==========================================================

BOT_NAME = "IKBR_SCALPING_BOT"
BOT_VERSION = "v1.6.0"
BOT_PATCH = "P260326034"
BOT_STAGE = "TEST"  # TEST | PAPER | LIVE

logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)

# ==========================================================
# CONFIG
# ==========================================================

SPREADS = {
    "MNQ": 0.25,
    "MES": 0.25,
    "FDXM": 0.5,
    "M6E": 0.00005,
}

TICK_SIZES = {
    "MNQ": 0.25,
    "MES": 0.25,
    "FDXM": 0.5,
    "M6E": 0.00005,
}

# ==========================================================
# BOT
# ==========================================================

class ScalpingBot:

    def __init__(self):

        self.ib = IB()

        self.IB_HOST = "127.0.0.1"
        self.IB_PORT = 7497
        self.IB_CLIENT_ID = 1

        self.contract_cache = {}
        self.contract_min_ticks = {}
        self.execution_queue = queue.Queue()

        self.trade_state = "IDLE"

        self.connection_lock = threading.Lock()
        self.order_id_lock = threading.Lock()
        self.next_order_id = None

        self.last_signal = None
        self.last_signal_time = 0

        self.last_diagnostic = None
        self.last_diagnostic_time = 0

        self.trade_analysis_lock = threading.Lock()
        self.trade_analysis = {}
        self.order_to_trade = {}
        self.completed_trade_ids = set()
        self.trade_seq = 0
        self.aggregate_stats = {
            "total_trades": 0,
            "closed_trades": 0,
            "filled_trades": 0,
            "tp_count": 0,
            "sl_count": 0,
            "cancelled_count": 0,
            "rejected_count": 0,
            "incomplete_count": 0,
            "gross_pnl": 0.0,
            "commission": 0.0,
            "net_pnl": 0.0,
        }

        threading.Thread(target=self.execution_worker, daemon=True).start()
        threading.Thread(target=self.ib_watchdog, daemon=True).start()

    # ==========================================================
    # PRICE LOGIC
    # ==========================================================

    def get_tick_size(self, symbol):
        return self.contract_min_ticks.get(symbol, TICK_SIZES[symbol])

    def round_to_tick(self, symbol, price):
        tick = decimal.Decimal(str(self.get_tick_size(symbol)))
        price = decimal.Decimal(str(price))
        return float((price / tick).quantize(0) * tick)

    def apply_spread(self, symbol, side, price):
        spread = SPREADS.get(symbol, 0)

        if side == "long":
            price += spread
        else:
            price -= spread

        logger.info(f"SPREAD | {symbol} {side} → {price}")
        return price

    # ==========================================================
    # TIME / ANALYSIS HELPERS
    # ==========================================================

    def utc_now_iso(self):
        return datetime.now(timezone.utc).isoformat()

    def to_iso(self, value):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.isoformat()
        return value

    def seconds_between(self, start, end):
        try:
            if isinstance(start, datetime) and isinstance(end, datetime):
                return round((end - start).total_seconds(), 3)
        except Exception:
            return None
        return None

    def extract_grade(self, data):
        return data.get("grade", "")

    def is_diagnostic_payload(self, data):
        if not isinstance(data, dict):
            return False

        if data.get("mode") == "diagnostic":
            return True

        event = data.get("event")
        return event in {
            "ctx_long_on",
            "ctx_short_on",
            "setup_long_on",
            "setup_short_on",
            "blocked_snapshot",
        }

    def log_diagnostic_signal(self, data):
        event = data.get("event", "")
        symbol = str(data.get("symbol", "")).upper().replace("1!", "")
        if symbol == "FDAX":
            symbol = "FDXM"

        timeframe = data.get("timeframe", "")
        price = data.get("price")
        signal_time = data.get("time")
        blocker = data.get("blocker", "")
        now = time.time()

        key = f"{symbol}-{timeframe}-{event}-{blocker}-{price}"

        if key == self.last_diagnostic and now - self.last_diagnostic_time < 5:
            logger.info("DIAGNOSTIC DUPLICATE IGNORED")
            return

        self.last_diagnostic = key
        self.last_diagnostic_time = now

        logger.info(
            "DIAGNOSTIC EVENT | "
            f"symbol={symbol} "
            f"timeframe={timeframe} "
            f"event={event} "
            f"time={signal_time} "
            f"price={price} "
            f"blocker={blocker}"
        )
        logger.info(f"DIAGNOSTIC PAYLOAD | {json.dumps(data, sort_keys=True)}")

    def calculate_expected_gross_pnl(self, record):
        mult_map = {
            "MES": 5.0,
            "MNQ": 2.0,
            "FDXM": 5.0,
            "M6E": 12500.0,
        }
        multiplier = mult_map.get(record["symbol"], 1.0)

        if record["exit_reason"] == "TP":
            points = abs(record["target_price"] - record["entry_price"])
        elif record["exit_reason"] == "SL":
            points = abs(record["stop_price"] - record["entry_price"])
        else:
            return 0.0

        return round(points * multiplier, 2)

    def calculate_price_slippage(self, record, actual_price, intended_price, leg):
        if actual_price is None or intended_price is None:
            return None

        if leg == "entry":
            if record["side"] == "long":
                return round(actual_price - intended_price, 10)
            return round(intended_price - actual_price, 10)

        if leg == "target":
            if record["side"] == "long":
                return round(intended_price - actual_price, 10)
            return round(actual_price - intended_price, 10)

        if leg == "stop":
            if record["side"] == "long":
                return round(intended_price - actual_price, 10)
            return round(actual_price - intended_price, 10)

        return None

    def calculate_execution_quality_metrics(self, record):
        entry_slippage = self.calculate_price_slippage(
            record,
            record["entry_fill_price"],
            record["entry_price"],
            "entry"
        )

        target_slippage = None
        stop_slippage = None
        exit_slippage = None

        if record["exit_reason"] == "TP":
            target_slippage = self.calculate_price_slippage(
                record,
                record["exit_fill_price"],
                record["target_price"],
                "target"
            )
            exit_slippage = target_slippage
        elif record["exit_reason"] == "SL":
            stop_slippage = self.calculate_price_slippage(
                record,
                record["exit_fill_price"],
                record["stop_price"],
                "stop"
            )
            exit_slippage = stop_slippage

        expected_gross_pnl = self.calculate_expected_gross_pnl(record)
        realized_vs_expected_gross = None
        realized_vs_expected_net = None

        if expected_gross_pnl is not None:
            if record["exit_reason"] == "SL":
                realized_vs_expected_gross = round(record["gross_pnl"] + expected_gross_pnl, 2)
            elif record["exit_reason"] == "TP":
                realized_vs_expected_gross = round(record["gross_pnl"] - expected_gross_pnl, 2)

        if realized_vs_expected_gross is not None:
            realized_vs_expected_net = round(record["net_pnl"] - (record["gross_pnl"] - record["commission"]), 2)

        return {
            "entry_slippage": entry_slippage,
            "target_slippage": target_slippage,
            "stop_slippage": stop_slippage,
            "exit_slippage": exit_slippage,
            "expected_gross_pnl": expected_gross_pnl,
            "realized_vs_expected_gross": realized_vs_expected_gross,
            "realized_vs_expected_net": realized_vs_expected_net,
        }

    def build_trade_record(
        self,
        job,
        signal_time,
        enqueue_time,
        execution_start_time,
        spread_adjusted_entry,
        entry,
        stop,
        target,
        parent_id,
        tp_id,
        sl_id
    ):
        self.trade_seq += 1
        trade_id = f"T{self.trade_seq:06d}"

        record = {
            "trade_id": trade_id,
            "state": "SUBMITTING",
            "symbol": job["symbol"],
            "side": job["side"],
            "grade": job.get("grade", ""),
            "signal_time": signal_time,
            "enqueue_time": enqueue_time,
            "execution_start_time": execution_start_time,
            "entry_signal_price": job["entry"],
            "entry_spread_adjusted": spread_adjusted_entry,
            "entry_price": entry,
            "stop_price": stop,
            "target_price": target,
            "parent_order_id": parent_id,
            "tp_order_id": tp_id,
            "sl_order_id": sl_id,
            "parent_perm_id": None,
            "tp_perm_id": None,
            "sl_perm_id": None,
            "entry_fill_price": None,
            "entry_fill_time": None,
            "exit_fill_price": None,
            "exit_fill_time": None,
            "exit_reason": None,
            "entry_filled": False,
            "closed": False,
            "gross_pnl": 0.0,
            "commission": 0.0,
            "net_pnl": 0.0,
            "position_size": 1.0,
            "anomalies": [],
            "events": [],
            "summary_logged": False,
        }

        return record

    def append_trade_event(self, trade_id, message):
        record = self.trade_analysis.get(trade_id)
        if record is None:
            return
        record["events"].append(f"{self.utc_now_iso()} | {message}")

    def register_trade_analysis(
        self,
        job,
        signal_time,
        enqueue_time,
        execution_start_time,
        spread_adjusted_entry,
        entry,
        stop,
        target,
        parent_id,
        tp_id,
        sl_id
    ):
        with self.trade_analysis_lock:
            record = self.build_trade_record(
                job,
                signal_time,
                enqueue_time,
                execution_start_time,
                spread_adjusted_entry,
                entry,
                stop,
                target,
                parent_id,
                tp_id,
                sl_id
            )
            trade_id = record["trade_id"]

            self.trade_analysis[trade_id] = record
            self.order_to_trade[parent_id] = trade_id
            self.order_to_trade[tp_id] = trade_id
            self.order_to_trade[sl_id] = trade_id
            self.aggregate_stats["total_trades"] += 1

            self.append_trade_event(
                trade_id,
                f"REGISTERED symbol={record['symbol']} side={record['side']} "
                f"entry={record['entry_price']} stop={record['stop_price']} target={record['target_price']}"
            )

            return trade_id

    def get_trade_by_order_id(self, order_id):
        if order_id is None:
            return None, None

        with self.trade_analysis_lock:
            trade_id = self.order_to_trade.get(order_id)
            if trade_id is None:
                return None, None
            return trade_id, self.trade_analysis.get(trade_id)

    def update_trade_perm_id(self, order_id, perm_id):
        if not perm_id:
            return

        trade_id, record = self.get_trade_by_order_id(order_id)
        if record is None:
            return

        if order_id == record["parent_order_id"]:
            record["parent_perm_id"] = perm_id
        elif order_id == record["tp_order_id"]:
            record["tp_perm_id"] = perm_id
        elif order_id == record["sl_order_id"]:
            record["sl_perm_id"] = perm_id

    def mark_trade_state(self, order_id, status):
        trade_id, record = self.get_trade_by_order_id(order_id)
        if record is None:
            return

        current_state = record["state"]

        if order_id == record["parent_order_id"]:
            if status in ("PendingSubmit", "ApiPending", "PreSubmitted", "Submitted"):
                if not record["entry_filled"]:
                    record["state"] = "ENTRY_WORKING"
            elif status == "Filled":
                record["state"] = "ENTRY_FILLED"
            elif status in ("Cancelled", "ApiCancelled", "Inactive"):
                if not record["entry_filled"]:
                    record["state"] = "CANCELLED"

        elif order_id in (record["tp_order_id"], record["sl_order_id"]):
            if record["entry_filled"] and status in ("PendingSubmit", "ApiPending", "PreSubmitted", "Submitted"):
                record["state"] = "EXIT_WORKING"
            elif order_id == record["tp_order_id"] and status == "Filled":
                record["state"] = "TP_FILLED"
            elif order_id == record["sl_order_id"] and status == "Filled":
                record["state"] = "SL_FILLED"

        if current_state != record["state"]:
            self.append_trade_event(trade_id, f"STATE {current_state} -> {record['state']}")

    def append_anomaly(self, trade_id, message):
        record = self.trade_analysis.get(trade_id)
        if record is None:
            return
        if message not in record["anomalies"]:
            record["anomalies"].append(message)
            self.append_trade_event(trade_id, f"ANOMALY {message}")

    def update_trade_from_status(self, trade):
        try:
            order = trade.order
            status = trade.orderStatus
            order_id = getattr(order, "orderId", None)
            perm_id = getattr(status, "permId", None)
            state = getattr(status, "status", None)

            trade_id, record = self.get_trade_by_order_id(order_id)
            if record is None:
                return

            self.update_trade_perm_id(order_id, perm_id)
            self.mark_trade_state(order_id, state)
            self.append_trade_event(
                trade_id,
                f"STATUS orderId={order_id} orderType={getattr(order, 'orderType', None)} "
                f"status={state} filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)}"
            )

            if (
                state in ("Cancelled", "ApiCancelled", "Inactive")
                and order_id == record["parent_order_id"]
                and not record["entry_filled"]
            ):
                self.aggregate_stats["cancelled_count"] += 1
                self.finalize_trade_if_complete(trade_id)
        except Exception:
            logger.exception("TRADE STATUS ANALYSIS FAILED")

    def calculate_gross_pnl(self, record):
        if record["entry_fill_price"] is None or record["exit_fill_price"] is None:
            return 0.0

        mult_map = {
            "MES": 5.0,
            "MNQ": 2.0,
            "FDXM": 5.0,
            "M6E": 12500.0,
        }
        multiplier = mult_map.get(record["symbol"], 1.0)

        if record["side"] == "long":
            return round((record["exit_fill_price"] - record["entry_fill_price"]) * multiplier, 2)
        return round((record["entry_fill_price"] - record["exit_fill_price"]) * multiplier, 2)

    def finalize_trade_if_complete(self, trade_id):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return
            if record["summary_logged"]:
                return

            ready = False

            if record["entry_filled"] and record["exit_fill_price"] is not None and record["exit_reason"] in ("TP", "SL"):
                ready = True
            elif not record["entry_filled"] and record["state"] in ("CANCELLED", "REJECTED", "INCOMPLETE"):
                ready = True

            if not ready:
                return

            if record["entry_filled"]:
                record["closed"] = True
                record["state"] = "CLOSED"
                record["gross_pnl"] = self.calculate_gross_pnl(record)
                record["net_pnl"] = round(record["gross_pnl"] - record["commission"], 2)
                self.aggregate_stats["closed_trades"] += 1
                self.aggregate_stats["gross_pnl"] = round(self.aggregate_stats["gross_pnl"] + record["gross_pnl"], 2)
                self.aggregate_stats["commission"] = round(self.aggregate_stats["commission"] + record["commission"], 2)
                self.aggregate_stats["net_pnl"] = round(self.aggregate_stats["net_pnl"] + record["net_pnl"], 2)
                if record["exit_reason"] == "TP":
                    self.aggregate_stats["tp_count"] += 1
                elif record["exit_reason"] == "SL":
                    self.aggregate_stats["sl_count"] += 1
            else:
                if record["state"] == "INCOMPLETE":
                    self.aggregate_stats["incomplete_count"] += 1

            record["summary_logged"] = True
            self.completed_trade_ids.add(trade_id)

            duration_to_fill = self.seconds_between(record["execution_start_time"], record["entry_fill_time"])
            duration_in_trade = self.seconds_between(record["entry_fill_time"], record["exit_fill_time"])
            execution_metrics = self.calculate_execution_quality_metrics(record)

            logger.info(
                "TRADE SUMMARY | "
                f"trade_id={record['trade_id']} "
                f"symbol={record['symbol']} "
                f"side={record['side']} "
                f"grade={record['grade']} "
                f"parent_order_id={record['parent_order_id']} "
                f"signal_price={record['entry_signal_price']} "
                f"spread_adjusted_entry={record['entry_spread_adjusted']} "
                f"entry_target={record['entry_price']} "
                f"entry_fill={record['entry_fill_price']} "
                f"stop={record['stop_price']} "
                f"target={record['target_price']} "
                f"exit_fill={record['exit_fill_price']} "
                f"exit_reason={record['exit_reason']} "
                f"gross_pnl={record['gross_pnl']} "
                f"commission={record['commission']} "
                f"net_pnl={record['net_pnl']} "
                f"duration_to_fill_sec={duration_to_fill} "
                f"duration_in_trade_sec={duration_in_trade} "
                f"anomalies={';'.join(record['anomalies']) if record['anomalies'] else 'none'}"
            )

            logger.info(
                "EXECUTION QUALITY | "
                f"trade_id={record['trade_id']} "
                f"symbol={record['symbol']} "
                f"side={record['side']} "
                f"entry_intended={record['entry_price']} "
                f"entry_fill={record['entry_fill_price']} "
                f"entry_slippage={execution_metrics['entry_slippage']} "
                f"stop_intended={record['stop_price']} "
                f"target_intended={record['target_price']} "
                f"exit_fill={record['exit_fill_price']} "
                f"exit_reason={record['exit_reason']} "
                f"stop_slippage={execution_metrics['stop_slippage']} "
                f"target_slippage={execution_metrics['target_slippage']} "
                f"exit_slippage={execution_metrics['exit_slippage']} "
                f"expected_gross_pnl={execution_metrics['expected_gross_pnl']} "
                f"realized_vs_expected_gross={execution_metrics['realized_vs_expected_gross']} "
                f"realized_vs_expected_net={execution_metrics['realized_vs_expected_net']} "
                f"fill_latency_sec={duration_to_fill} "
                f"time_in_trade_sec={duration_in_trade}"
            )

            logger.info(
                "TEST STATS | "
                f"total_trades={self.aggregate_stats['total_trades']} "
                f"closed_trades={self.aggregate_stats['closed_trades']} "
                f"filled_trades={self.aggregate_stats['filled_trades']} "
                f"tp_count={self.aggregate_stats['tp_count']} "
                f"sl_count={self.aggregate_stats['sl_count']} "
                f"cancelled_count={self.aggregate_stats['cancelled_count']} "
                f"rejected_count={self.aggregate_stats['rejected_count']} "
                f"incomplete_count={self.aggregate_stats['incomplete_count']} "
                f"gross_pnl={self.aggregate_stats['gross_pnl']} "
                f"commission={self.aggregate_stats['commission']} "
                f"net_pnl={self.aggregate_stats['net_pnl']}"
            )

    def update_trade_from_fill(self, fill):
        try:
            execution = fill.execution
            order_id = getattr(execution, "orderId", None)
            price = getattr(execution, "price", None)
            fill_time = getattr(fill, "time", None)

            trade_id, record = self.get_trade_by_order_id(order_id)
            if record is None:
                return

            self.append_trade_event(
                trade_id,
                f"FILL orderId={order_id} side={getattr(execution, 'side', None)} "
                f"shares={getattr(execution, 'shares', None)} price={price}"
            )

            if order_id == record["parent_order_id"]:
                record["entry_fill_price"] = price
                record["entry_fill_time"] = fill_time
                record["entry_filled"] = True
                record["state"] = "ENTRY_FILLED"
                self.aggregate_stats["filled_trades"] += 1
            elif order_id == record["tp_order_id"]:
                record["exit_fill_price"] = price
                record["exit_fill_time"] = fill_time
                record["exit_reason"] = "TP"
                record["state"] = "TP_FILLED"
            elif order_id == record["sl_order_id"]:
                record["exit_fill_price"] = price
                record["exit_fill_time"] = fill_time
                record["exit_reason"] = "SL"
                record["state"] = "SL_FILLED"

            self.finalize_trade_if_complete(trade_id)
        except Exception:
            logger.exception("TRADE FILL ANALYSIS FAILED")

    def update_trade_commission(self, trade, fill):
        try:
            order_id = getattr(fill.execution, "orderId", None)
            report = getattr(fill, "commissionReport", None)
            if report is None:
                return

            commission = float(getattr(report, "commission", 0.0) or 0.0)
            trade_id, record = self.get_trade_by_order_id(order_id)
            if record is None:
                return

            record["commission"] = round(record["commission"] + commission, 2)
            self.append_trade_event(trade_id, f"COMMISSION orderId={order_id} commission={commission}")

            self.finalize_trade_if_complete(trade_id)
        except Exception:
            logger.exception("TRADE COMMISSION ANALYSIS FAILED")

    def update_trade_from_error(self, req_id, error_code, error_string):
        try:
            trade_id, record = self.get_trade_by_order_id(req_id)
            if record is None:
                return

            self.append_trade_event(
                trade_id,
                f"ERROR reqId={req_id} errorCode={error_code} errorString={error_string}"
            )

            if error_code == 110:
                if req_id == record["parent_order_id"] and not record["entry_filled"] and record["state"] != "REJECTED":
                    record["state"] = "REJECTED"
                    self.aggregate_stats["rejected_count"] += 1
                    self.finalize_trade_if_complete(trade_id)
                else:
                    self.append_anomaly(trade_id, f"NON_PARENT_TICK_REJECT_{req_id}")
            elif error_code == 135:
                self.append_anomaly(trade_id, f"CHILD_PARENT_LINK_FAILURE_{req_id}")
            elif error_code == 202 and not record["entry_filled"]:
                if req_id == record["parent_order_id"]:
                    record["state"] = "CANCELLED"
                    self.aggregate_stats["cancelled_count"] += 1
                    self.finalize_trade_if_complete(trade_id)
                else:
                    self.append_anomaly(trade_id, f"CHILD_CANCELLED_{req_id}")
        except Exception:
            logger.exception("TRADE ERROR ANALYSIS FAILED")

    # ==========================================================
    # SIGNAL NORMALIZATION / OBSERVABILITY
    # ==========================================================

    def _safe_float(self, value, default=None):
        if value is None or value == "":
            return default
        try:
            return float(value)
        except Exception:
            return default

    def _safe_int(self, value, default=None):
        if value is None or value == "":
            return default
        try:
            return int(value)
        except Exception:
            return default

    def _safe_bool(self, value, default=False):
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "y", "on"}:
                return True
            if normalized in {"false", "0", "no", "n", "off"}:
                return False
        return default

    def _safe_str(self, value, default="unknown"):
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    def _normalize_symbol(self, value):
        symbol = self._safe_str(value, default="")
        symbol = symbol.upper().replace("1!", "")
        if symbol == "FDAX":
            symbol = "FDXM"
        return symbol

    def _normalize_side(self, payload):
        side = payload.get("candidate_side")
        if side is None:
            side = payload.get("side")
        if side is None:
            side = payload.get("direction")

        side = self._safe_str(side, default="").lower()

        if side in {"buy", "bull", "up"}:
            side = "long"
        elif side in {"sell", "bear", "down"}:
            side = "short"

        if side not in {"long", "short"}:
            return None
        return side

    def _normalize_reason_flags(self, value):
        if value is None:
            return []
        if isinstance(value, list):
            return [str(x) for x in value if str(x).strip()]
        if isinstance(value, tuple):
            return [str(x) for x in value if str(x).strip()]
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return []
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed if str(x).strip()]
            except Exception:
                pass
            return [item.strip() for item in raw.split(",") if item.strip()]
        return [str(value)]

    def detect_payload_format(self, payload):
        candidate_keys = {
            "schema_version",
            "signal_id",
            "timestamp_utc",
            "candidate_side",
            "strategy_family",
            "tv_score",
            "tv_candidate_grade",
            "reason_flags",
            "bias_5m",
        }
        if any(key in payload for key in candidate_keys):
            return "candidate"
        return "legacy"

    def normalize_signal(self, payload):
        payload_format = self.detect_payload_format(payload)

        symbol = self._normalize_symbol(payload.get("symbol"))
        side = self._normalize_side(payload)

        entry_price = self._safe_float(
            payload.get("entry_price", payload.get("entry")),
            default=None,
        )
        stop_loss = self._safe_float(
            payload.get("stop_loss", payload.get("stop")),
            default=None,
        )
        take_profit = self._safe_float(
            payload.get("take_profit", payload.get("target")),
            default=None,
        )

        grade = self._safe_str(payload.get("grade"), default="")
        candidate_grade = self._safe_str(payload.get("tv_candidate_grade"), default="")
        if not candidate_grade:
            candidate_grade = grade

        normalized = {
            "raw_payload": payload,
            "payload_format": payload_format,
            "schema_version": payload.get("schema_version"),
            "signal_id": self._safe_str(payload.get("signal_id"), default=""),
            "timestamp_utc": self._safe_str(
                payload.get("timestamp_utc", payload.get("time")),
                default="",
            ),
            "symbol": symbol,
            "timeframe": self._safe_str(payload.get("timeframe"), default=""),
            "instrument_type": self._safe_str(payload.get("instrument_type"), default="unknown"),
            "strategy_family": self._safe_str(payload.get("strategy_family"), default="unknown"),
            "side": side,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "grade": grade,
            "candidate_grade": candidate_grade,
            "tv_score": self._safe_float(payload.get("tv_score"), default=None),
            "reason_flags": self._normalize_reason_flags(payload.get("reason_flags")),
            "session_name": self._safe_str(payload.get("session_name"), default="unknown"),
            "minutes_from_open": self._safe_int(payload.get("minutes_from_open"), default=None),
            "vwap_price": self._safe_float(payload.get("vwap_price"), default=None),
            "vwap_distance_points": self._safe_float(payload.get("vwap_distance_points"), default=None),
            "vwap_distance_atr": self._safe_float(payload.get("vwap_distance_atr"), default=None),
            "vwap_slope_1m": self._safe_float(payload.get("vwap_slope_1m"), default=None),
            "vwap_slope_5m": self._safe_float(payload.get("vwap_slope_5m"), default=None),
            "above_vwap": self._safe_bool(payload.get("above_vwap"), default=False),
            "bias_5m": self._safe_str(payload.get("bias_5m"), default="unknown"),
            "trend_strength_5m": self._safe_float(payload.get("trend_strength_5m"), default=None),
            "range_state_5m": self._safe_str(payload.get("range_state_5m"), default="unknown"),
            "overlap_ratio_5m": self._safe_float(payload.get("overlap_ratio_5m"), default=None),
            "sweep_detected": self._safe_bool(payload.get("sweep_detected"), default=False),
            "sweep_side": self._safe_str(payload.get("sweep_side"), default="unknown"),
            "rejection_detected": self._safe_bool(payload.get("rejection_detected"), default=False),
            "rejection_wick_ratio": self._safe_float(payload.get("rejection_wick_ratio"), default=None),
            "structure_1m_ok": self._safe_bool(payload.get("structure_1m_ok"), default=False),
            "reacceleration_1m_ok": self._safe_bool(payload.get("reacceleration_1m_ok"), default=False),
            "pullback_depth_1m": self._safe_float(payload.get("pullback_depth_1m"), default=None),
            "rr_estimate": self._safe_float(payload.get("rr_estimate"), default=None),
            "spread_estimate_ticks": self._safe_float(payload.get("spread_estimate_ticks"), default=None),
            "execution_quality_hint": self._safe_str(payload.get("execution_quality_hint"), default="unknown"),
        }

        normalized["execution_ready"] = (
            bool(normalized["symbol"]) and
            normalized["side"] in {"long", "short"} and
            normalized["entry_price"] is not None
        )

        return normalized

    def build_normalized_summary(self, normalized):
        summary = {
            "payload_format": normalized["payload_format"],
            "schema_version": normalized["schema_version"],
            "signal_id": normalized["signal_id"],
            "symbol": normalized["symbol"],
            "side": normalized["side"],
            "entry_price": normalized["entry_price"],
            "grade": normalized["grade"],
            "candidate_grade": normalized["candidate_grade"],
            "tv_score": normalized["tv_score"],
            "session_name": normalized["session_name"],
            "bias_5m": normalized["bias_5m"],
            "reason_flags": normalized["reason_flags"],
            "execution_ready": normalized["execution_ready"],
        }
        return summary

    def log_normalized_signal(self, normalized):
        logger.info(f"RAW PAYLOAD RECEIVED | {json.dumps(normalized['raw_payload'], sort_keys=True)}")
        logger.info(f"NORMALIZED SIGNAL | {json.dumps(self.build_normalized_summary(normalized), sort_keys=True)}")
        logger.info(
            "PAYLOAD OBSERVABILITY | "
            f"payload_format={normalized['payload_format']} "
            f"schema_version={normalized['schema_version']} "
            f"signal_id={normalized['signal_id']} "
            f"symbol={normalized['symbol']} "
            f"side={normalized['side']} "
            f"grade={normalized['grade']} "
            f"candidate_grade={normalized['candidate_grade']} "
            f"tv_score={normalized['tv_score']} "
            f"session_name={normalized['session_name']} "
            f"bias_5m={normalized['bias_5m']} "
            f"reason_flags={normalized['reason_flags']}"
        )

    # ==========================================================
    # IB CONNECTION
    # ==========================================================

    def connect_ib(self):

        if self.ib.isConnected():
            return

        with self.connection_lock:

            if self.ib.isConnected():
                return

            logger.info("Connecting to IBKR...")

            self.ib.connect(self.IB_HOST, self.IB_PORT, clientId=self.IB_CLIENT_ID)

            if not self.ib.isConnected():
                raise RuntimeError("IBKR connection failed")

            with self.order_id_lock:
                if self.next_order_id is None:
                    self.next_order_id = self.ib.client.getReqId()

            logger.info("IBKR connected")

            self.attach_ib_events()
            self.qualify_contracts()

    def allocate_bracket_order_ids(self):

        with self.order_id_lock:
            if self.next_order_id is None:
                self.next_order_id = self.ib.client.getReqId()

            parent_id = self.next_order_id
            tp_id = parent_id + 1
            sl_id = parent_id + 2
            self.next_order_id += 3

        return parent_id, tp_id, sl_id

    def log_trade_snapshot(self, label, trade):

        try:
            order = trade.order
            status = trade.orderStatus

            logger.info(
                f"{label} → "
                f"orderId={getattr(order, 'orderId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"action={getattr(order, 'action', None)} "
                f"orderType={getattr(order, 'orderType', None)} "
                f"lmtPrice={getattr(order, 'lmtPrice', None)} "
                f"auxPrice={getattr(order, 'auxPrice', None)} "
                f"transmit={getattr(order, 'transmit', None)} "
                f"status={getattr(status, 'status', None)} "
                f"filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} "
                f"avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)} "
                f"permId={getattr(status, 'permId', None)}"
            )

            if getattr(trade, "advancedError", None):
                logger.error(f"{label} ADVANCED ERROR → {trade.advancedError}")

            if getattr(trade, "log", None):
                for entry in trade.log:
                    logger.info(
                        f"{label} TRADE LOG → "
                        f"time={getattr(entry, 'time', None)} "
                        f"status={getattr(entry, 'status', None)} "
                        f"message={getattr(entry, 'message', None)} "
                        f"errorCode={getattr(entry, 'errorCode', None)}"
                    )

        except Exception:
            logger.exception(f"{label} SNAPSHOT FAILED")

    def attach_ib_events(self):

        if hasattr(self.ib, "_events_attached"):
            return

        def on_exec(trade, fill):
            logger.info(f"FILL: {fill}")

            self.update_trade_from_fill(fill)

            positions = self.ib.positions()
            if not any(p.position != 0 for p in positions):
                self.trade_state = "IDLE"
                logger.info("→ IDLE")

        def on_open_order(trade):
            order = trade.order
            status = trade.orderStatus
            logger.info(
                f"OPEN ORDER EVENT → "
                f"orderId={getattr(order, 'orderId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"action={getattr(order, 'action', None)} "
                f"orderType={getattr(order, 'orderType', None)} "
                f"lmtPrice={getattr(order, 'lmtPrice', None)} "
                f"auxPrice={getattr(order, 'auxPrice', None)} "
                f"transmit={getattr(order, 'transmit', None)} "
                f"status={getattr(status, 'status', None)} "
                f"filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} "
                f"avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)} "
                f"permId={getattr(status, 'permId', None)}"
            )

        def on_order_status(trade):
            order = trade.order
            status = trade.orderStatus
            logger.info(
                f"ORDER STATUS EVENT → "
                f"orderId={getattr(order, 'orderId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"action={getattr(order, 'action', None)} "
                f"orderType={getattr(order, 'orderType', None)} "
                f"status={getattr(status, 'status', None)} "
                f"filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} "
                f"avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)} "
                f"permId={getattr(status, 'permId', None)}"
            )
            self.update_trade_from_status(trade)

        def on_commission_report(trade, fill, report):
            self.update_trade_commission(trade, fill)

        def on_error(reqId, errorCode, errorString, contract):
            logger.error(
                f"ERROR EVENT → "
                f"reqId={reqId} "
                f"errorCode={errorCode} "
                f"errorString={errorString} "
                f"contract={contract}"
            )
            self.update_trade_from_error(reqId, errorCode, errorString)

        self.ib.execDetailsEvent += on_exec
        self.ib.openOrderEvent += on_open_order
        self.ib.orderStatusEvent += on_order_status
        self.ib.commissionReportEvent += on_commission_report
        self.ib.errorEvent += on_error

        self.ib._events_attached = True

    # ==========================================================
    # CONTRACTS
    # ==========================================================

    def build_base_contract(self, symbol):

        if symbol in ("MNQ", "MES", "M6E"):
            return Future(symbol=symbol, exchange="CME", currency="USD")

        elif symbol == "FDXM":
            return Future(symbol="FDXM", exchange="EUREX", currency="EUR", tradingClass="FDXM")

        elif symbol == "EURUSD":
            return Forex("EURUSD")

        else:
            raise ValueError(f"Unsupported symbol: {symbol}")

    def select_front_month_detail(self, details):

        sorted_details = sorted(
            details,
            key=lambda d: d.contract.lastTradeDateOrContractMonth
        )

        for detail in sorted_details:
            expiry = detail.contract.lastTradeDateOrContractMonth
            if expiry and len(expiry) >= 6:
                return detail

        raise RuntimeError("No valid contract")

    def qualify_contracts(self):

        logger.info("QUALIFY CONTRACTS")

        for sym in ["MNQ", "MES", "M6E", "FDXM"]:

            try:
                base = self.build_base_contract(sym)
                details = self.ib.reqContractDetails(base)

                detail = self.select_front_month_detail(details)
                contract = detail.contract

                self.contract_cache[sym] = contract
                self.contract_min_ticks[sym] = float(getattr(detail, "minTick", TICK_SIZES[sym]))

                logger.info(f"{sym} → {contract.lastTradeDateOrContractMonth}")

            except Exception:
                logger.exception(f"FAILED {sym}")

    def get_contract(self, symbol):

        if symbol in self.contract_cache:
            return self.contract_cache[symbol]

        if symbol == "EURUSD":
            return Forex("EURUSD")

        raise ValueError(f"{symbol} not cached")

    # ==========================================================
    # SIGNAL HANDLING
    # ==========================================================

    def handle_webhook_signal(self, data):

        logger.info(f"WEBHOOK RECEIVED: {data}")

        if not isinstance(data, dict):
            logger.error(f"INVALID PAYLOAD TYPE: {type(data)}")
            return "invalid_payload_type"

        if self.is_diagnostic_payload(data):
            self.log_diagnostic_signal(data)
            return "diagnostic_logged"

        normalized = self.normalize_signal(data)
        self.log_normalized_signal(normalized)

        if not normalized["symbol"] or not normalized["side"]:
            logger.error(
                "INVALID SIGNAL PAYLOAD | "
                f"missing_canonical_fields symbol={normalized['symbol']} side={normalized['side']} "
                f"payload_format={normalized['payload_format']}"
            )
            return "invalid_signal_payload"

        if not normalized["execution_ready"]:
            logger.info(
                "CANDIDATE PAYLOAD LOGGED NO EXECUTION | "
                f"payload_format={normalized['payload_format']} "
                f"signal_id={normalized['signal_id']} "
                f"symbol={normalized['symbol']} "
                f"side={normalized['side']} "
                f"missing_execution_field=entry_price"
            )
            return "candidate_logged_no_execution"

        symbol = normalized["symbol"]
        side = normalized["side"]
        entry = normalized["entry_price"]
        signal_time = normalized["timestamp_utc"] or data.get("time")
        grade = normalized["grade"] or normalized["candidate_grade"]
        enqueue_time = self.utc_now_iso()

        now = time.time()
        key = f"{symbol}-{side}-{round(entry, 8)}"

        if key == self.last_signal and now - self.last_signal_time < 5:
            logger.info("Duplicate ignored")
            return "duplicate_ignored"

        self.last_signal = key
        self.last_signal_time = now

        job = {
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "signal_time": signal_time,
            "enqueue_time": enqueue_time,
            "grade": grade,
            "normalized_signal": normalized,
            "raw_payload": data,
            "payload_format": normalized["payload_format"],
            "schema_version": normalized["schema_version"],
            "signal_id": normalized["signal_id"],
        }

        logger.info(
            "QUEUE PUT | "
            f"symbol={job['symbol']} "
            f"side={job['side']} "
            f"entry={job['entry']} "
            f"grade={job['grade']} "
            f"payload_format={job['payload_format']} "
            f"signal_id={job['signal_id']}"
        )

        self.execution_queue.put(job)
        return "queued"

    # ==========================================================
    # WORKER
    # ==========================================================

    def execution_worker(self):

        asyncio.set_event_loop(asyncio.new_event_loop())

        logger.info("EXECUTION WORKER STARTED")
        logger.info(f"BOT STAGE: {BOT_STAGE}")

        self.connect_ib()

        while True:

            job = self.execution_queue.get()

            logger.info(f"WORKER RECEIVED JOB: {job}")

            try:
                logger.info(f"IB CONNECTED: {self.ib.isConnected()}")
                logger.info("STARTING ORDER EXECUTION")

                self.place_bracket_order(job)

                logger.info("ORDER EXECUTION FINISHED")

            except Exception:
                logger.exception("Execution error")

            self.execution_queue.task_done()

    # ==========================================================
    # 🔥 P033 — EXECUTION QUALITY LOGGING
    # ==========================================================

    def place_bracket_order(self, job):

        symbol = job["symbol"]
        side = job["side"]

        spread_adjusted_entry = self.apply_spread(symbol, side, job["entry"])
        entry = self.round_to_tick(symbol, spread_adjusted_entry)

        contract = self.get_contract(symbol)

        if symbol in ("MES", "MNQ"):
            stop_dist = 2
            target_dist = 4
        elif symbol == "M6E":
            stop_dist = 0.002
            target_dist = 0.004
        else:
            stop_dist = 2
            target_dist = 4

        if side == "long":
            stop = entry - stop_dist
            target = entry + target_dist
            parent_action = "BUY"
            child_action = "SELL"
        else:
            stop = entry + stop_dist
            target = entry - target_dist
            parent_action = "SELL"
            child_action = "BUY"

        stop = self.round_to_tick(symbol, stop)
        target = self.round_to_tick(symbol, target)

        logger.info(
            f"OFFICIAL BRACKET | {symbol} {side} entry={entry} stop={stop} target={target}"
        )

        parent_id, tp_id, sl_id = self.allocate_bracket_order_ids()

        trade_id = self.register_trade_analysis(
            job=job,
            signal_time=job.get("signal_time"),
            enqueue_time=job.get("enqueue_time"),
            execution_start_time=datetime.now(timezone.utc),
            spread_adjusted_entry=spread_adjusted_entry,
            entry=entry,
            stop=stop,
            target=target,
            parent_id=parent_id,
            tp_id=tp_id,
            sl_id=sl_id
        )

        parent = LimitOrder(parent_action, 1, entry)
        parent.orderId = parent_id
        parent.transmit = False
        parent.tif = "GTC"

        tp = LimitOrder(child_action, 1, target)
        tp.orderId = tp_id
        tp.parentId = parent_id
        tp.transmit = False
        tp.tif = "GTC"

        sl = StopOrder(child_action, 1, stop)
        sl.orderId = sl_id
        sl.parentId = parent_id
        sl.transmit = True
        sl.tif = "GTC"

        logger.info(
            f"ORDER DEF PARENT → orderId={parent.orderId} parentId={parent.parentId} "
            f"action={parent.action} orderType={parent.orderType} "
            f"lmtPrice={getattr(parent, 'lmtPrice', None)} auxPrice={getattr(parent, 'auxPrice', None)} "
            f"transmit={parent.transmit}"
        )
        logger.info(
            f"ORDER DEF TP → orderId={tp.orderId} parentId={tp.parentId} "
            f"action={tp.action} orderType={tp.orderType} "
            f"lmtPrice={getattr(tp, 'lmtPrice', None)} auxPrice={getattr(tp, 'auxPrice', None)} "
            f"transmit={tp.transmit}"
        )
        logger.info(
            f"ORDER DEF SL → orderId={sl.orderId} parentId={sl.parentId} "
            f"action={sl.action} orderType={sl.orderType} "
            f"lmtPrice={getattr(sl, 'lmtPrice', None)} auxPrice={getattr(sl, 'auxPrice', None)} "
            f"transmit={sl.transmit}"
        )

        self.append_trade_event(trade_id, "PLACE ORDER START")

        parent_trade = self.ib.placeOrder(contract, parent)
        tp_trade = self.ib.placeOrder(contract, tp)
        sl_trade = self.ib.placeOrder(contract, sl)

        self.log_trade_snapshot("POST PLACE PARENT", parent_trade)
        self.log_trade_snapshot("POST PLACE TP", tp_trade)
        self.log_trade_snapshot("POST PLACE SL", sl_trade)

        self.ib.sleep(0.20)

        self.log_trade_snapshot("POST WAIT PARENT", parent_trade)
        self.log_trade_snapshot("POST WAIT TP", tp_trade)
        self.log_trade_snapshot("POST WAIT SL", sl_trade)

        confirmed_statuses = {"PreSubmitted", "Submitted", "Filled"}
        submission_deadline = time.time() + 2.0
        confirmed = False

        while time.time() < submission_deadline:
            parent_status = getattr(parent_trade.orderStatus, "status", None)
            tp_status = getattr(tp_trade.orderStatus, "status", None)
            sl_status = getattr(sl_trade.orderStatus, "status", None)

            if (
                parent_status in confirmed_statuses
                and tp_status in confirmed_statuses
                and sl_status in confirmed_statuses
            ):
                confirmed = True
                break

            self.ib.sleep(0.10)

        self.log_trade_snapshot("POST CONFIRM PARENT", parent_trade)
        self.log_trade_snapshot("POST CONFIRM TP", tp_trade)
        self.log_trade_snapshot("POST CONFIRM SL", sl_trade)

        open_trades = self.ib.openTrades()

        for open_trade in open_trades:
            order = open_trade.order
            if getattr(order, "orderId", None) in (parent_id, tp_id, sl_id):
                label = "POST OPENTRADES"
                if getattr(order, "orderId", None) == parent_id:
                    label = "POST OPENTRADES PARENT"
                elif getattr(order, "orderId", None) == tp_id:
                    label = "POST OPENTRADES TP"
                elif getattr(order, "orderId", None) == sl_id:
                    label = "POST OPENTRADES SL"
                self.log_trade_snapshot(label, open_trade)

        if not confirmed:
            self.append_anomaly(trade_id, "SUBMISSION_CONFIRMATION_TIMEOUT")
            logger.error(
                f"BRACKET NOT CONFIRMED | trade_id={trade_id} "
                f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id}"
            )
            return

        self.append_trade_event(trade_id, "BRACKET SUBMITTED")
        logger.info("BRACKET SUBMITTED (P034 SUBMISSION CONFIRMATION GUARD)")

    # ==========================================================
    # WATCHDOG
    # ==========================================================

    def ib_watchdog(self):

        asyncio.set_event_loop(asyncio.new_event_loop())

        while True:

            if not self.ib.isConnected():
                logger.warning("Reconnecting...")
                try:
                    self.connect_ib()
                except Exception:
                    logger.exception("Reconnect failed")

            time.sleep(10)

# ==========================================================
# API
# ==========================================================

app = FastAPI()
bot = ScalpingBot()

@app.get("/health")
def health():
    return {
        "status": "ok",
        "bot_name": BOT_NAME,
        "bot_version": BOT_VERSION,
        "bot_patch": BOT_PATCH,
        "bot_stage": BOT_STAGE
    }

@app.post("/webhook/tradingview")
async def webhook_handler(request: Request):

    raw_body = await request.body()
    raw_text = raw_body.decode("utf-8", errors="replace")
    logger.info(f"WEBHOOK RAW BODY: {raw_text}")

    try:
        data = json.loads(raw_text)
    except Exception:
        logger.exception("WEBHOOK JSON PARSE FAILED")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not isinstance(data, dict):
        logger.error(f"WEBHOOK JSON ROOT MUST BE OBJECT, GOT: {type(data)}")
        raise HTTPException(status_code=400, detail="JSON payload must be an object")

    if data.get("secret") != "FDAX_bot_secure_2026":
        raise HTTPException(status_code=403)

    try:
        status = bot.handle_webhook_signal(data)
    except Exception:
        logger.exception("WEBHOOK PROCESSING FAILED")
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

    return {"status": status}