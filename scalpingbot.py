# ==========================================================
# IKBR SCALPING BOT | VERSION v1.6.0 P051 | STAGE: TEST
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
BOT_PATCH = "P051"
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

BRACKET_CONFIRM_STATUSES = {
    "PendingSubmit",
    "ApiPending",
    "PreSubmitted",
    "Submitted",
    "Filled",
}

ACTIVE_TRADE_STATES = {
    "SUBMITTING",
    "ENTRY_WORKING",
    "ENTRY_FILLED",
    "EXIT_WORKING",
    "TP_FILLED",
    "SL_FILLED",
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

        self.connection_lock = threading.RLock()
        self.order_id_lock = threading.Lock()
        self.execution_lock = threading.Lock()
        self.trade_analysis_lock = threading.Lock()

        self.next_order_id = None

        # Session health state
        self.session_socket_connected = False
        self.session_initialized = False
        self.session_healthy = False
        self.session_reconnect_count = 0
        self.last_reconnect_time = None
        self.events_attached = False
        self.session_recovery_in_progress = False

        # Event handler refs for safe detach/reattach
        self.ib_event_handlers = {
            "exec": None,
            "open_order": None,
            "order_status": None,
            "commission": None,
            "error": None,
        }

        # Execution safety
        self.active_execution_trade_id = None

        self.last_signal = None
        self.last_signal_time = 0

        self.last_diagnostic = None
        self.last_diagnostic_time = 0

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

    def get_preferred_reference_price(self, normalized):
        """Get the preferred analytical reference price from normalized signal.
        Prefers entry_reference_price when available, then price, then entry_price.
        """
        ref = normalized.get("entry_reference_price")
        if ref is not None:
            return ref
        ref = normalized.get("price")
        if ref is not None:
            return ref
        return normalized.get("entry_price")

    def apply_spread(self, symbol, side, price):
        spread = SPREADS.get(symbol, 0)

        if side == "long":
            price += spread
        else:
            price -= spread

        logger.info(f"SPREAD | {symbol} {side} → {price}")
        return price

    # ==========================================================
    # DET EXECUTION HELPERS (P049)
    # ==========================================================

    def get_grade_risk_percent(self, execution_grade):
        """Return intended risk percent for execution grade."""
        if execution_grade == "A+":
            return 0.005  # 0.5%
        elif execution_grade == "A":
            return 0.003  # 0.3%
        else:
            return 0.0

    def get_fallback_stop_distance(self, symbol, execution_grade=None):
        """Return instrument-aware fallback stop distance in price units (not ticks).
        Used as temporary pragmatic model; ready for later structure-anchor integration.
        """
        if symbol == "M6E":
            # Forex: 0.00020 price distance (2 pips)
            return 0.00020
        elif symbol == "MES":
            # Micro E-mini S&P 500: 2.00 price distance (8 ticks, 1 tick = 0.25)
            return 2.00
        elif symbol == "MNQ":
            # Micro E-mini Nasdaq: 2.00 price distance (8 ticks, 1 tick = 0.25)
            return 2.00
        elif symbol == "FDXM":
            # Euro DAX: 2.00 price distance (4 ticks, 1 tick = 0.5)
            return 2.00
        else:
            # Default fallback
            return 2.00

    def get_entry_band_limit(self, symbol, execution_grade):
        """Return max entry drift in price distance units for grade and symbol.
        All values return consistent price-distance units (not mixed ticks/pips).
        """
        if symbol == "M6E":
            # Forex: pips converted to price distance (0.0001 = 1 pip)
            return 0.00020 if execution_grade == "A+" else 0.00040  # 2 vs 4 pips
        elif symbol == "MES":
            # MES: 1 tick = 0.25, convert to price distance
            return 1.00 if execution_grade == "A+" else 2.00  # 4 vs 8 ticks
        elif symbol == "MNQ":
            # MNQ: 1 tick = 0.25, convert to price distance
            return 2.00 if execution_grade == "A+" else 3.00  # 8 vs 12 ticks
        elif symbol == "FDXM":
            # FDXM: 1 tick = 0.5, convert to price distance
            return 1.00 if execution_grade == "A+" else 2.00  # 2 vs 4 ticks
        else:
            return 2.00  # Default fallback

    def derive_candidate_executable_entry(self, symbol, side, normalized):
        """Derive the planned executable entry candidate including spread adjustment.
        This is used for entry-band validation to make the check non-trivial.
        Prefers entry_reference_price from Pine when available, otherwise falls back to price.
        """
        # Prefer entry_reference_price from Pine (bot-side analytical reference)
        reference_price = self.get_preferred_reference_price(normalized)
        if reference_price is None:
            return None
        
        # Apply the same spread logic bot will use for execution
        spread_adjusted = self.apply_spread(symbol, side, reference_price)
        
        # Round to tick as bot will do
        executable_candidate = self.round_to_tick(symbol, spread_adjusted)
        
        return executable_candidate

    def validate_entry_band(self, symbol, execution_grade, reference_price, candidate_entry):
        """Validate candidate entry is within allowed band from reference price.
        Both actual_drift and allowed_limit now use consistent price-distance units.
        Uses preferred_reference_price logic: entry_reference_price > price.
        Returns (is_valid, actual_drift, allowed_limit).
        """
        if reference_price is None or candidate_entry is None:
            return (False, None, None)

        # Calculate actual drift in price distance
        actual_drift = abs(candidate_entry - reference_price)
        
        # Get allowed limit in same price-distance units
        allowed_limit = self.get_entry_band_limit(symbol, execution_grade)
        
        is_valid = actual_drift <= allowed_limit

        return (is_valid, actual_drift, allowed_limit)

    def derive_det_stop_price(self, symbol, entry_price, side, execution_grade=None, normalized_signal=None):
        """Derive stop price from final executable entry price.
        Optionally uses structure anchors from Pine when available.
        Falls back to instrument-aware stop distance if anchors unavailable.
        
        For long: prefers lowest valid anchor below entry from {structure_anchor_low, trigger_bar_low}
        For short: prefers highest valid anchor above entry from {structure_anchor_high, trigger_bar_high}
        """
        fallback_stop_dist = self.get_fallback_stop_distance(symbol, execution_grade)
        
        # Try to use structure anchors if normalized_signal provided
        if normalized_signal is not None:
            if side == "long":
                # For long, prefer the lowest valid downside anchor below entry
                structure_anchor_low = normalized_signal.get("structure_anchor_low")
                trigger_bar_low = normalized_signal.get("trigger_bar_low")
                
                candidates = []
                if structure_anchor_low is not None and structure_anchor_low < entry_price:
                    candidates.append(structure_anchor_low)
                if trigger_bar_low is not None and trigger_bar_low < entry_price:
                    candidates.append(trigger_bar_low)
                
                if candidates:
                    anchor_stop = min(candidates)
                    risk_dist = entry_price - anchor_stop
                    if risk_dist > 0:
                        return anchor_stop
            elif side == "short":
                # For short, prefer the highest valid upside anchor above entry
                structure_anchor_high = normalized_signal.get("structure_anchor_high")
                trigger_bar_high = normalized_signal.get("trigger_bar_high")
                
                candidates = []
                if structure_anchor_high is not None and structure_anchor_high > entry_price:
                    candidates.append(structure_anchor_high)
                if trigger_bar_high is not None and trigger_bar_high > entry_price:
                    candidates.append(trigger_bar_high)
                
                if candidates:
                    anchor_stop = max(candidates)
                    risk_dist = anchor_stop - entry_price
                    if risk_dist > 0:
                        return anchor_stop
        
        # Fallback to instrument-aware distance if no valid anchors
        if side == "long":
            return entry_price - fallback_stop_dist
        else:
            return entry_price + fallback_stop_dist

    def derive_target_from_r(self, entry_price, stop_price, side):
        """Derive target from entry and stop (2R target).
        1R = abs(entry - stop)
        target = entry ± 2R
        """
        r_distance = abs(entry_price - stop_price)
        
        if side == "long":
            return entry_price + (2 * r_distance)
        else:
            return entry_price - (2 * r_distance)

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
            "det_classification": job.get("det_classification", ""),
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

            self.active_execution_trade_id = trade_id

            self.append_trade_event(
                trade_id,
                f"REGISTERED symbol={record['symbol']} side={record['side']} "
                f"grade={record['grade']} det_classification={record['det_classification']} "
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

    def has_active_trade_locked(self):
        with self.trade_analysis_lock:
            for record in self.trade_analysis.values():
                if record["summary_logged"]:
                    continue
                if record["state"] in ACTIVE_TRADE_STATES:
                    return True, record["trade_id"], record["symbol"], record["state"]
            return False, None, None, None

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

            if self.active_execution_trade_id == trade_id:
                self.active_execution_trade_id = None

            duration_to_fill = self.seconds_between(record["execution_start_time"], record["entry_fill_time"])
            duration_in_trade = self.seconds_between(record["entry_fill_time"], record["exit_fill_time"])
            execution_metrics = self.calculate_execution_quality_metrics(record)

            logger.info(
                "TRADE SUMMARY | "
                f"trade_id={record['trade_id']} "
                f"symbol={record['symbol']} "
                f"side={record['side']} "
                f"grade={record['grade']} "
                f"det_classification={record['det_classification']} "
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

    def _infer_side_from_context(self, payload):
        event = self._safe_str(payload.get("event"), default="").lower()

        if event in {"ctx_long_on", "setup_long_on"}:
            return "long"
        if event in {"ctx_short_on", "setup_short_on"}:
            return "short"

        long_score = 0
        short_score = 0

        if self._safe_bool(payload.get("htf_setup_long"), default=False):
            long_score += 4
        if self._safe_bool(payload.get("htf_setup_short"), default=False):
            short_score += 4

        if self._safe_bool(payload.get("htf_structure_long"), default=False):
            long_score += 3
        if self._safe_bool(payload.get("htf_structure_short"), default=False):
            short_score += 3

        if self._safe_bool(payload.get("htf_trend_up"), default=False):
            long_score += 2
        if self._safe_bool(payload.get("htf_trend_down"), default=False):
            short_score += 2

        if self._safe_bool(payload.get("htf_vwap_up"), default=False):
            long_score += 2
        if self._safe_bool(payload.get("htf_vwap_down"), default=False):
            short_score += 2

        if self._safe_bool(payload.get("htf_moved_away_long"), default=False):
            long_score += 1
        if self._safe_bool(payload.get("htf_moved_away_short"), default=False):
            short_score += 1

        bias_5m = self._safe_str(payload.get("bias_5m"), default="").lower()
        if bias_5m in {"long", "bull", "up"}:
            long_score += 1
        elif bias_5m in {"short", "bear", "down"}:
            short_score += 1

        candidate_side = self._safe_str(payload.get("candidate_side"), default="").lower()
        if candidate_side in {"long", "buy", "bull", "up"}:
            long_score += 5
        elif candidate_side in {"short", "sell", "bear", "down"}:
            short_score += 5

        sweep_side = self._safe_str(payload.get("sweep_side"), default="").lower()
        if sweep_side == "down":
            long_score += 1
        elif sweep_side == "up":
            short_score += 1

        blocker = self._safe_str(payload.get("blocker"), default="").lower()
        if blocker == "late_failed":
            if self._safe_bool(payload.get("htf_not_late_long"), default=True) is False and self._safe_bool(payload.get("htf_not_late_short"), default=True):
                long_score += 1
            elif self._safe_bool(payload.get("htf_not_late_short"), default=True) is False and self._safe_bool(payload.get("htf_not_late_long"), default=True):
                short_score += 1

        if long_score > short_score:
            return "long"
        if short_score > long_score:
            return "short"

        return None

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

        if side in {"long", "short"}:
            return side

        return self._infer_side_from_context(payload)

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
            return [item.strip() for item in raw.replace("|", ",").split(",") if item.strip()]
        return [str(value)]

    def detect_payload_format(self, payload):
        if self.is_diagnostic_payload(payload):
            return "diagnostic"

        enriched_keys = {
            "schema_version",
            "signal_id",
            "timestamp_utc",
            "bar_time_unix_ms",
            "candidate_side",
            "strategy_family",
            "tv_score",
            "tv_candidate_grade",
            "candidate_grade",
            "reason_flags",
            "mode",
            "event",
            "detector_state",
            "execution_candidate_valid",
            "price",
            "blocker",
            "primary_blocker",
            "blocker_count",
            "pine_pass_count",
            "pine_fail_count",
            "pine_detector_score",
            "body_strength_value",
            "body_strength_threshold",
            "distance_from_vwap_atr_value",
            "distance_from_vwap_atr_threshold",
            "ema_spread_atr_value",
            "ema_spread_atr_threshold",
            "not_choppy_flag",
            "late_filter_flag",
            "moved_away_flag",
            "structure_flag",
            "setup_flag",
            "trigger_flag",
            "session_valid",
            "vwap_bias_valid",
            "body_strength_valid",
            "structure_valid",
            "setup_valid",
            "trigger_valid",
            "entry_reference_price",
            "trigger_bar_high",
            "trigger_bar_low",
            "structure_anchor_low",
            "structure_anchor_high",
            "htf_trend_up",
            "htf_trend_down",
            "htf_vwap_up",
            "htf_vwap_down",
            "htf_vwap_not_flat",
            "htf_not_choppy",
            "htf_not_late_long",
            "htf_not_late_short",
            "htf_moved_away_long",
            "htf_moved_away_short",
            "htf_structure_long",
            "htf_structure_short",
            "htf_setup_long",
            "htf_setup_short",
            "distance_from_vwap_atr",
            "htf_ema_spread_atr",
            "body_strength",
        }
        if any(key in payload for key in enriched_keys):
            return "enriched_candidate"
        return "legacy_execution"

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
        candidate_grade = self._safe_str(payload.get("candidate_grade"), default="")
        if not candidate_grade:
            candidate_grade = self._safe_str(payload.get("tv_candidate_grade"), default="")
        if not candidate_grade:
            candidate_grade = grade

        timestamp_utc = self._safe_str(
            payload.get("timestamp_utc", payload.get("time")),
            default="",
        )

        normalized = {
            "raw_payload": payload,
            "payload_format": payload_format,
            "schema_version": payload.get("schema_version"),
            "signal_id": self._safe_str(payload.get("signal_id"), default=""),
            "timestamp_utc": timestamp_utc,
            "time": timestamp_utc,
            "bar_time_unix_ms": payload.get("bar_time_unix_ms"),
            "symbol": symbol,
            "normalized_symbol": symbol,
            "timeframe": self._safe_str(payload.get("timeframe"), default=""),
            "instrument_type": self._safe_str(payload.get("instrument_type"), default="unknown"),
            "strategy_family": self._safe_str(payload.get("strategy_family"), default="unknown"),
            "mode": self._safe_str(payload.get("mode"), default="").lower(),
            "event": self._safe_str(payload.get("event"), default="").lower(),
            "detector_state": self._safe_str(payload.get("detector_state"), default="").lower(),
            "candidate_side": self._safe_str(payload.get("candidate_side"), default="").lower(),
            "side": side,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "price": self._safe_float(payload.get("price"), default=entry_price),
            "blocker": self._safe_str(payload.get("blocker"), default=""),
            "primary_blocker": self._safe_str(payload.get("primary_blocker"), default=""),
            "blocker_count": self._safe_int(payload.get("blocker_count"), default=None),
            "grade": grade,
            "candidate_grade": candidate_grade,
            "tv_score": self._safe_float(payload.get("tv_score"), default=None),
            "body_strength_value": self._safe_float(payload.get("body_strength_value"), default=None),
            "body_strength_threshold": self._safe_float(payload.get("body_strength_threshold"), default=None),
            "distance_from_vwap_atr_value": self._safe_float(payload.get("distance_from_vwap_atr_value"), default=None),
            "distance_from_vwap_atr_threshold": self._safe_float(payload.get("distance_from_vwap_atr_threshold"), default=None),
            "ema_spread_atr_value": self._safe_float(payload.get("ema_spread_atr_value"), default=None),
            "ema_spread_atr_threshold": self._safe_float(payload.get("ema_spread_atr_threshold"), default=None),
            "not_choppy_flag": self._safe_bool(payload.get("not_choppy_flag"), default=False),
            "late_filter_flag": self._safe_bool(payload.get("late_filter_flag"), default=False),
            "moved_away_flag": self._safe_bool(payload.get("moved_away_flag"), default=False),
            "structure_flag": self._safe_bool(payload.get("structure_flag"), default=False),
            "setup_flag": self._safe_bool(payload.get("setup_flag"), default=False),
            "trigger_flag": self._safe_bool(payload.get("trigger_flag"), default=False),
            "session_valid": self._safe_bool(payload.get("session_valid"), default=False),
            "vwap_bias_valid": self._safe_bool(payload.get("vwap_bias_valid"), default=False),
            "body_strength_valid": self._safe_bool(payload.get("body_strength_valid"), default=False),
            "structure_valid": self._safe_bool(payload.get("structure_valid"), default=False),
            "setup_valid": payload.get("setup_valid"),
            "trigger_valid": payload.get("trigger_valid"),
            "execution_candidate_valid": self._safe_bool(payload.get("execution_candidate_valid"), default=False),
            "pine_pass_count": self._safe_int(payload.get("pine_pass_count"), default=None),
            "pine_fail_count": self._safe_int(payload.get("pine_fail_count"), default=None),
            "pine_detector_score": self._safe_int(payload.get("pine_detector_score"), default=None),
            "entry_reference_price": self._safe_float(payload.get("entry_reference_price"), default=None),
            "trigger_bar_high": self._safe_float(payload.get("trigger_bar_high"), default=None),
            "trigger_bar_low": self._safe_float(payload.get("trigger_bar_low"), default=None),
            "structure_anchor_low": self._safe_float(payload.get("structure_anchor_low"), default=None),
            "structure_anchor_high": self._safe_float(payload.get("structure_anchor_high"), default=None),
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
            "htf_trend_up": self._safe_bool(payload.get("htf_trend_up"), default=False),
            "htf_trend_down": self._safe_bool(payload.get("htf_trend_down"), default=False),
            "htf_vwap_up": self._safe_bool(payload.get("htf_vwap_up"), default=False),
            "htf_vwap_down": self._safe_bool(payload.get("htf_vwap_down"), default=False),
            "htf_vwap_not_flat": self._safe_bool(payload.get("htf_vwap_not_flat"), default=False),
            "htf_not_choppy": self._safe_bool(payload.get("htf_not_choppy"), default=True),
            "htf_not_late_long": self._safe_bool(payload.get("htf_not_late_long"), default=True),
            "htf_not_late_short": self._safe_bool(payload.get("htf_not_late_short"), default=True),
            "htf_moved_away_long": self._safe_bool(payload.get("htf_moved_away_long"), default=False),
            "htf_moved_away_short": self._safe_bool(payload.get("htf_moved_away_short"), default=False),
            "htf_structure_long": self._safe_bool(payload.get("htf_structure_long"), default=False),
            "htf_structure_short": self._safe_bool(payload.get("htf_structure_short"), default=False),
            "htf_setup_long": self._safe_bool(payload.get("htf_setup_long"), default=False),
            "htf_setup_short": self._safe_bool(payload.get("htf_setup_short"), default=False),
            "distance_from_vwap_atr": self._safe_float(payload.get("distance_from_vwap_atr"), default=None),
            "htf_ema_spread_atr": self._safe_float(payload.get("htf_ema_spread_atr"), default=None),
            "body_strength": self._safe_float(payload.get("body_strength"), default=None),
        }

        normalized["candidate_side"] = normalized["candidate_side"] or normalized["side"]
        normalized["execution_ready"] = (
            payload_format == "legacy_execution" and
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
            "mode": normalized["mode"],
            "event": normalized["event"],
            "symbol": normalized["symbol"],
            "normalized_symbol": normalized["normalized_symbol"],
            "side": normalized["side"],
            "entry_price": normalized["entry_price"],
            "price": normalized["price"],
            "entry_reference_price": normalized["entry_reference_price"],
            "trigger_bar_high": normalized["trigger_bar_high"],
            "trigger_bar_low": normalized["trigger_bar_low"],
            "structure_anchor_low": normalized["structure_anchor_low"],
            "structure_anchor_high": normalized["structure_anchor_high"],
            "blocker": normalized["blocker"],
            "grade": normalized["grade"],
            "candidate_grade": normalized["candidate_grade"],
            "tv_score": normalized["tv_score"],
            "body_strength_value": normalized["body_strength_value"],
            "setup_valid": normalized["setup_valid"],
            "trigger_valid": normalized["trigger_valid"],
            "session_name": normalized["session_name"],
            "bias_5m": normalized["bias_5m"],
            "reason_flags": normalized["reason_flags"],
            "execution_ready": normalized["execution_ready"],
            "htf_trend_up": normalized["htf_trend_up"],
            "htf_trend_down": normalized["htf_trend_down"],
            "htf_vwap_up": normalized["htf_vwap_up"],
            "htf_vwap_down": normalized["htf_vwap_down"],
            "htf_vwap_not_flat": normalized["htf_vwap_not_flat"],
            "htf_not_choppy": normalized["htf_not_choppy"],
            "htf_not_late_long": normalized["htf_not_late_long"],
            "htf_not_late_short": normalized["htf_not_late_short"],
            "htf_moved_away_long": normalized["htf_moved_away_long"],
            "htf_moved_away_short": normalized["htf_moved_away_short"],
            "htf_structure_long": normalized["htf_structure_long"],
            "htf_structure_short": normalized["htf_structure_short"],
            "htf_setup_long": normalized["htf_setup_long"],
            "htf_setup_short": normalized["htf_setup_short"],
            "distance_from_vwap_atr": normalized["distance_from_vwap_atr"],
            "htf_ema_spread_atr": normalized["htf_ema_spread_atr"],
            "body_strength": normalized["body_strength"],
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

    def evaluate_stage_execution_policy(self, normalized):
        stage = BOT_STAGE
        payload_format = normalized["payload_format"]
        policy_branch = "classification_only"
        policy_reason = (
            "non-legacy execution paths remain classification-only; "
            "diagnostic payloads follow this branch"
        )
        allow_queue = False

        if payload_format == "enriched_candidate":
            if stage == "TEST":
                policy_branch = "future_det_gated_execution_test_enabled"
                allow_queue = True
                policy_reason = (
                    "DET-gated execution is enabled for TEST only; "
                    "enriched candidates may execute if DET gate permits"
                )
            else:
                policy_branch = "future_det_gated_execution_restricted"
                allow_queue = False
                policy_reason = (
                    "DET-gated execution remains disabled for PAPER/LIVE in this patch"
                )
        elif payload_format == "legacy_execution":
            policy_branch = "legacy_execution_deprecated"
            allow_queue = False
            policy_reason = (
                "legacy execution payloads are deprecated and denied for queueing; "
                "enriched candidate input must be used instead"
            )
        else:
            policy_branch = "classification_only"

        return {
            "stage": stage,
            "policy_branch": policy_branch,
            "allow_queue": allow_queue,
            "reason": policy_reason,
        }

    def assess_det_signal(self, normalized):
        side = normalized["side"]
        hard_blockers = []
        soft_blockers = []
        secondary_reasons = []

        # Use new Pine fields as primary, with old as fallback
        session_valid = normalized.get("session_valid", True)

        vwap_bias_valid = normalized.get("vwap_bias_valid", False)
        if not vwap_bias_valid:
            # Fallback to old calculation
            if side == "long":
                vwap_bias_valid = normalized["htf_vwap_not_flat"] and normalized["htf_vwap_up"] and not normalized["htf_vwap_down"]
            elif side == "short":
                vwap_bias_valid = normalized["htf_vwap_not_flat"] and normalized["htf_vwap_down"] and not normalized["htf_vwap_up"]

        structure_valid = normalized.get("structure_valid", False)
        if not structure_valid:
            # Fallback to old calculation
            if side == "long":
                structure_valid = normalized["htf_structure_long"] and not normalized["htf_structure_short"]
            elif side == "short":
                structure_valid = normalized["htf_structure_short"] and not normalized["htf_structure_long"]

        # P050: Use Pine setup_valid directly as primary signal
        setup_valid = normalized.get("setup_valid")
        if setup_valid is None:
            # Fallback to old calculation
            if side == "long":
                setup_valid = normalized["htf_setup_long"]
            elif side == "short":
                setup_valid = normalized["htf_setup_short"]
        # If Pine provided False, keep False; no override

        # P050: Use Pine trigger_valid directly as primary signal
        trigger_valid = normalized.get("trigger_valid")
        if trigger_valid is None:
            # Fallback to old trigger logic
            trigger_valid = (
                normalized["reacceleration_1m_ok"]
                or normalized["structure_1m_ok"]
                or normalized["rejection_detected"]
                or (
                    normalized["body_strength"] is not None
                    and normalized["body_strength"] >= 0.70
                )
            )
            if normalized["event"] == "blocked_snapshot":
                trigger_valid = False
        # If Pine provided False, keep False; no override

        conflicting_context = False
        if side == "long":
            conflicting_context = normalized["htf_trend_down"] or normalized["htf_vwap_down"]
        elif side == "short":
            conflicting_context = normalized["htf_trend_up"] or normalized["htf_vwap_up"]

        late_invalid = False
        late_filter_flag = normalized.get("late_filter_flag", None)
        if late_filter_flag is not None:
            late_invalid = not late_filter_flag
        else:
            # Fallback to old
            if side == "long":
                late_invalid = not normalized["htf_not_late_long"]
            elif side == "short":
                late_invalid = not normalized["htf_not_late_short"]

        not_choppy_flag = normalized.get("not_choppy_flag", None)
        chop_failed = False
        if not_choppy_flag is not None:
            chop_failed = not not_choppy_flag
        else:
            chop_failed = not normalized["htf_not_choppy"]

        if normalized["blocker"] == "chop_failed":
            chop_failed = True

        if not vwap_bias_valid:
            hard_blockers.append("no_vwap_bias")

        if chop_failed:
            hard_blockers.append("chop")

        if late_invalid or normalized["blocker"] == "late_failed":
            hard_blockers.append("late")

        if conflicting_context:
            hard_blockers.append("conflicting_context")

        if not structure_valid or normalized["blocker"] == "structure_failed":
            hard_blockers.append("poor_structure")

        if normalized["blocker"]:
            secondary_reasons.append(f"pine_blocker:{normalized['blocker']}")

        if normalized["reason_flags"]:
            secondary_reasons.extend([f"reason_flag:{flag}" for flag in normalized["reason_flags"]])

        distance_from_vwap_atr = normalized.get("distance_from_vwap_atr_value", normalized.get("distance_from_vwap_atr"))
        if distance_from_vwap_atr is not None:
            if distance_from_vwap_atr >= 1.5:
                soft_blockers.append("extended_from_vwap")
            if distance_from_vwap_atr >= 2.0 and "late" not in hard_blockers:
                hard_blockers.append("late_extension_proxy")

        # P050: Use Pine body_strength_value directly when available
        body_strength_value = normalized.get("body_strength_value")
        if body_strength_value is None:
            body_strength_value = normalized.get("body_strength")
        
        body_strength_valid = normalized.get("body_strength_valid", None)
        if body_strength_valid is not None:
            if not body_strength_valid:
                soft_blockers.append("weak_body_strength")
        elif body_strength_value is not None and body_strength_value < 0.60:
            soft_blockers.append("weak_body_strength")

        ema_spread_atr = normalized.get("ema_spread_atr_value", normalized.get("htf_ema_spread_atr"))
        if ema_spread_atr is not None and ema_spread_atr < 0.20:
            soft_blockers.append("low_ema_spread")

        if not setup_valid:
            soft_blockers.append("no_setup")

        if not trigger_valid:
            soft_blockers.append("weak_or_missing_trigger")

        # P049: Enforce session_valid as hard blocker if explicitly false
        if not session_valid:
            hard_blockers.append("session_invalid")

        if hard_blockers:
            det_classification = "REJECT"
            primary_reason = hard_blockers[0]
        else:
            strong_alignment = False
            if side == "long":
                strong_alignment = (
                    normalized["htf_trend_up"]
                    and normalized["htf_vwap_up"]
                    and structure_valid
                    and setup_valid
                    and trigger_valid
                    and (not_choppy_flag if not_choppy_flag is not None else normalized["htf_not_choppy"])
                    and (late_filter_flag if late_filter_flag is not None else normalized["htf_not_late_long"])
                )
            elif side == "short":
                strong_alignment = (
                    normalized["htf_trend_down"]
                    and normalized["htf_vwap_down"]
                    and structure_valid
                    and setup_valid
                    and trigger_valid
                    and (not_choppy_flag if not_choppy_flag is not None else normalized["htf_not_choppy"])
                    and (late_filter_flag if late_filter_flag is not None else normalized["htf_not_late_short"])
                )

            base_a_quality = (
                vwap_bias_valid
                and structure_valid
                and setup_valid
                and trigger_valid
            )

            if strong_alignment and not soft_blockers and body_strength_value is not None and body_strength_value >= 0.85:
                det_classification = "EXECUTE_A_PLUS"
                primary_reason = "strong_aligned_context"
            elif base_a_quality and len(soft_blockers) <= 1:
                det_classification = "EXECUTE_A"
                primary_reason = "valid_setup_and_trigger"
            else:
                det_classification = "SHADOW"
                if soft_blockers:
                    primary_reason = soft_blockers[0]
                else:
                    primary_reason = "borderline_context"

        return {
            "session_valid": session_valid,
            "vwap_bias_valid": vwap_bias_valid,
            "structure_valid": structure_valid,
            "setup_valid": setup_valid,
            "trigger_valid": trigger_valid,
            "hard_blockers": list(dict.fromkeys(hard_blockers)),
            "soft_blockers": list(dict.fromkeys(soft_blockers)),
            "primary_reason": primary_reason,
            "secondary_reasons": list(dict.fromkeys(secondary_reasons)),
            "det_classification": det_classification,
        }

    def build_classification_result(self, normalized, assessment):
        det_classification = assessment["det_classification"]
        execution_permission = det_classification in ("EXECUTE_A", "EXECUTE_A_PLUS")
        execution_grade = ""

        if det_classification == "EXECUTE_A_PLUS":
            execution_grade = "A+"
        elif det_classification == "EXECUTE_A":
            execution_grade = "A"

        return {
            "det_classification": det_classification,
            "execution_permission": execution_permission,
            "execution_grade": execution_grade,
            "primary_reason": assessment["primary_reason"],
            "secondary_reasons": assessment["secondary_reasons"],
            "hard_blockers": assessment["hard_blockers"],
            "soft_blockers": assessment["soft_blockers"],
            "pine_blocker": normalized["blocker"],
            "bot_primary_reason": assessment["primary_reason"],
        }

    def log_det_result(self, normalized, assessment, classification_result):
        assessment_summary = {
            "payload_format": normalized["payload_format"],
            "signal_id": normalized["signal_id"],
            "symbol": normalized["symbol"],
            "side": normalized["side"],
            "session_valid": assessment["session_valid"],
            "vwap_bias_valid": assessment["vwap_bias_valid"],
            "structure_valid": assessment["structure_valid"],
            "setup_valid": assessment["setup_valid"],
            "trigger_valid": assessment["trigger_valid"],
            "hard_blockers": assessment["hard_blockers"],
            "soft_blockers": assessment["soft_blockers"],
        }

        logger.info(f"DET ASSESSMENT | {json.dumps(assessment_summary, sort_keys=True)}")
        logger.info(f"DET CLASSIFICATION RESULT | {json.dumps(classification_result, sort_keys=True)}")
        logger.info(
            "DET REASON COMPARE | "
            f"pine_blocker={classification_result['pine_blocker']} "
            f"bot_primary_reason={classification_result['bot_primary_reason']}"
        )

    def evaluate_det_execution_gate(self, classification_result):
        det_classification = classification_result["det_classification"]
        stage = BOT_STAGE

        gate_eligible = det_classification in ("EXECUTE_A_PLUS", "EXECUTE_A")
        gate_enabled = stage == "TEST"
        gate_branch = "det_execution_gate"
        queue_allowed = gate_enabled and gate_eligible

        reason = (
            "DET execution gate evaluated; "
            f"classification={det_classification} eligible={gate_eligible} "
            f"enabled={gate_enabled} stage={stage}"
        )

        return {
            "gate_branch": gate_branch,
            "gate_enabled": gate_enabled,
            "gate_eligible": gate_eligible,
            "queue_allowed": queue_allowed,
            "det_classification": det_classification,
            "stage": stage,
            "reason": reason,
        }

    # ==========================================================
    # IB CONNECTION & SESSION HEALTH
    # ==========================================================

    def update_session_health(self):
        try:
            socket_connected = self.ib.isConnected()
            initialization_complete = (
                socket_connected and
                self.next_order_id is not None and
                self.is_baseline_contract_cache_ready() and
                self.events_attached
            )

            self.session_socket_connected = socket_connected
            self.session_initialized = initialization_complete
            self.session_healthy = socket_connected and initialization_complete
        except Exception:
            self.session_socket_connected = False
            self.session_initialized = False
            self.session_healthy = False

    def log_session_health(self, label):
        logger.info(
            f"SESSION HEALTH | {label} | "
            f"socket_connected={self.session_socket_connected} "
            f"initialized={self.session_initialized} "
            f"healthy={self.session_healthy} "
            f"reconnect_count={self.session_reconnect_count} "
            f"order_id={self.next_order_id} "
            f"contract_cache_size={len(self.contract_cache)} "
            f"events_attached={self.events_attached}"
        )

    def ensure_order_id_initialized(self):
        with self.order_id_lock:
            if self.next_order_id is None:
                self.next_order_id = self.ib.client.getReqId()
                logger.info(f"ORDER ID INITIALIZED | next_order_id={self.next_order_id}")
            else:
                logger.info(f"ORDER ID AVAILABLE | next_order_id={self.next_order_id}")

    def detach_ib_events(self):
        try:
            if self.ib_event_handlers["exec"] is not None:
                self.ib.execDetailsEvent -= self.ib_event_handlers["exec"]
            if self.ib_event_handlers["open_order"] is not None:
                self.ib.openOrderEvent -= self.ib_event_handlers["open_order"]
            if self.ib_event_handlers["order_status"] is not None:
                self.ib.orderStatusEvent -= self.ib_event_handlers["order_status"]
            if self.ib_event_handlers["commission"] is not None:
                self.ib.commissionReportEvent -= self.ib_event_handlers["commission"]
            if self.ib_event_handlers["error"] is not None:
                self.ib.errorEvent -= self.ib_event_handlers["error"]
        except Exception:
            logger.exception("EVENT HANDLER DETACH FAILED")
        finally:
            self.ib_event_handlers = {
                "exec": None,
                "open_order": None,
                "order_status": None,
                "commission": None,
                "error": None,
            }
            self.events_attached = False
            logger.info("EVENT HANDLERS DETACHED / RESET")

    def attach_ib_events(self, force_reset=False):
        if force_reset:
            logger.info("EVENT HANDLER FORCE RESET REQUESTED")
            self.detach_ib_events()

        if self.events_attached:
            logger.info("EVENT HANDLERS ALREADY ATTACHED (SKIPPED)")
            return

        logger.info("ATTACHING NEW EVENT HANDLERS")

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

        self.ib_event_handlers["exec"] = on_exec
        self.ib_event_handlers["open_order"] = on_open_order
        self.ib_event_handlers["order_status"] = on_order_status
        self.ib_event_handlers["commission"] = on_commission_report
        self.ib_event_handlers["error"] = on_error

        self.events_attached = True
        logger.info("EVENT HANDLERS ATTACHED SUCCESSFULLY")

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

    def ensure_contract_cache_initialized(self):
        if self.is_baseline_contract_cache_ready():
            logger.info(f"CONTRACT CACHE BASELINE READY | size={len(self.contract_cache)}")
            return

        logger.info("CONTRACT CACHE MISSING BASELINE | qualifying contracts")
        self.qualify_contracts()

    def is_baseline_contract_cache_ready(self):
        required = {"MNQ", "MES", "M6E", "FDXM"}
        missing = required - set(self.contract_cache.keys())
        if missing:
            return False
        return True

    def force_session_recovery(self, reason_label):
        logger.warning(f"FORCED SESSION RECOVERY (SOCKET ALIVE BUT SESSION UNHEALTHY) | reason={reason_label}")
        self.log_session_health(f"FORCED_RECOVERY_START_{reason_label}")

        with self.connection_lock:
            if self.session_recovery_in_progress:
                logger.warning(
                    "FORCED RECOVERY SKIPPED | already in progress | "
                    f"reason={reason_label} | socket_connected={self.ib.isConnected()} | "
                    f"session_healthy={self.session_healthy} session_initialized={self.session_initialized}"
                )
                return

            self.session_recovery_in_progress = True
            try:
                if self.ib.isConnected():
                    self.attach_ib_events(force_reset=True)
                    self.ensure_order_id_initialized()
                    self.ensure_contract_cache_initialized()
                    self.update_session_health()

                    postfailure = []
                    if not self.ib.isConnected():
                        postfailure.append("socket_disconnected")
                    if self.next_order_id is None:
                        postfailure.append("missing_next_order_id")
                    if not self.events_attached:
                        postfailure.append("events_not_attached")
                    if not self.is_baseline_contract_cache_ready():
                        postfailure.append("baseline_contract_cache_incomplete")

                    if postfailure:
                        logger.error(
                            "FORCED RECOVERY FAILED POSTCONDITIONS | "
                            f"reason={reason_label} failures={','.join(postfailure)}"
                        )
                        self.update_session_health()
                        self.log_session_health(f"FORCED_RECOVERY_INCOMPLETE_{reason_label}")
                        return

                    self.update_session_health()
                    self.log_session_health(f"FORCED_RECOVERY_COMPLETE_{reason_label}")
                else:
                    logger.warning("FORCED SESSION RECOVERY SKIPPED | socket not connected")
            finally:
                self.session_recovery_in_progress = False

    def connect_ib(self):
        self.update_session_health()

        if self.session_healthy:
            self.log_session_health("CONNECT SKIPPED (SESSION_HEALTHY)")
            return

        with self.connection_lock:
            self.update_session_health()

            if self.session_healthy:
                self.log_session_health("CONNECT SKIPPED (SESSION_HEALTHY_RECHECK)")
                return

            socket_connected = self.ib.isConnected()

            if not socket_connected:
                logger.info("CONNECTING TO IBKR")
                self.log_session_health("CONNECT START (SOCKET_CONNECT_REQUIRED)")

                try:
                    self.ib.connect(self.IB_HOST, self.IB_PORT, clientId=self.IB_CLIENT_ID)
                except Exception:
                    logger.exception("CONNECTION FAILED")
                    self.update_session_health()
                    self.log_session_health("CONNECT FAILED")
                    raise

                if not self.ib.isConnected():
                    logger.error("CONNECTION CHECK FAILED")
                    self.update_session_health()
                    self.log_session_health("CONNECT CHECK FAILED")
                    raise RuntimeError("IBKR connection check failed")

                logger.info("SOCKET CONNECTED")
            else:
                logger.info("CONNECT_IB CONTINUING INITIALIZATION ON EXISTING SOCKET")
                self.log_session_health("CONNECT CONTINUE (SOCKET_ALREADY_CONNECTED_SESSION_NOT_HEALTHY)")

            self.ensure_order_id_initialized()

            if not self.events_attached:
                logger.info("ENSURING EVENT HANDLERS")
                self.attach_ib_events()
            else:
                logger.info("EVENT HANDLERS FLAG TRUE | preserving current attachment state")

            if not self.is_baseline_contract_cache_ready():
                logger.info("ENSURING CONTRACT QUALIFICATION")
                self.qualify_contracts()
            else:
                logger.info(f"CONTRACT CACHE BASELINE READY | size={len(self.contract_cache)}")

            self.update_session_health()
            self.log_session_health("CONNECT COMPLETE")

            if not self.session_healthy:
                raise RuntimeError("IBKR session initialization incomplete after connect_ib")

            logger.info("IBKR FULLY INITIALIZED")

    def ensure_symbol_contract_ready(self, symbol):
        if symbol in self.contract_cache:
            return True

        logger.warning(f"CONTRACT CACHE MISS | symbol={symbol} | attempting re-qualification")

        try:
            if symbol == "EURUSD":
                self.contract_cache[symbol] = Forex("EURUSD")
                logger.info("CONTRACT CACHE RECOVERED | symbol=EURUSD")
                return True

            base = self.build_base_contract(symbol)
            details = self.ib.reqContractDetails(base)
            detail = self.select_front_month_detail(details)
            contract = detail.contract

            self.contract_cache[symbol] = contract
            self.contract_min_ticks[symbol] = float(getattr(detail, "minTick", TICK_SIZES[symbol]))

            logger.info(
                f"CONTRACT CACHE RECOVERED | symbol={symbol} expiry={contract.lastTradeDateOrContractMonth}"
            )
            return True
        except Exception:
            logger.exception(f"CONTRACT CACHE RECOVERY FAILED | symbol={symbol}")
            return False

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

    def get_contract(self, symbol):
        if symbol in self.contract_cache:
            return self.contract_cache[symbol]

        if symbol == "EURUSD":
            return Forex("EURUSD")

        raise ValueError(f"{symbol} not cached")

    # ==========================================================
    # SIGNAL HANDLING
    # ==========================================================

    def build_det_execution_job(self, normalized, classification_result):
        """Build execution job with DET planning details.
        P050: Includes new enriched fields for structure-aware stop derivation.
        P049: Moved stop/target derivation to place_bracket_order() after final executable entry.
        This job now carries planning info only; final R is computed at execution time.
        """
        entry = self.derive_entry_price_from_candidate(normalized)
        execution_grade = classification_result["execution_grade"]
        reference_price = self.get_preferred_reference_price(normalized)
        symbol = normalized["symbol"]
        side = normalized["side"]

        # Get intended risk percent for grade
        intended_risk_percent = self.get_grade_risk_percent(execution_grade)

        return {
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "signal_time": normalized["timestamp_utc"] or normalized["time"],
            "enqueue_time": self.utc_now_iso(),
            "grade": execution_grade,
            "intended_risk_percent": intended_risk_percent,
            "det_classification": classification_result["det_classification"],
            "reference_price": reference_price,
            "normalized_signal": normalized,
            "raw_payload": normalized["raw_payload"],
            "payload_format": normalized["payload_format"],
            "schema_version": normalized["schema_version"],
            "signal_id": normalized["signal_id"],
        }

    def derive_entry_price_from_candidate(self, normalized):
        if normalized["payload_format"] == "enriched_candidate":
            price_ref = self.get_preferred_reference_price(normalized)
            if price_ref is None:
                return None
            if normalized["symbol"]:
                return self.round_to_tick(normalized["symbol"], price_ref)
            return price_ref

        return normalized["entry_price"] if normalized["entry_price"] is not None else normalized["price"]

    def handle_webhook_signal(self, data):
        logger.info(f"WEBHOOK RECEIVED: {data}")

        if not isinstance(data, dict):
            logger.error(f"INVALID PAYLOAD TYPE: {type(data)}")
            return "invalid_payload_type"

        normalized = self.normalize_signal(data)
        self.log_normalized_signal(normalized)

        if normalized["payload_format"] == "diagnostic":
            self.log_diagnostic_signal(data)
            assessment = self.assess_det_signal(normalized)
            classification_result = self.build_classification_result(normalized, assessment)
            self.log_det_result(normalized, assessment, classification_result)
            logger.info(
                "QUEUE DECISION | "
                f"path=classification_only "
                f"payload_format={normalized['payload_format']} "
                f"queued=false "
                f"det_classification={classification_result['det_classification']} "
                f"execution_permission={classification_result['execution_permission']}"
            )
            return "diagnostic_classified_no_execution"

        if not normalized["symbol"] or not normalized["side"]:
            logger.error(
                "INVALID SIGNAL PAYLOAD | "
                f"missing_canonical_fields symbol={normalized['symbol']} side={normalized['side']} "
                f"payload_format={normalized['payload_format']}"
            )
            return "invalid_signal_payload"

        policy = self.evaluate_stage_execution_policy(normalized)

        if normalized["payload_format"] == "enriched_candidate":
            assessment = self.assess_det_signal(normalized)
            classification_result = self.build_classification_result(normalized, assessment)
            self.log_det_result(normalized, assessment, classification_result)

            gate_result = self.evaluate_det_execution_gate(classification_result)
            logger.info(
                "DET EXECUTION GATE DECISION | "
                f"gate_branch={gate_result['gate_branch']} "
                f"det_classification={gate_result['det_classification']} "
                f"gate_enabled={gate_result['gate_enabled']} "
                f"gate_eligible={gate_result['gate_eligible']} "
                f"queue_allowed={gate_result['queue_allowed']} "
                f"reason={gate_result['reason']}"
            )

            if not policy["allow_queue"] or not gate_result["queue_allowed"]:
                logger.info(
                    "QUEUE DECISION | "
                    f"path={policy['policy_branch']} "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"det_classification={classification_result['det_classification']} "
                    f"queued=false "
                    f"reason={gate_result['reason']}"
                )
                return "enriched_candidate_classified_no_execution"

            entry_for_job = self.derive_entry_price_from_candidate(normalized)
            if entry_for_job is None:
                logger.info(
                    "QUEUE DECISION | "
                    f"path=future_det_gated_execution "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"det_classification={classification_result['det_classification']} "
                    f"queued=false "
                    f"reason=missing_price_reference"
                )
                return "enriched_candidate_missing_price"

            symbol = normalized["symbol"]
            side = normalized["side"]
            entry = entry_for_job
            execution_grade = classification_result["execution_grade"]
            reference_price = self.get_preferred_reference_price(normalized)

            # P049: Derive candidate executable entry WITH spread for non-trivial band validation
            candidate_executable_entry = self.derive_candidate_executable_entry(
                symbol, side, normalized
            )
            if candidate_executable_entry is None:
                logger.info(
                    "QUEUE DECISION | "
                    f"path=candidate_entry_derivation_failed "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"queued=false"
                )
                return "enriched_candidate_entry_derivation_failed"

            # P049: Validate entry is within grade-specific band from reference price
            # Uses non-trivial executable entry (with spread)
            band_valid, actual_drift, allowed_limit = self.validate_entry_band(
                symbol, execution_grade, reference_price, candidate_executable_entry
            )
            if not band_valid:
                logger.info(
                    "QUEUE DECISION | "
                    f"path=entry_band_validation_failed "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"grade={execution_grade} "
                    f"reference_price={reference_price} "
                    f"candidate_executable_entry={candidate_executable_entry} "
                    f"actual_drift={actual_drift} "
                    f"allowed_limit_price_distance={allowed_limit} "
                    f"queued=false"
                )
                return "enriched_candidate_entry_band_rejected"

            now = time.time()
            key = f"{symbol}-{side}-{round(entry, 8)}"

            if key == self.last_signal and now - self.last_signal_time < 5:
                logger.info("Duplicate ignored")
                return "duplicate_ignored"

            self.last_signal = key
            self.last_signal_time = now

            job = self.build_det_execution_job(normalized, classification_result)

            logger.info(
                "QUEUE PUT | "
                f"symbol={job['symbol']} "
                f"side={job['side']} "
                f"entry={job['entry']} "
                f"reference_price={job['reference_price']} "
                f"grade={job['grade']} "
                f"intended_risk_pct={job['intended_risk_percent']*100:.1f}% "
                f"det_classification={job['det_classification']} "
                f"payload_format={job['payload_format']} "
                f"signal_id={job['signal_id']}"
            )

            self.execution_queue.put(job)
            logger.info(
                "QUEUE DECISION | "
                f"path=future_det_gated_execution "
                f"payload_format={normalized['payload_format']} "
                f"signal_id={normalized['signal_id']} "
                f"symbol={normalized['symbol']} "
                f"side={normalized['side']} "
                f"det_classification={classification_result['det_classification']} "
                f"queued=true"
            )
            return "queued"

        if normalized["payload_format"] == "legacy_execution":
            logger.info(
                "LEGACY EXECUTION IGNORED | "
                f"payload_format={normalized['payload_format']} "
                f"signal_id={normalized['signal_id']} "
                f"symbol={normalized['symbol']} "
                f"side={normalized['side']} "
                "legacy_execution payloads are deprecated and denied for queueing"
            )
            return "legacy_execution_denied"

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

        if not policy["allow_queue"]:
            logger.info(
                "QUEUE DECISION | "
                f"path={policy['policy_branch']} "
                f"payload_format={normalized['payload_format']} "
                f"signal_id={normalized['signal_id']} "
                f"symbol={normalized['symbol']} "
                f"side={normalized['side']} "
                f"stage={policy['stage']} "
                f"queued=false "
                f"reason={policy['reason']}"
            )
            return "execution_denied_by_stage_policy"

        # Legacy execution queueing removed - legacy payloads are denied earlier

    # ==========================================================
    # WORKER & EXECUTION PREFLIGHT
    # ==========================================================

    def execution_preflight_check(self, job):
        symbol = job["symbol"]
        reasons = []

        self.update_session_health()

        if not self.session_socket_connected:
            reasons.append("socket_disconnected")
        if not self.session_initialized:
            reasons.append("session_not_initialized")
        if not self.session_healthy:
            reasons.append("session_not_healthy")
        if self.next_order_id is None:
            reasons.append("missing_next_order_id")
        if not self.events_attached:
            reasons.append("events_not_attached")

        if reasons:
            logger.warning(
                "EXECUTION PREFLIGHT INITIAL FAILURE | "
                f"symbol={symbol} reasons={','.join(reasons)}"
            )
            self.log_session_health("PREFLIGHT_BEFORE_RECOVERY")

            try:
                if self.ib.isConnected():
                    self.force_session_recovery("EXECUTION_PREFLIGHT")
                else:
                    self.connect_ib()
            except Exception:
                logger.exception("PREFLIGHT SESSION RECOVERY FAILED")
                self.update_session_health()

        self.update_session_health()
        reasons = []

        if not self.session_socket_connected:
            reasons.append("socket_disconnected")
        if not self.session_initialized:
            reasons.append("session_not_initialized")
        if not self.session_healthy:
            reasons.append("session_not_healthy")
        if self.next_order_id is None:
            reasons.append("missing_next_order_id")
        if not self.events_attached:
            reasons.append("events_not_attached")

        if not self.ensure_symbol_contract_ready(symbol):
            reasons.append(f"contract_not_ready:{symbol}")

        if reasons:
            logger.error(
                "EXECUTION PREFLIGHT DENIED | "
                f"symbol={symbol} reasons={','.join(reasons)}"
            )
            self.log_session_health("PREFLIGHT_DENIED")
            return False

        logger.info(f"EXECUTION PREFLIGHT OK | symbol={symbol}")
        self.log_session_health("EXECUTION_PREFLIGHT_OK")
        return True

    def execution_worker(self):
        asyncio.set_event_loop(asyncio.new_event_loop())

        logger.info("EXECUTION WORKER STARTED")
        logger.info(f"BOT STAGE: {BOT_STAGE}")

        self.connect_ib()

        while True:
            job = self.execution_queue.get()
            logger.info(f"WORKER RECEIVED JOB: {job}")

            try:
                with self.execution_lock:
                    has_lock, locked_trade_id, locked_symbol, locked_state = self.has_active_trade_locked()
                    if has_lock:
                        logger.warning(
                            "TRADE LOCK BLOCKED EXECUTION | "
                            f"locked_trade_id={locked_trade_id} "
                            f"locked_symbol={locked_symbol} "
                            f"locked_state={locked_state} "
                            f"incoming_symbol={job['symbol']}"
                        )
                        continue

                    logger.info("PRE-EXECUTION HEALTH CHECK")
                    if not self.execution_preflight_check(job):
                        logger.error("EXECUTION PREFLIGHT FAILED | SKIPPING JOB")
                        continue

                    logger.info(f"IB CONNECTED: {self.ib.isConnected()}")
                    logger.info("STARTING ORDER EXECUTION")

                    self.place_bracket_order(job)

                    logger.info("ORDER EXECUTION FINISHED")

            except Exception:
                logger.exception("Execution error")
            finally:
                self.execution_queue.task_done()

    # ==========================================================
    # EXECUTION QUALITY LOGGING
    # ==========================================================

    def place_bracket_order(self, job):
        symbol = job["symbol"]
        side = job["side"]

        spread_adjusted_entry = self.apply_spread(symbol, side, job["entry"])
        entry = self.round_to_tick(symbol, spread_adjusted_entry)

        contract = self.get_contract(symbol)

        # P050: Derive stop from FINAL executable entry (after spread/rounding)
        # Optionally uses structure anchors from normalized_signal for enriched stop derivation
        execution_grade = job.get("grade", "A")
        normalized_signal = job.get("normalized_signal")
        stop = self.derive_det_stop_price(symbol, entry, side, execution_grade, normalized_signal)
        stop = self.round_to_tick(symbol, stop)

        # P049: Derive 2R target from final entry and stop
        target = self.derive_target_from_r(entry, stop, side)
        target = self.round_to_tick(symbol, target)

        # P049: Compute final 1R distance from final executable entry and stop
        risk_distance_r = abs(entry - stop)

        if side == "long":
            parent_action = "BUY"
            child_action = "SELL"
        else:
            parent_action = "SELL"
            child_action = "BUY"

        logger.info(
            f"OFFICIAL BRACKET | {symbol} {side} "
            f"grade={job.get('grade', '')} "
            f"det_classification={job.get('det_classification', '')} "
            f"reference_price={job.get('reference_price', 'N/A')} "
            f"entry_reference_price={job.get('normalized_signal', {}).get('entry_reference_price', 'N/A')} "
            f"trigger_bar_high={job.get('normalized_signal', {}).get('trigger_bar_high', 'N/A')} "
            f"trigger_bar_low={job.get('normalized_signal', {}).get('trigger_bar_low', 'N/A')} "
            f"structure_anchor_low={job.get('normalized_signal', {}).get('structure_anchor_low', 'N/A')} "
            f"structure_anchor_high={job.get('normalized_signal', {}).get('structure_anchor_high', 'N/A')} "
            f"final_entry={entry} "
            f"final_stop={stop} "
            f"final_target={target} "
            f"risk_r={risk_distance_r} "
            f"intended_risk_pct={job.get('intended_risk_percent', 0)*100:.1f}%"
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

        submission_deadline = time.time() + 4.0
        confirmed = False

        parent_logged_transitions = set()
        tp_logged_transitions = set()
        sl_logged_transitions = set()

        while time.time() < submission_deadline:
            parent_status = getattr(parent_trade.orderStatus, "status", None)
            tp_status = getattr(tp_trade.orderStatus, "status", None)
            sl_status = getattr(sl_trade.orderStatus, "status", None)

            parent_perm = getattr(parent_trade.orderStatus, "permId", 0)
            tp_perm = getattr(tp_trade.orderStatus, "permId", 0)
            sl_perm = getattr(sl_trade.orderStatus, "permId", 0)

            # Log first transitions
            for leg, status, perm, order_id, logged_set in [
                ("parent", parent_status, parent_perm, parent_id, parent_logged_transitions),
                ("tp", tp_status, tp_perm, tp_id, tp_logged_transitions),
                ("sl", sl_status, sl_perm, sl_id, sl_logged_transitions),
            ]:
                if status == "PreSubmitted" and "PreSubmitted" not in logged_set:
                    logger.info(f"ORDER TRANSITION | leg={leg} orderId={order_id} status=PreSubmitted")
                    logged_set.add("PreSubmitted")
                if status == "Submitted" and "Submitted" not in logged_set:
                    logger.info(f"ORDER TRANSITION | leg={leg} orderId={order_id} status=Submitted")
                    logged_set.add("Submitted")
                if status == "Filled" and "Filled" not in logged_set:
                    logger.info(f"ORDER TRANSITION | leg={leg} orderId={order_id} status=Filled")
                    logged_set.add("Filled")
                if status in ("Cancelled", "Inactive", "ApiCancelled") and "cancelled" not in logged_set:
                    logger.info(f"ORDER TRANSITION | leg={leg} orderId={order_id} status={status}")
                    logged_set.add("cancelled")
                if perm != 0 and "permId_assigned" not in logged_set:
                    logger.info(f"ORDER TRANSITION | leg={leg} orderId={order_id} permId={perm} assigned")
                    logged_set.add("permId_assigned")

            parent_ok = parent_status != "PendingSubmit" or parent_perm != 0
            tp_ok = tp_status != "PendingSubmit" or tp_perm != 0
            sl_ok = sl_status != "PendingSubmit" or sl_perm != 0

            if parent_ok and tp_ok and sl_ok:
                confirmed = True
                break

            self.ib.sleep(0.10)

        self.log_trade_snapshot("POST CONFIRM PARENT", parent_trade)
        self.log_trade_snapshot("POST CONFIRM TP", tp_trade)
        self.log_trade_snapshot("POST CONFIRM SL", sl_trade)

        logger.info(
            f"BRACKET CONFIRMATION ASSESSMENT | confirmed={confirmed} | "
            f"parent: orderId={parent_id} status={parent_status} permId={parent_perm} | "
            f"tp: orderId={tp_id} status={tp_status} permId={tp_perm} | "
            f"sl: orderId={sl_id} status={sl_status} permId={sl_perm} | "
            f"reason={'all legs confirmed (status != PendingSubmit or permId != 0)' if confirmed else 'confirmation timeout or legs stuck in PendingSubmit with permId=0'}"
        )

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
            self.trade_analysis[trade_id]["state"] = "INCOMPLETE"
            self.finalize_trade_if_complete(trade_id)
            logger.error(
                f"BRACKET NOT CONFIRMED | trade_id={trade_id} "
                f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id}"
            )
            return

        self.append_trade_event(trade_id, "BRACKET SUBMITTED")
        logger.info("BRACKET SUBMITTED")

    # ==========================================================
    # WATCHDOG
    # ==========================================================

    def ib_watchdog(self):
        asyncio.set_event_loop(asyncio.new_event_loop())

        while True:
            try:
                self.update_session_health()

                if not self.ib.isConnected():
                    self.session_reconnect_count += 1
                    self.last_reconnect_time = datetime.now(timezone.utc)
                    logger.warning(
                        f"RECONNECT ATTEMPT #{self.session_reconnect_count} | "
                        f"socket_connected=false"
                    )
                    self.log_session_health(f"RECONNECT_ATTEMPT_{self.session_reconnect_count}")

                    try:
                        self.connect_ib()
                        logger.info(f"RECONNECT SUCCESS #{self.session_reconnect_count}")
                        self.log_session_health(f"RECONNECT_SUCCESS_{self.session_reconnect_count}")
                    except Exception:
                        logger.exception(f"RECONNECT FAILED #{self.session_reconnect_count}")
                        self.log_session_health(f"RECONNECT_FAILED_{self.session_reconnect_count}")
                else:
                    if not self.session_healthy:
                        logger.warning("FORCED SESSION RECOVERY (SOCKET ALIVE BUT SESSION UNHEALTHY)")
                        self.log_session_health("WATCHDOG_UNHEALTHY_BEFORE_RECOVERY")
                        self.force_session_recovery("WATCHDOG_SOCKET_ALIVE_SESSION_UNHEALTHY")
            except Exception:
                logger.exception("WATCHDOG ERROR")

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