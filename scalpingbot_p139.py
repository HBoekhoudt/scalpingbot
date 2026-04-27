# ==========================================================
# IKBR SCALPING BOT | VERSION v1.6.0 P139 | STAGE: TEST
# ==========================================================

import logging
import asyncio
import threading
import queue
import time
import decimal
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, HTTPException
from ib_insync import IB, Future, Forex, LimitOrder, StopOrder

# ==========================================================
# VERSIONING
# ==========================================================

BOT_NAME = "IKBR_SCALPING_BOT"
BOT_VERSION = "v1.6.0"
BOT_PATCH = "P139"
BOT_STAGE = "TEST"  # TEST | PAPER | LIVE

logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")

# ==========================================================
# CONFIG
# ==========================================================

INSTRUMENT_SPECS = {
    "MNQ": {
        "symbol": "MNQ",
        "broker_type": "future",
        "exchange": "CME",
        "currency": "USD",
        "trading_class": "MNQ",
        "tick_size": 0.25,
        "price_decimals": 2,
        "point_value": 2.0,
        "tick_value": 0.5,
        "spread_assumption": 0.25,
        "fallback_stop_distance": 2.00,
        "min_stop_distance": 2.00,
        "entry_band_a": 3.00,
        "entry_band_a_plus": 2.00,
        "min_size": 1,
        "size_step": 1,
        "max_size": None,
        "session_profile": "US_INDEX",
    },
    "MES": {
        "symbol": "MES",
        "broker_type": "future",
        "exchange": "CME",
        "currency": "USD",
        "trading_class": "MES",
        "tick_size": 0.25,
        "price_decimals": 2,
        "point_value": 5.0,
        "tick_value": 1.25,
        "spread_assumption": 0.25,
        "fallback_stop_distance": 2.00,
        "min_stop_distance": 2.00,
        "entry_band_a": 2.00,
        "entry_band_a_plus": 1.00,
        "min_size": 1,
        "size_step": 1,
        "max_size": None,
        "session_profile": "US_INDEX",
    },
    "FDXM": {
        "symbol": "FDXM",
        "broker_type": "future",
        "exchange": "EUREX",
        "currency": "EUR",
        "trading_class": "FDXM",
        "tick_size": 0.5,
        "price_decimals": 1,
        "point_value": 5.0,
        "tick_value": 2.5,
        "spread_assumption": 0.5,
        "fallback_stop_distance": 5.00,
        "min_stop_distance": 5.0,
        "entry_band_a": 2.00,
        "entry_band_a_plus": 1.00,
        "min_size": 1,
        "size_step": 1,
        "max_size": 10,
        "session_profile": "EU_INDEX",
    },
    "M6E": {
        "symbol": "M6E",
        "broker_type": "future",
        "exchange": "CME",
        "currency": "USD",
        "trading_class": "M6E",
        "tick_size": 0.00005,
        "price_decimals": 5,
        "point_value": 12500.0,
        "tick_value": 6.25,
        "spread_assumption": 0.00005,
        "fallback_stop_distance": 0.00020,
        "min_stop_distance": 0.00020,
        "entry_band_a": 0.00040,
        "entry_band_a_plus": 0.00020,
        "min_size": 1,
        "size_step": 1,
        "max_size": None,
        "session_profile": "FX",
    },
    "EURUSD": {
        "symbol": "EURUSD",
        "broker_type": "forex",
        "exchange": "IDEALPRO",
        "currency": "USD",
        "trading_class": None,
        "tick_size": 0.00005,
        "price_decimals": 5,
        "point_value": None,
        "tick_value": None,
        "spread_assumption": 0.00005,
        "fallback_stop_distance": 0.00020,
        "min_stop_distance": 0.00020,
        "entry_band_a": 0.00040,
        "entry_band_a_plus": 0.00020,
        "min_size": None,
        "size_step": None,
        "max_size": None,
        "session_profile": "FX",
    },
}

QUALIFIED_FUTURE_SYMBOLS = ("MNQ", "MES", "M6E", "FDXM")

EXECUTION_CAPITAL_BASE = 170000.0

BRACKET_CONFIRM_STATUSES = {
    "PreSubmitted",
    "Submitted",
    "Filled",
}

BRACKET_PENDING_ACK_STATUSES = {
    None,
    "",
    "PendingSubmit",
    "ApiPending",
    "PreSubmitted",
}

BRACKET_LIVE_ACK_STATUSES = {
    "Submitted",
    "Filled",
}

BROKER_ACK_PENDING_TIMEOUT_SECONDS = 30.0
PARTIAL_ENTRY_TIMEOUT_SECONDS = 10.0

SHADOW_TEST_PROHIBITED_REASONS = {
    "chop",
    "late",
    "poor_structure",
    "conflicting_bias",
    "session_invalid",
}

ACTIVE_TRADE_STATES = {
    "SUBMITTING",
    "BROKER_ACK_PENDING",
    "ENTRY_WORKING",
    "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL",
    "ENTRY_FILLED",
    "EXIT_WORKING",
    "TP_FILLED",
    "SL_FILLED",
}

OPEN_BROKER_ORDER_STATUSES = {
    None,
    "",
    "PendingSubmit",
    "ApiPending",
    "PreSubmitted",
    "Submitted",
    "PendingCancel",
}

RECOGNIZED_STRATEGY_FAMILIES = {
    "DET",
    "ORB",
    "VWAP_PULLBACK",
}

RUNTIME_ACTIVE_EXECUTION_STRATEGY_FAMILIES = {
    "DET",
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
        self.trade_analysis_lock = threading.RLock()

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

        self.live_daily_stop_day_key = None
        self.live_daily_stop_active = False
        self.live_daily_stop_trigger_trade_id = None

        self.trade_analysis = {}
        self.order_to_trade = {}
        self.completed_trade_ids = set()
        self.trade_seq = 0
        self.processed_execution_ids = set()
        self.processed_commission_ids = set()
        self.aggregate_stats = {
            "total_trades": 0,
            "closed_trades": 0,
            "filled_trades": 0,
            "tp_count": 0,
            "sl_count": 0,
            "mixed_exit_count": 0,
            "cancelled_count": 0,
            "rejected_count": 0,
            "incomplete_count": 0,
            "diagnostic_reject_count": 0,
            "diagnostic_shadow_count": 0,
            "diagnostic_execute_a_count": 0,
            "diagnostic_execute_a_plus_count": 0,
            "execution_reject_count": 0,
            "execution_shadow_count": 0,
            "execution_execute_a_count": 0,
            "execution_execute_a_plus_count": 0,
            "queued_count": 0,
            "shadow_test_queued_count": 0,
            "submitted_count": 0,
            "shadow_test_submitted_count": 0,
            "broker_acknowledged_count": 0,
            "broker_live_count": 0,
            "shadow_test_entry_filled_count": 0,
            "shadow_test_tp_count": 0,
            "shadow_test_sl_count": 0,
            "shadow_test_incomplete_count": 0,
            "gross_pnl": 0.0,
            "commission": 0.0,
            "net_pnl": 0.0,
            "shadow_test_net_pnl": 0.0,
        }
        self.symbol_stats = {
            symbol: {
                "total_trades": 0,
                "filled_trades": 0,
                "tp_count": 0,
                "sl_count": 0,
                "gross_pnl": 0.0,
                "commission": 0.0,
                "net_pnl": 0.0,
            }
            for symbol in INSTRUMENT_SPECS.keys()
        }

        threading.Thread(target=self.execution_worker, daemon=True).start()
        threading.Thread(target=self.ib_watchdog, daemon=True).start()

    # ==========================================================
    # PRICE LOGIC
    # ==========================================================

    def get_instrument_spec(self, symbol):
        spec = INSTRUMENT_SPECS.get(symbol)
        if spec is None:
            raise ValueError(f"Unsupported symbol: {symbol}")
        return spec

    def get_tick_size(self, symbol):
        spec = self.get_instrument_spec(symbol)
        return self.contract_min_ticks.get(symbol, spec["tick_size"])

    def get_spread_assumption(self, symbol):
        return self.get_instrument_spec(symbol)["spread_assumption"]

    def get_point_value(self, symbol):
        spec = self.get_instrument_spec(symbol)
        return spec["point_value"] if spec["point_value"] is not None else 1.0

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
        spread = self.get_spread_assumption(symbol)

        if side == "long":
            price += spread
        else:
            price -= spread

        logger.info(f"SPREAD | {symbol} {side} → {price}")
        return price

    def derive_executable_entry_plan(self, symbol, side, reference_price):
        if not symbol or side not in {"long", "short"} or reference_price is None:
            return None

        spread_adjusted_entry = self.apply_spread(symbol, side, reference_price)
        final_entry = self.round_to_tick(symbol, spread_adjusted_entry)

        return {
            "reference_price": reference_price,
            "spread_adjusted_entry": spread_adjusted_entry,
            "final_entry": final_entry,
        }

    def derive_executable_entry_plan_from_normalized(self, normalized):
        return self.derive_executable_entry_plan(
            normalized.get("symbol"),
            normalized.get("side"),
            self.get_preferred_reference_price(normalized),
        )

    def derive_executable_entry_plan_from_job(self, job):
        return self.derive_executable_entry_plan(
            job.get("symbol"),
            job.get("side"),
            job.get("reference_price"),
        )

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

    def get_size_constraints(self, symbol):
        spec = self.get_instrument_spec(symbol)
        return {
            "min_size": spec["min_size"],
            "size_step": spec["size_step"],
            "max_size": spec["max_size"],
            "broker_type": spec["broker_type"],
        }

    def calculate_allowed_money_risk(self, execution_grade):
        intended_risk_percent = self.get_grade_risk_percent(execution_grade)
        if intended_risk_percent <= 0:
            return None, intended_risk_percent
        return EXECUTION_CAPITAL_BASE * intended_risk_percent, intended_risk_percent

    def calculate_stop_distance_points(self, entry_price, stop_price):
        try:
            if entry_price is None or stop_price is None:
                return None
            stop_distance_points = abs(float(entry_price) - float(stop_price))
            if stop_distance_points <= 0:
                return None
            return stop_distance_points
        except Exception:
            return None

    def calculate_raw_position_size(self, symbol, allowed_money_risk, stop_distance_points):
        spec = self.get_instrument_spec(symbol)

        if spec["broker_type"] != "future":
            return None, None, "unsupported_instrument_sizing_model"

        point_value = spec["point_value"]
        if point_value is None or point_value <= 0:
            return None, point_value, "missing_point_value"

        if allowed_money_risk is None or allowed_money_risk <= 0:
            return None, point_value, "invalid_allowed_money_risk"

        if stop_distance_points is None or stop_distance_points <= 0:
            return None, point_value, "invalid_stop_distance"

        risk_per_contract = stop_distance_points * point_value
        if risk_per_contract <= 0:
            return None, point_value, "invalid_risk_per_contract"

        raw_size = allowed_money_risk / risk_per_contract
        if raw_size <= 0:
            return None, point_value, "non_positive_raw_size"

        return raw_size, point_value, None

    def normalize_position_size(self, symbol, raw_size):
        constraints = self.get_size_constraints(symbol)
        min_size = constraints["min_size"]
        size_step = constraints["size_step"]
        max_size = constraints["max_size"]

        if raw_size is None or raw_size <= 0:
            return None
        if min_size is None or min_size <= 0:
            return None
        if size_step is None or size_step <= 0:
            return None

        raw_decimal = decimal.Decimal(str(raw_size))
        step_decimal = decimal.Decimal(str(size_step))

        if max_size is not None:
            raw_decimal = min(raw_decimal, decimal.Decimal(str(max_size)))

        normalized_decimal = (
            raw_decimal / step_decimal
        ).to_integral_value(rounding=decimal.ROUND_DOWN) * step_decimal

        if normalized_decimal <= 0:
            return None

        if normalized_decimal == normalized_decimal.to_integral_value():
            return int(normalized_decimal)
        return float(normalized_decimal)

    def validate_position_size(self, symbol, size):
        constraints = self.get_size_constraints(symbol)
        min_size = constraints["min_size"]
        size_step = constraints["size_step"]
        max_size = constraints["max_size"]

        if size is None:
            return False, "size_missing"
        if size <= 0:
            return False, "size_non_positive"
        if min_size is None or size < min_size:
            return False, "size_below_minimum"
        if max_size is not None and size > max_size:
            return False, "size_above_maximum"
        if size_step is None or size_step <= 0:
            return False, "invalid_size_step"

        size_decimal = decimal.Decimal(str(size))
        step_decimal = decimal.Decimal(str(size_step))
        min_decimal = decimal.Decimal(str(min_size))
        remainder = (size_decimal - min_decimal) % step_decimal
        if remainder != 0:
            return False, "size_step_misaligned"

        return True, "size_valid"

    def calculate_execution_position_size(self, symbol, execution_grade, entry_price, stop_price):
        allowed_money_risk, intended_risk_percent = self.calculate_allowed_money_risk(execution_grade)
        stop_distance_points = self.calculate_stop_distance_points(entry_price, stop_price)

        raw_size = None
        normalized_size = None
        point_value = None
        risk_per_contract = None
        validation_reason = None

        if not execution_grade:
            return {
                "ok": False,
                "reason": "missing_execution_grade",
                "capital_base": EXECUTION_CAPITAL_BASE,
                "intended_risk_percent": intended_risk_percent,
                "allowed_money_risk": allowed_money_risk,
                "stop_distance_points": stop_distance_points,
                "point_value": point_value,
                "risk_per_contract": risk_per_contract,
                "raw_position_size": raw_size,
                "normalized_position_size": normalized_size,
                "validation_reason": validation_reason,
            }

        if intended_risk_percent <= 0:
            return {
                "ok": False,
                "reason": "invalid_intended_risk_percent",
                "capital_base": EXECUTION_CAPITAL_BASE,
                "intended_risk_percent": intended_risk_percent,
                "allowed_money_risk": allowed_money_risk,
                "stop_distance_points": stop_distance_points,
                "point_value": point_value,
                "risk_per_contract": risk_per_contract,
                "raw_position_size": raw_size,
                "normalized_position_size": normalized_size,
                "validation_reason": validation_reason,
            }

        if stop_distance_points is None:
            return {
                "ok": False,
                "reason": "invalid_stop_distance_points",
                "capital_base": EXECUTION_CAPITAL_BASE,
                "intended_risk_percent": intended_risk_percent,
                "allowed_money_risk": allowed_money_risk,
                "stop_distance_points": stop_distance_points,
                "point_value": point_value,
                "risk_per_contract": risk_per_contract,
                "raw_position_size": raw_size,
                "normalized_position_size": normalized_size,
                "validation_reason": validation_reason,
            }

        raw_size, point_value, raw_reason = self.calculate_raw_position_size(
            symbol, allowed_money_risk, stop_distance_points
        )
        if raw_reason is not None:
            return {
                "ok": False,
                "reason": raw_reason,
                "capital_base": EXECUTION_CAPITAL_BASE,
                "intended_risk_percent": intended_risk_percent,
                "allowed_money_risk": allowed_money_risk,
                "stop_distance_points": stop_distance_points,
                "point_value": point_value,
                "risk_per_contract": None,
                "raw_position_size": raw_size,
                "normalized_position_size": normalized_size,
                "validation_reason": validation_reason,
            }

        risk_per_contract = stop_distance_points * point_value
        normalized_size = self.normalize_position_size(symbol, raw_size)
        size_valid, validation_reason = self.validate_position_size(symbol, normalized_size)

        if not size_valid:
            return {
                "ok": False,
                "reason": validation_reason,
                "capital_base": EXECUTION_CAPITAL_BASE,
                "intended_risk_percent": intended_risk_percent,
                "allowed_money_risk": allowed_money_risk,
                "stop_distance_points": stop_distance_points,
                "point_value": point_value,
                "risk_per_contract": risk_per_contract,
                "raw_position_size": raw_size,
                "normalized_position_size": normalized_size,
                "validation_reason": validation_reason,
            }

        return {
            "ok": True,
            "reason": "size_valid",
            "capital_base": EXECUTION_CAPITAL_BASE,
            "intended_risk_percent": intended_risk_percent,
            "allowed_money_risk": allowed_money_risk,
            "stop_distance_points": stop_distance_points,
            "point_value": point_value,
            "risk_per_contract": risk_per_contract,
            "raw_position_size": raw_size,
            "normalized_position_size": normalized_size,
            "validation_reason": validation_reason,
        }

    def get_fallback_stop_distance(self, symbol, execution_grade=None):
        """Return instrument-aware fallback stop distance in price units (not ticks).
        Used as temporary pragmatic model; ready for later structure-anchor integration.
        """
        return self.get_instrument_spec(symbol)["fallback_stop_distance"]

    def get_min_stop_distance(self, symbol):
        return self.get_instrument_spec(symbol).get("min_stop_distance")

    def get_entry_band_limit(self, symbol, execution_grade):
        """Return max entry drift in price distance units for grade and symbol.
        All values return consistent price-distance units (not mixed ticks/pips).
        """
        spec = self.get_instrument_spec(symbol)
        if execution_grade == "A+":
            return spec["entry_band_a_plus"]
        return spec["entry_band_a"]

    def derive_candidate_executable_entry(self, symbol, side, normalized):
        """Compatibility wrapper for canonical executable entry derivation."""
        plan = self.derive_executable_entry_plan_from_normalized(normalized)
        if plan is None:
            return None
        return plan["final_entry"]

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

    def derive_det_stop_plan(self, symbol, entry_price, side, execution_grade=None, normalized_signal=None):
        """Derive stop plan from final executable entry price.
        Keeps the existing anchor-selection semantics and exposes the source for logging.
        """
        fallback_stop_dist = self.get_fallback_stop_distance(symbol, execution_grade)
        fallback_stop = (
            entry_price - fallback_stop_dist
            if side == "long"
            else entry_price + fallback_stop_dist
        )

        stop_plan = {
            "stop_price": fallback_stop,
            "stop_source": "fallback_stop_distance",
            "selected_stop_source": "fallback_stop_distance",
            "anchor_based": False,
        }

        if normalized_signal is not None:
            if side == "long":
                structure_anchor_low = normalized_signal.get("structure_anchor_low")
                trigger_bar_low = normalized_signal.get("trigger_bar_low")

                candidates = []
                if structure_anchor_low is not None and structure_anchor_low < entry_price:
                    candidates.append(("structure_anchor_low", structure_anchor_low))
                if trigger_bar_low is not None and trigger_bar_low < entry_price:
                    candidates.append(("trigger_bar_low", trigger_bar_low))

                if candidates:
                    selected_source, anchor_stop = min(candidates, key=lambda item: item[1])
                    risk_dist = entry_price - anchor_stop
                    if risk_dist > 0:
                        stop_plan["stop_price"] = anchor_stop
                        stop_plan["stop_source"] = "anchor_trigger"
                        stop_plan["selected_stop_source"] = selected_source
                        stop_plan["anchor_based"] = True
                        return stop_plan
            elif side == "short":
                structure_anchor_high = normalized_signal.get("structure_anchor_high")
                trigger_bar_high = normalized_signal.get("trigger_bar_high")

                candidates = []
                if structure_anchor_high is not None and structure_anchor_high > entry_price:
                    candidates.append(("structure_anchor_high", structure_anchor_high))
                if trigger_bar_high is not None and trigger_bar_high > entry_price:
                    candidates.append(("trigger_bar_high", trigger_bar_high))

                if candidates:
                    selected_source, anchor_stop = max(candidates, key=lambda item: item[1])
                    risk_dist = anchor_stop - entry_price
                    if risk_dist > 0:
                        stop_plan["stop_price"] = anchor_stop
                        stop_plan["stop_source"] = "anchor_trigger"
                        stop_plan["selected_stop_source"] = selected_source
                        stop_plan["anchor_based"] = True
                        return stop_plan

        return stop_plan

    def derive_det_stop_price(self, symbol, entry_price, side, execution_grade=None, normalized_signal=None):
        return self.derive_det_stop_plan(
            symbol,
            entry_price,
            side,
            execution_grade,
            normalized_signal,
        )["stop_price"]

    def validate_final_stop_distance(self, symbol, entry_price, stop_price):
        final_stop_distance = self.calculate_stop_distance_points(entry_price, stop_price)
        min_stop_distance = self.get_min_stop_distance(symbol)

        if min_stop_distance is None:
            return {
                "ok": True,
                "final_stop_distance": final_stop_distance,
                "min_stop_distance": min_stop_distance,
                "reason": "no_min_stop_distance_configured",
            }

        if final_stop_distance is None:
            return {
                "ok": False,
                "final_stop_distance": final_stop_distance,
                "min_stop_distance": min_stop_distance,
                "reason": "invalid_final_stop_distance",
            }

        if final_stop_distance < min_stop_distance:
            return {
                "ok": False,
                "final_stop_distance": final_stop_distance,
                "min_stop_distance": min_stop_distance,
                "reason": "final_stop_distance_below_min_stop_distance",
            }

        return {
            "ok": True,
            "final_stop_distance": final_stop_distance,
            "min_stop_distance": min_stop_distance,
            "reason": "final_stop_distance_valid",
        }

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

    def amsterdam_now(self):
        return datetime.now(AMSTERDAM_TZ)

    def get_amsterdam_day_key(self, dt_value=None):
        if dt_value is None:
            dt_value = self.amsterdam_now()
        elif isinstance(dt_value, datetime):
            if dt_value.tzinfo is None:
                dt_value = dt_value.replace(tzinfo=timezone.utc)
            dt_value = dt_value.astimezone(AMSTERDAM_TZ)
        else:
            dt_value = self.amsterdam_now()

        return dt_value.strftime("%Y-%m-%d")

    def refresh_live_daily_stop_state(self, reason_label, reference_time=None):
        day_key = self.get_amsterdam_day_key(reference_time)

        if self.live_daily_stop_day_key is None:
            self.live_daily_stop_day_key = day_key
            return day_key

        if self.live_daily_stop_day_key != day_key:
            logger.info(
                "LIVE DAILY SL STOP RESET | "
                f"old_day_key={self.live_daily_stop_day_key} "
                f"new_day_key={day_key} "
                f"previously_active={self.live_daily_stop_active} "
                f"trigger_trade_id={self.live_daily_stop_trigger_trade_id} "
                f"reason={reason_label}"
            )
            self.live_daily_stop_day_key = day_key
            self.live_daily_stop_active = False
            self.live_daily_stop_trigger_trade_id = None

        return day_key

    def activate_live_daily_sl_stop(self, trade_id, exit_time=None):
        day_key = self.refresh_live_daily_stop_state("LIVE_SL_TRIGGER", exit_time)

        if not self.live_daily_stop_active:
            self.live_daily_stop_active = True
            self.live_daily_stop_trigger_trade_id = trade_id
            logger.warning(
                "LIVE DAILY SL STOP ACTIVATED | "
                f"day_key={day_key} "
                f"trigger_trade_id={trade_id}"
            )
        else:
            logger.warning(
                "LIVE DAILY SL STOP ALREADY ACTIVE | "
                f"day_key={day_key} "
                f"existing_trigger_trade_id={self.live_daily_stop_trigger_trade_id} "
                f"incoming_trade_id={trade_id}"
            )

    def evaluate_live_execution_risk_regime(self, job):
        stage = BOT_STAGE

        if stage != "LIVE":
            return {
                "stage": stage,
                "risk_branch": "non_live_regime",
                "execution_allowed": True,
                "reason": "live_daily_sl_stop_not_applicable",
            }

        day_key = self.refresh_live_daily_stop_state("LIVE_EXECUTION_GATE")

        if self.live_daily_stop_active:
            reason = (
                "LIVE execution denied because a realized SL already activated the daily stop "
                f"for Amsterdam day {day_key}"
            )
            logger.warning(
                "LIVE DAILY SL STOP DENIAL | "
                f"day_key={day_key} "
                f"trigger_trade_id={self.live_daily_stop_trigger_trade_id} "
                f"incoming_symbol={job['symbol']} "
                f"reason={reason}"
            )
            return {
                "stage": stage,
                "risk_branch": "live_daily_sl_stop_active",
                "execution_allowed": False,
                "day_key": day_key,
                "trigger_trade_id": self.live_daily_stop_trigger_trade_id,
                "reason": reason,
            }

        logger.info(
            "LIVE RISK REGIME | "
            f"stage={stage} "
            f"day_key={day_key} "
            "execution_allowed=true "
            "reason=no_realized_sl_day_stop"
        )
        return {
            "stage": stage,
            "risk_branch": "live_daily_sl_stop_clear",
            "execution_allowed": True,
            "day_key": day_key,
            "trigger_trade_id": self.live_daily_stop_trigger_trade_id,
            "reason": "no_realized_sl_day_stop",
        }

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
        multiplier = self.get_point_value(record["symbol"])
        tp_exit_quantity = float(record.get("tp_exit_quantity") or 0.0)
        sl_exit_quantity = float(record.get("sl_exit_quantity") or 0.0)

        expected_gross_pnl = 0.0
        if tp_exit_quantity > 0:
            expected_gross_pnl += abs(record["target_price"] - record["entry_price"]) * multiplier * tp_exit_quantity
        if sl_exit_quantity > 0:
            expected_gross_pnl -= abs(record["stop_price"] - record["entry_price"]) * multiplier * sl_exit_quantity

        return round(expected_gross_pnl, 2)

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

    def get_execution_identity(self, execution, fill_time=None):
        exec_id = getattr(execution, "execId", None)
        if exec_id:
            return f"execId:{exec_id}"

        return (
            f"fallback:"
            f"{getattr(execution, 'orderId', None)}|"
            f"{getattr(execution, 'side', None)}|"
            f"{getattr(execution, 'shares', None)}|"
            f"{getattr(execution, 'price', None)}|"
            f"{fill_time}"
        )

    def should_process_execution(self, execution, fill_time=None):
        execution_identity = self.get_execution_identity(execution, fill_time)

        with self.trade_analysis_lock:
            if execution_identity in self.processed_execution_ids:
                return False, execution_identity
            self.processed_execution_ids.add(execution_identity)

        return True, execution_identity

    def get_commission_identity(self, order_id, report):
        exec_id = getattr(report, "execId", None)
        if exec_id:
            return f"commission_execId:{exec_id}"

        return (
            f"commission_fallback:"
            f"{order_id}|"
            f"{getattr(report, 'commission', None)}|"
            f"{getattr(report, 'currency', None)}|"
            f"{getattr(report, 'realizedPNL', None)}"
        )

    def should_process_commission(self, order_id, report):
        commission_identity = self.get_commission_identity(order_id, report)

        with self.trade_analysis_lock:
            if commission_identity in self.processed_commission_ids:
                return False, commission_identity
            self.processed_commission_ids.add(commission_identity)

        return True, commission_identity

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
                record.get("tp_exit_fill_price"),
                record["target_price"],
                "target"
            )
            exit_slippage = target_slippage
        elif record["exit_reason"] == "SL":
            stop_slippage = self.calculate_price_slippage(
                record,
                record.get("sl_exit_fill_price"),
                record["stop_price"],
                "stop"
            )
            exit_slippage = stop_slippage
        elif record["exit_reason"] == "MIXED_EXIT":
            target_slippage = self.calculate_price_slippage(
                record,
                record.get("tp_exit_fill_price"),
                record["target_price"],
                "target"
            )
            stop_slippage = self.calculate_price_slippage(
                record,
                record.get("sl_exit_fill_price"),
                record["stop_price"],
                "stop"
            )
            tp_exit_quantity = float(record.get("tp_exit_quantity") or 0.0)
            sl_exit_quantity = float(record.get("sl_exit_quantity") or 0.0)
            total_exit_quantity = tp_exit_quantity + sl_exit_quantity
            if total_exit_quantity > 0:
                weighted_exit_slippage = 0.0
                if target_slippage is not None and tp_exit_quantity > 0:
                    weighted_exit_slippage += target_slippage * tp_exit_quantity
                if stop_slippage is not None and sl_exit_quantity > 0:
                    weighted_exit_slippage += stop_slippage * sl_exit_quantity
                exit_slippage = round(weighted_exit_slippage / total_exit_quantity, 10)

        expected_gross_pnl = self.calculate_expected_gross_pnl(record)
        realized_vs_expected_gross = None

        if expected_gross_pnl is not None:
            realized_vs_expected_gross = round(record["gross_pnl"] - expected_gross_pnl, 2)

        return {
            "entry_slippage": entry_slippage,
            "target_slippage": target_slippage,
            "stop_slippage": stop_slippage,
            "exit_slippage": exit_slippage,
            "expected_gross_pnl": expected_gross_pnl,
            "realized_vs_expected_gross": realized_vs_expected_gross,
        }

    def get_symbol_stats_bucket(self, symbol):
        if symbol not in self.symbol_stats:
            self.symbol_stats[symbol] = {
                "total_trades": 0,
                "filled_trades": 0,
                "tp_count": 0,
                "sl_count": 0,
                "gross_pnl": 0.0,
                "commission": 0.0,
                "net_pnl": 0.0,
            }
        return self.symbol_stats[symbol]

    def increment_classification_stats(self, payload_format, classification_result):
        det_classification = classification_result.get("det_classification")
        if payload_format == "diagnostic":
            prefix = "diagnostic"
        elif payload_format == "enriched_candidate":
            prefix = "execution"
        else:
            return

        if det_classification == "REJECT":
            self.aggregate_stats[f"{prefix}_reject_count"] += 1
        elif det_classification == "SHADOW":
            self.aggregate_stats[f"{prefix}_shadow_count"] += 1
        elif det_classification == "EXECUTE_A":
            self.aggregate_stats[f"{prefix}_execute_a_count"] += 1
        elif det_classification == "EXECUTE_A_PLUS":
            self.aggregate_stats[f"{prefix}_execute_a_plus_count"] += 1

    def set_trade_timestamp(self, trade_id, field_name, value=None):
        if value is None:
            value = datetime.now(timezone.utc)
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return None
            record[field_name] = value
            return value

    def increment_trade_counter_once(self, trade_id, flag_field, aggregate_field, symbol_field=None):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None or record.get(flag_field):
                return False

            record[flag_field] = True
            self.aggregate_stats[aggregate_field] = self.aggregate_stats.get(aggregate_field, 0) + 1

            if self.is_shadow_test_trade(record):
                shadow_test_counter_map = {
                    "submitted_count": "shadow_test_submitted_count",
                }
                shadow_counter = shadow_test_counter_map.get(aggregate_field)
                if shadow_counter:
                    self.aggregate_stats[shadow_counter] = self.aggregate_stats.get(shadow_counter, 0) + 1

            if symbol_field:
                symbol_bucket = self.get_symbol_stats_bucket(record["symbol"])
                symbol_bucket[symbol_field] = symbol_bucket.get(symbol_field, 0) + 1

            return True

    def is_shadow_test_trade(self, item):
        return bool(item and item.get("promoted_from_shadow") and item.get("execution_lane") == "shadow_test")

    def adjust_shadow_test_net_pnl(self, delta):
        if not delta:
            return
        self.aggregate_stats["shadow_test_net_pnl"] = round(
            self.aggregate_stats.get("shadow_test_net_pnl", 0.0) + delta,
            2
        )

    def build_trade_timing_metrics(self, record):
        return {
            "webhook_to_classification_sec": self.seconds_between(
                record.get("webhook_received_time"),
                record.get("classification_completed_time"),
            ),
            "classification_to_queue_sec": self.seconds_between(
                record.get("classification_completed_time"),
                record.get("queue_put_time"),
            ),
            "queue_wait_sec": self.seconds_between(
                record.get("queue_put_time"),
                record.get("worker_pickup_time"),
            ),
            "pickup_to_preflight_sec": self.seconds_between(
                record.get("worker_pickup_time"),
                record.get("preflight_completed_time"),
            ),
            "preflight_to_submit_sec": self.seconds_between(
                record.get("preflight_completed_time"),
                record.get("bracket_submit_start_time"),
            ),
            "submit_to_broker_ack_sec": self.seconds_between(
                record.get("bracket_submit_start_time"),
                record.get("broker_acknowledged_at") or record.get("broker_live_at"),
            ),
            "submit_to_entry_fill_sec": self.seconds_between(
                record.get("bracket_submit_start_time"),
                record.get("entry_fill_time"),
            ),
            "entry_to_exit_sec": self.seconds_between(
                record.get("entry_fill_time"),
                record.get("exit_fill_time"),
            ),
            "total_trade_lifecycle_sec": self.seconds_between(
                record.get("webhook_received_time"),
                record.get("exit_fill_time"),
            ) or self.seconds_between(
                record.get("webhook_received_time"),
                record.get("entry_fill_time"),
            ) or self.seconds_between(
                record.get("webhook_received_time"),
                record.get("bracket_submit_end_time"),
            ),
        }

    def build_symbol_stats_snapshot(self):
        snapshot = {}
        for symbol, stats in self.symbol_stats.items():
            if (
                stats["total_trades"] > 0 or
                stats["filled_trades"] > 0 or
                stats["tp_count"] > 0 or
                stats["sl_count"] > 0 or
                stats["gross_pnl"] != 0.0 or
                stats["commission"] != 0.0 or
                stats["net_pnl"] != 0.0
            ):
                snapshot[symbol] = {
                    "total": stats["total_trades"],
                    "filled": stats["filled_trades"],
                    "tp": stats["tp_count"],
                    "sl": stats["sl_count"],
                    "gross": round(stats["gross_pnl"], 2),
                    "commission": round(stats["commission"], 2),
                    "net": round(stats["net_pnl"], 2),
                }
        return snapshot

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
            "bot_stage": job.get("bot_stage", BOT_STAGE),
            "state": "SUBMITTING",
            "symbol": job["symbol"],
            "side": job["side"],
            "grade": job.get("grade", ""),
            "truth_classification": job.get("truth_classification", ""),
            "det_classification": job.get("det_classification", ""),
            "primary_reason": job.get("primary_reason", ""),
            "execution_lane": job.get("execution_lane", "standard"),
            "promoted_from_shadow": bool(job.get("promoted_from_shadow", False)),
            "shadow_override_reason": job.get("shadow_override_reason", ""),
            "signal_time": signal_time,
            "enqueue_time": enqueue_time,
            "execution_start_time": execution_start_time,
            "webhook_received_time": job.get("webhook_received_time"),
            "classification_completed_time": job.get("classification_completed_time"),
            "queue_put_time": job.get("queue_put_time"),
            "worker_pickup_time": job.get("worker_pickup_time", execution_start_time),
            "preflight_completed_time": job.get("preflight_completed_time"),
            "bracket_submit_start_time": None,
            "bracket_submit_end_time": None,
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
            "intended_risk_percent": job.get("intended_risk_percent"),
            "allowed_money_risk": job.get("allowed_money_risk"),
            "stop_distance_points": job.get("stop_distance_points"),
            "raw_position_size": job.get("raw_position_size"),
            "normalized_position_size": job.get("normalized_position_size"),
            "intended_parent_quantity": float(job.get("normalized_position_size", 0.0) or 0.0),
            "original_intended_parent_quantity": float(job.get("normalized_position_size", 0.0) or 0.0),
            "planned_position_size": job.get("normalized_position_size", 0.0),
            "position_size": job.get("normalized_position_size", 0.0),
            "realized_entry_quantity": None,
            "realized_exit_quantity": None,
            "cumulative_entry_quantity": 0.0,
            "cumulative_entry_notional": 0.0,
            "cumulative_exit_quantity": 0.0,
            "cumulative_exit_notional": 0.0,
            "tp_exit_quantity": 0.0,
            "tp_exit_notional": 0.0,
            "tp_exit_fill_price": None,
            "sl_exit_quantity": 0.0,
            "sl_exit_notional": 0.0,
            "sl_exit_fill_price": None,
            "anomalies": [],
            "events": [],
            "summary_logged": False,
            "broker_ack_pending_since": None,
            "broker_ack_pending_last_check": None,
            "broker_ack_pending_reason": None,
            "broker_ack_pending_category": None,
            "broker_ack_pending_visible_order_ids": [],
            "partial_entry_timeout_started_at": None,
            "partial_entry_timeout_last_check": None,
            "partial_entry_timeout_deadline": None,
            "partial_entry_timeout_triggered": False,
            "partial_entry_timeout_snapshot_quantity": None,
            "partial_entry_timeout_snapshot_remaining_quantity": None,
            "partial_entry_timeout_snapshot_at": None,
            "partial_entry_remainder_cancel_requested_at": None,
            "partial_entry_remainder_cancel_reason": None,
            "partial_entry_remainder_cancelled": False,
            "partial_entry_parent_finality_pending_since": None,
            "partial_entry_parent_finality_last_check": None,
            "partial_entry_parent_final_status": None,
            "partial_entry_parent_final_quantity": None,
            "partial_entry_parent_final_remaining_quantity": None,
            "partial_entry_parent_final_authority_quantity": None,
            "partial_entry_parent_final_authority_source": None,
            "partial_entry_parent_finality_quantity_drift_detected": False,
            "partial_entry_parent_finality_drift_quantity": None,
            "partial_entry_parent_finality_candidate_quantity": None,
            "partial_entry_parent_finality_candidate_status": None,
            "partial_entry_parent_finality_candidate_stable_pass_seen": False,
            "parent_last_status": None,
            "execution_validation_status": "submitted_to_ib",
            "broker_acknowledged_at": None,
            "broker_live_at": None,
            "exit_cleanup_last_snapshot": None,
            "timeout_retained_replacement_parentless_allowed": False,
            "submitted_counted": False,
            "broker_acknowledged_counted": False,
            "broker_live_counted": False,
        }

        return record

    def append_trade_event(self, trade_id, message):
        with self.trade_analysis_lock:
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
            self.get_symbol_stats_bucket(record["symbol"])["total_trades"] += 1

            self.active_execution_trade_id = trade_id

            self.append_trade_event(
                trade_id,
                f"REGISTERED symbol={record['symbol']} side={record['side']} "
                f"grade={record['grade']} det_classification={record['det_classification']} "
                f"truth_classification={record['truth_classification']} "
                f"execution_lane={record['execution_lane']} "
                f"promoted_from_shadow={record['promoted_from_shadow']} "
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

        with self.trade_analysis_lock:
            trade_id = self.order_to_trade.get(order_id)
            if trade_id is None:
                return
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return

            if order_id == record["parent_order_id"]:
                record["parent_perm_id"] = perm_id
            elif order_id == record["tp_order_id"]:
                record["tp_perm_id"] = perm_id
            elif order_id == record["sl_order_id"]:
                record["sl_perm_id"] = perm_id

    def mark_trade_state(self, order_id, status):
        with self.trade_analysis_lock:
            trade_id = self.order_to_trade.get(order_id)
            if trade_id is None:
                return
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return

            current_state = record["state"]
            if order_id == record["parent_order_id"]:
                record["parent_last_status"] = status
            if current_state == "BROKER_ACK_PENDING":
                self.append_trade_event(
                    trade_id,
                    f"BROKER ACK PENDING STATE PRESERVED orderId={order_id} status={status}"
                )
                logger.info(
                    "BROKER_ACK_PENDING_PRESERVED | "
                    f"trade_id={trade_id} "
                    f"order_id={order_id} "
                    f"status={status} "
                    "reason=generic_status_update_does_not_exit_pending_ack"
                )
                return
            if self.is_partial_timeout_parent_finality_owner_active(record) and order_id == record["parent_order_id"]:
                self.append_trade_event(
                    trade_id,
                    f"PARENT FINALITY OWNERSHIP ACTIVE orderId={order_id} status={status} "
                    "decision=suppress_callback_state_promotion"
                )
                logger.info(
                    "PARENT_FINALITY_OWNERSHIP_ACTIVE | "
                    f"trade_id={trade_id} "
                    f"order_id={order_id} "
                    f"status={status} "
                    "reason=parent_finality_processor_controls_timeout_retained_promotion"
                )
                return

            if order_id == record["parent_order_id"]:
                if status in ("PendingSubmit", "ApiPending", "PreSubmitted", "Submitted"):
                    if not record["entry_filled"]:
                        record["state"] = "ENTRY_WORKING"
                elif status == "Filled":
                    if record["entry_filled"]:
                        record["state"] = "ENTRY_FILLED"
                    else:
                        record["state"] = "ENTRY_WORKING"
                elif status in ("Cancelled", "ApiCancelled", "Inactive"):
                    if not record["entry_filled"] and self.has_realized_parent_entry(record):
                        promotion_reason = (
                            "parent_cancelled_after_timeout_remainder_cancel"
                            if record.get("partial_entry_timeout_triggered")
                            else "parent_cancelled_after_partial_fill_status"
                        )
                        promoted = self.promote_partial_entry_to_live_quantity(
                            trade_id,
                            record,
                            promotion_reason
                        )
                        if promoted:
                            self.append_trade_event(
                                trade_id,
                                f"PARENT CANCEL PRESERVED REALIZED ENTRY quantity={record.get('cumulative_entry_quantity')} "
                                f"status={status} promotion_reason={promotion_reason}"
                            )
                            logger.warning(
                                "PARENT CANCEL PRESERVED REALIZED ENTRY | "
                                f"trade_id={trade_id} "
                                f"symbol={record['symbol']} "
                                f"status={status} "
                                f"promotion_reason={promotion_reason} "
                                f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                                f"resulting_state={record['state']}"
                            )
                        else:
                            self.append_anomaly(trade_id, "PARENT_CANCEL_PARTIAL_PROMOTION_FAILED")
                            record["state"] = "INCOMPLETE"
                            self.append_trade_event(
                                trade_id,
                                f"PARENT CANCEL FAIL-CLOSED status={status} "
                                "reason=partial_fill_promotion_failed resulting_state=INCOMPLETE"
                            )
                            logger.error(
                                "PARENT CANCEL FAIL-CLOSED | "
                                f"trade_id={trade_id} "
                                f"symbol={record['symbol']} "
                                f"status={status} "
                                "reason=partial_fill_promotion_failed "
                                "resulting_state=INCOMPLETE"
                            )
                    elif record["entry_filled"]:
                        self.append_trade_event(
                            trade_id,
                            f"PARENT CANCEL OBSERVED AFTER ENTRY LOCKED status={status} "
                            f"realized_entry_quantity={record.get('realized_entry_quantity')}"
                        )
                        logger.info(
                            "PARENT CANCEL AFTER ENTRY LOCKED | "
                            f"trade_id={trade_id} "
                            f"symbol={record['symbol']} "
                            f"status={status} "
                            f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                            f"state_preserved={record['state']}"
                        )
                    elif not record["entry_filled"]:
                        record["state"] = "CANCELLED"
                        self.append_trade_event(
                            trade_id,
                            f"PARENT CANCELLED WITHOUT ENTRY status={status} resulting_state={record['state']}"
                        )
                        logger.warning(
                            "PARENT CANCELLED WITHOUT ENTRY | "
                            f"trade_id={trade_id} "
                            f"symbol={record['symbol']} "
                            f"status={status} "
                            "resulting_state=CANCELLED"
                        )

            elif order_id in (record["tp_order_id"], record["sl_order_id"]):
                if record["entry_filled"] and status in ("PendingSubmit", "ApiPending", "PreSubmitted", "Submitted"):
                    record["state"] = "EXIT_WORKING"
                elif order_id == record["tp_order_id"] and status == "Filled":
                    if self.is_exit_fully_filled(record) and record.get("exit_reason") == "TP":
                        record["state"] = "TP_FILLED"
                    elif self.is_exit_fully_filled(record):
                        record["state"] = "CLOSED"
                    else:
                        record["state"] = "EXIT_WORKING"
                elif order_id == record["sl_order_id"] and status == "Filled":
                    if self.is_exit_fully_filled(record) and record.get("exit_reason") == "SL":
                        record["state"] = "SL_FILLED"
                    elif self.is_exit_fully_filled(record):
                        record["state"] = "CLOSED"
                    else:
                        record["state"] = "EXIT_WORKING"

            if current_state != record["state"]:
                self.append_trade_event(trade_id, f"STATE {current_state} -> {record['state']}")

    def append_anomaly(self, trade_id, message):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return
            if message not in record["anomalies"]:
                record["anomalies"].append(message)
                self.append_trade_event(trade_id, f"ANOMALY {message}")

    def contract_matches_symbol(self, contract, symbol):
        if contract is None or not symbol:
            return False

        values = {
            getattr(contract, "symbol", None),
            getattr(contract, "localSymbol", None),
            getattr(contract, "tradingClass", None),
        }

        for value in values:
            if value is None:
                continue
            text = str(value).upper()
            if text == symbol or text.startswith(symbol):
                return True

        return False

    def get_trade_broker_reality(self, record):
        order_ids = {
            record.get("parent_order_id"),
            record.get("tp_order_id"),
            record.get("sl_order_id"),
        }
        order_ids.discard(None)

        perm_ids = {
            record.get("parent_perm_id"),
            record.get("tp_perm_id"),
            record.get("sl_perm_id"),
        }
        perm_ids.discard(None)
        perm_ids.discard(0)

        open_trade_order_ids = []
        open_trade_perm_ids = []
        symbol_open_trade_order_ids = []
        symbol_open_trade_perm_ids = []
        open_order_ids = []
        position_sizes = []

        try:
            open_trades = self.ib.openTrades()
            positions = self.ib.positions()
            open_orders = self.ib.openOrders()
        except Exception as exc:
            logger.exception(
                "ACTIVE LOCK BROKER REALITY CHECK FAILED | "
                f"trade_id={record['trade_id']} symbol={record['symbol']} state={record['state']}"
            )
            return {
                "broker_real": True,
                "has_open_trade_match": False,
                "has_direct_open_trade_match": False,
                "has_symbol_open_trade_match": False,
                "has_open_order_match": False,
                "has_direct_broker_match": False,
                "has_position_match": False,
                "matching_open_trade_order_ids": [],
                "matching_open_trade_perm_ids": [],
                "matching_open_order_ids": [],
                "matching_position_sizes": [],
                "check_failed": True,
                "failure": str(exc),
            }

        for trade in open_trades:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            contract = getattr(trade, "contract", None)
            order_id = getattr(order, "orderId", None)
            perm_id = getattr(status, "permId", None)
            order_status = getattr(status, "status", None)

            if order_status in OPEN_BROKER_ORDER_STATUSES and order_id in order_ids:
                open_trade_order_ids.append(order_id)
            if order_status in OPEN_BROKER_ORDER_STATUSES and perm_id in perm_ids:
                open_trade_perm_ids.append(perm_id)
            if (
                order_status in OPEN_BROKER_ORDER_STATUSES
                and self.contract_matches_symbol(contract, record["symbol"])
            ):
                if order_id is not None:
                    symbol_open_trade_order_ids.append(order_id)
                if perm_id not in (None, 0):
                    symbol_open_trade_perm_ids.append(perm_id)

        for order in open_orders:
            order_id = getattr(order, "orderId", None)
            perm_id = getattr(order, "permId", None)
            if order_id in order_ids or perm_id in perm_ids:
                if order_id is not None:
                    open_order_ids.append(order_id)

        for position in positions:
            contract = getattr(position, "contract", None)
            position_size = getattr(position, "position", 0)
            if position_size and self.contract_matches_symbol(contract, record["symbol"]):
                position_sizes.append(position_size)

        has_open_trade_match = bool(
            open_trade_order_ids
            or open_trade_perm_ids
            or symbol_open_trade_order_ids
            or symbol_open_trade_perm_ids
        )
        has_direct_open_trade_match = bool(open_trade_order_ids or open_trade_perm_ids)
        has_symbol_open_trade_match = bool(symbol_open_trade_order_ids or symbol_open_trade_perm_ids)
        has_open_order_match = bool(open_order_ids)
        has_direct_broker_match = has_direct_open_trade_match or has_open_order_match
        has_position_match = bool(position_sizes)
        broker_real = has_open_trade_match or has_open_order_match or has_position_match

        return {
            "broker_real": broker_real,
            "has_open_trade_match": has_open_trade_match,
            "has_direct_open_trade_match": has_direct_open_trade_match,
            "has_symbol_open_trade_match": has_symbol_open_trade_match,
            "has_open_order_match": has_open_order_match,
            "has_direct_broker_match": has_direct_broker_match,
            "has_position_match": has_position_match,
            "matching_open_trade_order_ids": sorted(set(open_trade_order_ids + symbol_open_trade_order_ids)),
            "matching_open_trade_perm_ids": sorted(set(open_trade_perm_ids + symbol_open_trade_perm_ids)),
            "matching_open_order_ids": sorted(set(open_order_ids)),
            "matching_position_sizes": position_sizes,
            "check_failed": False,
        }

    def cleanup_stale_active_trade_lock(self, trade_id, broker_reality, reason_label):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return False

            if record["summary_logged"] or record["state"] not in ACTIVE_TRADE_STATES:
                return False

            previous_state = record["state"]
            self.append_anomaly(trade_id, "STALE_ACTIVE_LOCK_CLEANED")
            self.append_trade_event(
                trade_id,
                f"STALE ACTIVE LOCK CLEANUP trigger={reason_label} previous_state={previous_state}"
            )
            record["state"] = "INCOMPLETE"

            logger.warning(
                "STALE ACTIVE LOCK CLEANED | "
                f"trade_id={trade_id} "
                f"symbol={record['symbol']} "
                f"previous_state={previous_state} "
                f"new_state={record['state']} "
                f"entry_filled={record['entry_filled']} "
                f"open_trade_match={broker_reality['has_open_trade_match']} "
                f"open_order_match={broker_reality['has_open_order_match']} "
                f"position_match={broker_reality['has_position_match']} "
                f"open_trade_order_ids={broker_reality['matching_open_trade_order_ids']} "
                f"open_trade_perm_ids={broker_reality['matching_open_trade_perm_ids']} "
                f"open_order_ids={broker_reality['matching_open_order_ids']} "
                f"position_sizes={broker_reality['matching_position_sizes']} "
                f"reason={reason_label}"
            )
            logger.warning(
                "BRACKET_RECOVERY_DECISION | "
                f"trade_id={trade_id} "
                f"symbol={record['symbol']} "
                f"decision=mark_incomplete_and_release_if_finalizable "
                f"previous_state={previous_state} "
                f"broker_real={broker_reality['broker_real']} "
                f"open_trade_match={broker_reality['has_open_trade_match']} "
                f"open_order_match={broker_reality['has_open_order_match']} "
                f"position_match={broker_reality['has_position_match']} "
                f"reason={reason_label}"
            )

        self.finalize_trade_if_complete(trade_id)
        return True

    def should_release_fail_closed_incomplete_on_symbol_ambiguity(self, record, broker_reality):
        return bool(
            record.get("state") == "INCOMPLETE" and
            record.get("execution_validation_status") == "validation_incomplete" and
            broker_reality["has_symbol_open_trade_match"] and
            not broker_reality["has_direct_open_trade_match"] and
            not broker_reality["has_open_order_match"] and
            not broker_reality["has_direct_broker_match"] and
            not broker_reality["has_position_match"]
        )

    def recover_broker_ack_pending_trade(self, record_snapshot, broker_reality, reason_label):
        trade_id = record_snapshot["trade_id"]
        stage = BOT_STAGE
        now_dt = datetime.now(timezone.utc)
        pending_since = record_snapshot.get("broker_ack_pending_since")
        pending_age = self.seconds_between(pending_since, now_dt)
        snapshot_category = record_snapshot.get("broker_ack_pending_category") or "WAITABLE_ACK"

        if broker_reality["broker_real"]:
            confirmation = self.assess_broker_bracket_confirmation(
                record_snapshot["parent_order_id"],
                record_snapshot["tp_order_id"],
                record_snapshot["sl_order_id"]
            )
            broken_or_terminal_fail_closed = False
            broken_or_terminal_previous_state = None
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is None or record["summary_logged"] or record["state"] != "BROKER_ACK_PENDING":
                    return False
                record["broker_ack_pending_last_check"] = now_dt
                record["broker_ack_pending_reason"] = confirmation["reason"]
                record["broker_ack_pending_category"] = confirmation.get("broker_state_category")
                record["broker_ack_pending_visible_order_ids"] = confirmation["visible_order_ids"]
                broker_state_category = confirmation.get("broker_state_category", "BROKEN_OR_TERMINAL")
                if broker_state_category == "ACKNOWLEDGED":
                    previous_state = record["state"]
                    record["state"] = "ENTRY_WORKING"
                    self.append_trade_event(
                        trade_id,
                        f"BROKER ACK PENDING EXIT previous_state={previous_state} "
                        f"new_state={record['state']} broker_state_category={broker_state_category} "
                        f"reason={confirmation['reason']}"
                    )
                    logger.info(
                        "BROKER_ACK_PENDING_EXIT | "
                        f"trade_id={trade_id} "
                        f"symbol={record['symbol']} "
                        f"broker_state_category={broker_state_category} "
                        f"decision=entry_working "
                        f"reason={confirmation['reason']} "
                        f"pending_age_sec={pending_age} "
                        f"visible_order_ids={confirmation['visible_order_ids']}"
                    )
                    if confirmation.get("all_broker_live"):
                        self.set_execution_validation_status(
                            trade_id,
                            "broker_live",
                            f"{confirmation.get('outcome')}:{confirmation['reason']}"
                        )
                    else:
                        self.set_execution_validation_status(
                            trade_id,
                            "broker_acknowledged",
                            f"{confirmation.get('outcome')}:{confirmation['reason']}"
                        )
                    return True
                if broker_state_category == "BROKEN_OR_TERMINAL":
                    broken_or_terminal_previous_state = record["state"]
                    record["state"] = "INCOMPLETE"
                    self.append_trade_event(
                        trade_id,
                        f"BROKER ACK PENDING RECOVERY FAIL-CLOSED previous_state={broken_or_terminal_previous_state} "
                        f"new_state={record['state']} broker_state_category={broker_state_category} "
                        f"reason={confirmation['reason']}"
                    )
                    broken_or_terminal_fail_closed = True

            if broken_or_terminal_fail_closed:
                self.append_anomaly(trade_id, "BROKER_ACK_PENDING_BROKEN_OR_TERMINAL")
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    f"{confirmation.get('outcome')}:{confirmation['reason']}"
                )
                logger.error(
                    "BROKER_ACK_PENDING_RECOVERY_FAIL_CLOSED | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"stage={stage} "
                    "broker_state_category=BROKEN_OR_TERMINAL "
                    "decision=fail_closed_recovery "
                    f"previous_state={broken_or_terminal_previous_state} "
                    f"confirmation_reason={confirmation['reason']} "
                    f"visible_order_ids={confirmation['visible_order_ids']} "
                    f"reason_label={reason_label}"
                )
                self.finalize_trade_if_complete(trade_id)
                return True

            broker_state_category = confirmation.get("broker_state_category", "BROKEN_OR_TERMINAL")
            preserve_pending_ack = broker_state_category in {"WAITABLE_ACK", "AMBIGUOUS_ACK"}

            if not preserve_pending_ack:
                self.append_anomaly(trade_id, "BROKER_ACK_PENDING_UNRECOVERABLE_CATEGORY")
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    f"{confirmation.get('outcome')}:{confirmation['reason']}"
                )
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None and not record["summary_logged"] and record["state"] == "BROKER_ACK_PENDING":
                        previous_state = record["state"]
                        record["state"] = "INCOMPLETE"
                        record["broker_ack_pending_last_check"] = now_dt
                        record["broker_ack_pending_reason"] = confirmation["reason"]
                        record["broker_ack_pending_category"] = broker_state_category
                        record["broker_ack_pending_visible_order_ids"] = confirmation["visible_order_ids"]
                        self.append_trade_event(
                            trade_id,
                            f"BROKER ACK PENDING RECOVERY FAIL-CLOSED previous_state={previous_state} "
                            f"new_state={record['state']} broker_state_category={broker_state_category} "
                            f"reason={confirmation['reason']}"
                        )
                logger.error(
                    "BROKER_ACK_PENDING_RECOVERY_FAIL_CLOSED | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"stage={stage} "
                    f"broker_state_category={broker_state_category} "
                    "decision=fail_closed_recovery "
                    f"confirmation_reason={confirmation['reason']} "
                    f"visible_order_ids={confirmation['visible_order_ids']} "
                    f"reason_label={reason_label}"
                )
                self.finalize_trade_if_complete(trade_id)
                return True

            if pending_age is not None and pending_age >= BROKER_ACK_PENDING_TIMEOUT_SECONDS:
                if stage == "TEST":
                    stage_timeout_decision = "keep_symbol_locked_test_broker_real_present"
                elif stage == "PAPER":
                    stage_timeout_decision = "keep_symbol_locked_paper_broker_real_present"
                else:
                    stage_timeout_decision = "keep_symbol_locked_live_broker_real_present"
                logger.warning(
                    "BROKER_ACK_PENDING_TIMEOUT_WITH_BROKER_REALITY | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"stage={stage} "
                    f"broker_state_category={broker_state_category} "
                    f"decision={stage_timeout_decision} "
                    f"pending_age_sec={pending_age} "
                    f"broker_real={broker_reality['broker_real']} "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    f"submitted_unacknowledged={confirmation.get('submitted_unacknowledged')} "
                    f"ambiguous_broker_state={confirmation.get('ambiguous_broker_state')} "
                    f"confirmation_reason={confirmation['reason']} "
                    f"reason={reason_label}"
                )
            else:
                logger.warning(
                    "BROKER_ACK_PENDING_PRESERVE | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"stage={stage} "
                    f"broker_state_category={broker_state_category} "
                    "decision=preserve_lock_pending_broker_real "
                    f"pending_age_sec={pending_age} "
                    f"submitted_unacknowledged={confirmation.get('submitted_unacknowledged')} "
                    f"ambiguous_broker_state={confirmation.get('ambiguous_broker_state')} "
                    f"broken_or_terminal_state={confirmation.get('broken_or_terminal_state')} "
                    f"confirmation_reason={confirmation['reason']} "
                    f"reason={reason_label}"
                )
            return False

        if pending_age is not None and pending_age < BROKER_ACK_PENDING_TIMEOUT_SECONDS:
            logger.warning(
                "BROKER_ACK_PENDING_RECOVERY_WAIT | "
                f"trade_id={trade_id} "
                f"symbol={record_snapshot['symbol']} "
                f"stage={stage} "
                f"broker_state_category={snapshot_category} "
                "decision=wait_for_broker_clarity "
                f"pending_age_sec={pending_age} "
                f"timeout_sec={BROKER_ACK_PENDING_TIMEOUT_SECONDS} "
                f"reason={reason_label}"
            )
            return True

        if stage == "TEST":
            stage_timeout_decision = "mark_incomplete_test_no_broker_reality"
        elif stage == "PAPER":
            stage_timeout_decision = "mark_incomplete_paper_no_broker_reality"
        else:
            stage_timeout_decision = "mark_incomplete_live_no_broker_reality"

        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None or record["summary_logged"] or record["state"] != "BROKER_ACK_PENDING":
                return False
            previous_state = record["state"]
            self.append_anomaly(trade_id, "BROKER_ACK_PENDING_TIMEOUT_NO_BROKER_REALITY")
            record["state"] = "INCOMPLETE"
            record["broker_ack_pending_last_check"] = now_dt
            self.append_trade_event(
                trade_id,
                f"BROKER ACK PENDING TIMEOUT CLEANUP previous_state={previous_state} "
                f"new_state={record['state']} pending_age_sec={pending_age} "
                f"stage={stage} broker_state_category={snapshot_category}"
            )
            logger.warning(
                "BROKER_ACK_PENDING_TIMEOUT_NO_BROKER_REALITY | "
                f"trade_id={trade_id} "
                f"symbol={record['symbol']} "
                f"stage={stage} "
                f"broker_state_category={snapshot_category} "
                f"decision={stage_timeout_decision} "
                f"pending_age_sec={pending_age} "
                f"reason={reason_label}"
            )
            logger.warning(
                "BRACKET_RECOVERY_DECISION | "
                f"trade_id={trade_id} "
                f"symbol={record['symbol']} "
                f"stage={stage} "
                f"broker_state_category={snapshot_category} "
                f"decision={stage_timeout_decision} "
                f"previous_state={previous_state} "
                f"broker_real={broker_reality['broker_real']} "
                f"reason={reason_label}"
            )

            self.finalize_trade_if_complete(trade_id)
            self.set_execution_validation_status(
                trade_id,
                "validation_incomplete",
                f"broker_ack_pending_timeout:{reason_label}"
            )
            return True

    def get_stage_concurrency_policy(self):
        stage = BOT_STAGE

        if stage == "TEST":
            policy = {
                "stage": stage,
                "mode": "unrestricted",
                "description": "TEST allows different-symbol concurrency, but same-symbol active execution remains hard-blocked",
            }
        elif stage == "PAPER":
            policy = {
                "stage": stage,
                "mode": "per_instrument",
                "description": "PAPER allows different symbols in parallel and blocks same-symbol active trade conflicts",
            }
        else:
            policy = {
                "stage": stage,
                "mode": "global_single",
                "description": "LIVE blocks on any real active trade globally",
            }

        logger.info(
            "CONCURRENCY POLICY | "
            f"stage={policy['stage']} "
            f"mode={policy['mode']} "
            f"description={policy['description']}"
        )
        return policy

    def get_active_trade_candidates(self, reason_label="UNSPECIFIED"):
        with self.trade_analysis_lock:
            active_records = [
                {
                    "trade_id": record["trade_id"],
                    "symbol": record["symbol"],
                    "state": record["state"],
                    "entry_filled": record["entry_filled"],
                    "closed": record["closed"],
                    "summary_logged": record["summary_logged"],
                    "cumulative_entry_quantity": record["cumulative_entry_quantity"],
                    "cumulative_exit_quantity": record["cumulative_exit_quantity"],
                    "realized_entry_quantity": record["realized_entry_quantity"],
                    "realized_exit_quantity": record["realized_exit_quantity"],
                    "execution_validation_status": record.get("execution_validation_status"),
                    "parent_order_id": record["parent_order_id"],
                    "tp_order_id": record["tp_order_id"],
                    "sl_order_id": record["sl_order_id"],
                    "parent_perm_id": record["parent_perm_id"],
                    "tp_perm_id": record["tp_perm_id"],
                    "sl_perm_id": record["sl_perm_id"],
                    "broker_ack_pending_since": record.get("broker_ack_pending_since"),
                    "broker_ack_pending_last_check": record.get("broker_ack_pending_last_check"),
                    "broker_ack_pending_reason": record.get("broker_ack_pending_reason"),
                    "broker_ack_pending_category": record.get("broker_ack_pending_category"),
                    "broker_ack_pending_visible_order_ids": record.get("broker_ack_pending_visible_order_ids"),
                }
                for record in self.trade_analysis.values()
                if not record["summary_logged"]
            ]

        logger.info(
            "CONCURRENCY CANDIDATES | "
            f"reason={reason_label} "
            f"count={len(active_records)}"
        )

        broker_real_candidates = []

        for record_snapshot in active_records:
            broker_reality = self.get_trade_broker_reality(record_snapshot)

            logger.info(
                "ACTIVE LOCK BROKER REALITY CHECK | "
                f"trade_id={record_snapshot['trade_id']} "
                f"symbol={record_snapshot['symbol']} "
                f"state={record_snapshot['state']} "
                f"entry_filled={record_snapshot['entry_filled']} "
                f"broker_real={broker_reality['broker_real']} "
                f"open_trade_match={broker_reality['has_open_trade_match']} "
                f"direct_open_trade_match={broker_reality['has_direct_open_trade_match']} "
                f"symbol_open_trade_match={broker_reality['has_symbol_open_trade_match']} "
                f"open_order_match={broker_reality['has_open_order_match']} "
                f"direct_broker_match={broker_reality['has_direct_broker_match']} "
                f"position_match={broker_reality['has_position_match']} "
                f"open_trade_order_ids={broker_reality['matching_open_trade_order_ids']} "
                f"open_trade_perm_ids={broker_reality['matching_open_trade_perm_ids']} "
                f"open_order_ids={broker_reality['matching_open_order_ids']} "
                f"position_sizes={broker_reality['matching_position_sizes']} "
                f"check_failed={broker_reality['check_failed']} "
                f"reason={reason_label}"
            )

            if record_snapshot["state"] == "BROKER_ACK_PENDING":
                recovered = self.recover_broker_ack_pending_trade(
                    record_snapshot,
                    broker_reality,
                    reason_label
                )
                if recovered:
                    with self.trade_analysis_lock:
                        refreshed_record = self.trade_analysis.get(record_snapshot["trade_id"])
                        if refreshed_record is None or refreshed_record["summary_logged"]:
                            continue
                        refreshed_state = refreshed_record["state"]
                        refreshed_closed = refreshed_record["closed"]
                        refreshed_entry_filled = refreshed_record["entry_filled"]

                    if refreshed_state in ACTIVE_TRADE_STATES:
                        logger.warning(
                            "SYMBOL_LOCK_REASON | "
                            f"trade_id={record_snapshot['trade_id']} "
                            f"symbol={record_snapshot['symbol']} "
                            f"state={refreshed_state} "
                            "decision=lock_symbol "
                            "reason=broker_ack_pending_recovery_active "
                            f"open_trade_match={broker_reality['has_open_trade_match']} "
                            f"open_order_match={broker_reality['has_open_order_match']} "
                            f"position_match={broker_reality['has_position_match']}"
                        )
                        broker_real_candidates.append({
                            "trade_id": record_snapshot["trade_id"],
                            "symbol": record_snapshot["symbol"],
                            "state": refreshed_state,
                            "entry_filled": refreshed_entry_filled,
                            "closed": refreshed_closed,
                            "internal_lifecycle_active": True,
                            "broker_reality": broker_reality,
                        })
                    continue

            if self.should_release_fail_closed_incomplete_on_symbol_ambiguity(
                record_snapshot,
                broker_reality,
            ):
                logger.warning(
                    "FAIL_CLOSED_INCOMPLETE_LOCK_RELEASE | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    "decision=do_not_preserve_symbol_lock "
                    "reason=fail_closed_incomplete_with_symbol_level_broker_ambiguity_only "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"direct_open_trade_match={broker_reality['has_direct_open_trade_match']} "
                    f"symbol_open_trade_match={broker_reality['has_symbol_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"direct_broker_match={broker_reality['has_direct_broker_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    f"reason_label={reason_label}"
                )
                self.finalize_trade_if_complete(record_snapshot["trade_id"])
                with self.trade_analysis_lock:
                    refreshed_record = self.trade_analysis.get(record_snapshot["trade_id"])
                    if refreshed_record is None or refreshed_record["summary_logged"]:
                        continue
                continue

            if broker_reality["broker_real"]:
                logger.warning(
                    "ACTIVE LOCK PRESERVED | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    f"reason={reason_label} "
                    "broker-side activity still exists"
                )
                logger.warning(
                    "SYMBOL_LOCK_REASON | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    "decision=lock_symbol "
                    "reason=broker_real_activity_present "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"direct_open_trade_match={broker_reality['has_direct_open_trade_match']} "
                    f"symbol_open_trade_match={broker_reality['has_symbol_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"direct_broker_match={broker_reality['has_direct_broker_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    f"open_trade_order_ids={broker_reality['matching_open_trade_order_ids']} "
                    f"open_order_ids={broker_reality['matching_open_order_ids']} "
                    f"position_sizes={broker_reality['matching_position_sizes']}"
                )
                broker_real_candidates.append({
                    "trade_id": record_snapshot["trade_id"],
                    "symbol": record_snapshot["symbol"],
                    "state": record_snapshot["state"],
                    "entry_filled": record_snapshot["entry_filled"],
                    "closed": record_snapshot["closed"],
                    "internal_lifecycle_active": True,
                    "broker_reality": broker_reality,
                })
                continue

            if record_snapshot["state"] in ACTIVE_TRADE_STATES:
                if record_snapshot["state"] == "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL":
                    logger.warning(
                        "ACTIVE LOCK PRESERVED | "
                        f"trade_id={record_snapshot['trade_id']} "
                        f"symbol={record_snapshot['symbol']} "
                        f"state={record_snapshot['state']} "
                        f"reason={reason_label} "
                        "parent finality unresolved"
                    )
                    logger.warning(
                        "SYMBOL_LOCK_REASON | "
                        f"trade_id={record_snapshot['trade_id']} "
                        f"symbol={record_snapshot['symbol']} "
                        f"state={record_snapshot['state']} "
                        "decision=lock_symbol "
                        "reason=partial_timeout_parent_finality_unresolved "
                        f"open_trade_match={broker_reality['has_open_trade_match']} "
                        f"open_order_match={broker_reality['has_open_order_match']} "
                        f"position_match={broker_reality['has_position_match']}"
                    )
                    broker_real_candidates.append({
                        "trade_id": record_snapshot["trade_id"],
                        "symbol": record_snapshot["symbol"],
                        "state": record_snapshot["state"],
                        "entry_filled": record_snapshot["entry_filled"],
                        "closed": record_snapshot["closed"],
                        "internal_lifecycle_active": True,
                        "broker_reality": broker_reality,
                    })
                    continue

                cleanup_completed = self.cleanup_stale_active_trade_lock(
                    record_snapshot["trade_id"],
                    broker_reality,
                    reason_label
                )
                if cleanup_completed:
                    logger.warning(
                        "STALE ACTIVE LOCK CLEANUP COMPLETE | "
                        f"trade_id={record_snapshot['trade_id']} "
                        f"symbol={record_snapshot['symbol']} "
                        f"state={record_snapshot['state']} "
                        f"reason={reason_label} "
                        "decision=release_candidate_removed"
                    )
                    continue

                logger.warning(
                    "ACTIVE LOCK PRESERVED | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    f"reason={reason_label} "
                    "internal lifecycle not safely finalized"
                )
                logger.warning(
                    "SYMBOL_LOCK_REASON | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    "decision=lock_symbol "
                    "reason=internal_lifecycle_active "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"position_match={broker_reality['has_position_match']}"
                )
                broker_real_candidates.append({
                    "trade_id": record_snapshot["trade_id"],
                    "symbol": record_snapshot["symbol"],
                    "state": record_snapshot["state"],
                    "entry_filled": record_snapshot["entry_filled"],
                    "closed": record_snapshot["closed"],
                    "internal_lifecycle_active": True,
                    "broker_reality": broker_reality,
                })
                continue

            logger.warning(
                "UNFINALIZED TRADE LOCK PRESERVED | "
                f"trade_id={record_snapshot['trade_id']} "
                f"symbol={record_snapshot['symbol']} "
                f"state={record_snapshot['state']} "
                f"reason={reason_label} "
                "no broker-side activity remains but summary is not logged"
            )
            logger.warning(
                "BRACKET_RECOVERY_DECISION | "
                f"trade_id={record_snapshot['trade_id']} "
                f"symbol={record_snapshot['symbol']} "
                f"state={record_snapshot['state']} "
                "decision=attempt_finalize_unfinalized_trade "
                f"broker_real={broker_reality['broker_real']} "
                f"reason={reason_label}"
            )
            self.finalize_trade_if_complete(record_snapshot["trade_id"])
            with self.trade_analysis_lock:
                refreshed_record = self.trade_analysis.get(record_snapshot["trade_id"])
                if refreshed_record is None or refreshed_record["summary_logged"]:
                    logger.info(
                        "UNFINALIZED TRADE LOCK RELEASED | "
                        f"trade_id={record_snapshot['trade_id']} "
                        f"symbol={record_snapshot['symbol']} "
                        f"reason={reason_label} "
                        "decision=release_candidate_removed"
                    )
                    continue

            broker_real_candidates.append({
                "trade_id": record_snapshot["trade_id"],
                "symbol": record_snapshot["symbol"],
                "state": record_snapshot["state"],
                "entry_filled": record_snapshot["entry_filled"],
                "closed": record_snapshot["closed"],
                "internal_lifecycle_active": True,
                "broker_reality": broker_reality,
            })

        logger.info(
            "CONCURRENCY CANDIDATES RESULT | "
            f"reason={reason_label} "
            f"active_count={len(broker_real_candidates)}"
        )
        return broker_real_candidates

    def has_conflicting_active_trade_for_job(self, job):
        policy = self.get_stage_concurrency_policy()
        incoming_symbol = job["symbol"]
        active_candidates = self.get_active_trade_candidates(
            f"CONCURRENCY_CHECK_{policy['stage']}"
        )

        if policy["mode"] == "unrestricted":
            if active_candidates:
                for candidate in active_candidates:
                    broker_reality = candidate["broker_reality"]
                    if candidate["symbol"] == incoming_symbol:
                        logger.warning(
                            "CONCURRENCY CHECK | "
                            f"stage={policy['stage']} "
                            f"incoming_symbol={incoming_symbol} "
                            f"conflicting_trade_id={candidate['trade_id']} "
                            f"conflicting_symbol={candidate['symbol']} "
                            f"conflicting_state={candidate['state']} "
                            f"internal_lifecycle_active={candidate['internal_lifecycle_active']} "
                            f"open_trade_match={broker_reality['has_open_trade_match']} "
                            f"open_order_match={broker_reality['has_open_order_match']} "
                            f"position_match={broker_reality['has_position_match']} "
                            f"open_trade_order_ids={broker_reality['matching_open_trade_order_ids']} "
                            f"open_order_ids={broker_reality['matching_open_order_ids']} "
                            f"position_sizes={broker_reality['matching_position_sizes']} "
                            "decision=block "
                            "reason=same_symbol_hard_lock"
                        )
                        return True, candidate, policy
                    logger.info(
                        "CONCURRENCY CHECK | "
                        f"stage={policy['stage']} "
                        f"incoming_symbol={incoming_symbol} "
                        f"observed_trade_id={candidate['trade_id']} "
                        f"observed_symbol={candidate['symbol']} "
                        f"observed_state={candidate['state']} "
                        f"internal_lifecycle_active={candidate['internal_lifecycle_active']} "
                        f"open_trade_match={broker_reality['has_open_trade_match']} "
                        f"open_order_match={broker_reality['has_open_order_match']} "
                        f"position_match={broker_reality['has_position_match']} "
                        "decision=allow "
                        "reason=test_stage_unrestricted_concurrency"
                    )
            else:
                logger.info(
                    "CONCURRENCY CHECK | "
                    f"stage={policy['stage']} "
                    f"incoming_symbol={incoming_symbol} "
                    "decision=allow "
                    "reason=test_stage_unrestricted_concurrency_no_active_candidates"
                )
            return False, None, policy

        if policy["mode"] == "per_instrument":
            for candidate in active_candidates:
                if candidate["symbol"] == incoming_symbol:
                    broker_reality = candidate["broker_reality"]
                    logger.warning(
                        "CONCURRENCY CHECK | "
                        f"stage={policy['stage']} "
                        f"incoming_symbol={incoming_symbol} "
                        f"conflicting_trade_id={candidate['trade_id']} "
                        f"conflicting_symbol={candidate['symbol']} "
                        f"conflicting_state={candidate['state']} "
                        f"internal_lifecycle_active={candidate['internal_lifecycle_active']} "
                        f"open_trade_match={broker_reality['has_open_trade_match']} "
                        f"open_order_match={broker_reality['has_open_order_match']} "
                        f"position_match={broker_reality['has_position_match']} "
                        "decision=block "
                        "reason=same_symbol_active_trade"
                    )
                    return True, candidate, policy

                broker_reality = candidate["broker_reality"]
                logger.info(
                    "CONCURRENCY CHECK | "
                    f"stage={policy['stage']} "
                    f"incoming_symbol={incoming_symbol} "
                    f"conflicting_trade_id={candidate['trade_id']} "
                    f"conflicting_symbol={candidate['symbol']} "
                    f"conflicting_state={candidate['state']} "
                    f"internal_lifecycle_active={candidate['internal_lifecycle_active']} "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    "decision=allow "
                    "reason=different_symbol"
                )

            logger.info(
                "CONCURRENCY CHECK | "
                f"stage={policy['stage']} "
                f"incoming_symbol={incoming_symbol} "
                "decision=allow "
                "reason=no_same_symbol_conflict"
            )
            return False, None, policy

        for candidate in active_candidates:
            broker_reality = candidate["broker_reality"]
            logger.warning(
                "CONCURRENCY CHECK | "
                f"stage={policy['stage']} "
                f"incoming_symbol={incoming_symbol} "
                f"conflicting_trade_id={candidate['trade_id']} "
                f"conflicting_symbol={candidate['symbol']} "
                f"conflicting_state={candidate['state']} "
                f"internal_lifecycle_active={candidate['internal_lifecycle_active']} "
                f"open_trade_match={broker_reality['has_open_trade_match']} "
                f"open_order_match={broker_reality['has_open_order_match']} "
                f"position_match={broker_reality['has_position_match']} "
                "decision=block "
                "reason=live_global_single_active_trade"
            )
            return True, candidate, policy

        logger.info(
            "CONCURRENCY CHECK | "
            f"stage={policy['stage']} "
            f"incoming_symbol={incoming_symbol} "
            "decision=allow "
            "reason=no_active_trade_conflict"
        )
        return False, None, policy

    def has_active_trade_locked(self):
        active_candidates = self.get_active_trade_candidates("LEGACY_ACTIVE_LOCK_CHECK")
        if active_candidates:
            candidate = active_candidates[0]
            return True, candidate["trade_id"], candidate["symbol"], candidate["state"]
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

            if state in ("Cancelled", "ApiCancelled", "Inactive") and order_id == record["parent_order_id"]:
                if record["entry_filled"] or self.has_realized_parent_entry(record):
                    logger.warning(
                        "PARENT CANCEL STATUS PRESERVED REALIZED ENTRY | "
                        f"trade_id={trade_id} "
                        f"symbol={record['symbol']} "
                        f"status={state} "
                        f"entry_filled={record['entry_filled']} "
                        f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                        f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
                        f"partial_entry_timeout_triggered={record.get('partial_entry_timeout_triggered')} "
                        "cancelled_count_incremented=false"
                    )
                else:
                    logger.warning(
                        "PARENT CANCEL STATUS ORDINARY NON-ENTRY | "
                        f"trade_id={trade_id} "
                        f"symbol={record['symbol']} "
                        f"status={state} "
                        "cancelled_count_incremented=true"
                    )
                    self.aggregate_stats["cancelled_count"] += 1
                    self.finalize_trade_if_complete(trade_id)

            if state in ("Cancelled", "ApiCancelled", "Inactive") and record["state"] in ACTIVE_TRADE_STATES:
                self.get_active_trade_candidates("STATUS_CANCEL_CLEANUP")
        except Exception:
            logger.exception("TRADE STATUS ANALYSIS FAILED")

    def calculate_gross_pnl(self, record):
        if record["entry_fill_price"] is None or record["exit_fill_price"] is None:
            return 0.0

        multiplier = self.get_point_value(record["symbol"])
        entry_fill_price = record["entry_fill_price"]
        total_gross_pnl = 0.0

        for exit_price_field, exit_quantity_field in (
            ("tp_exit_fill_price", "tp_exit_quantity"),
            ("sl_exit_fill_price", "sl_exit_quantity"),
        ):
            exit_fill_price = record.get(exit_price_field)
            exit_quantity = float(record.get(exit_quantity_field) or 0.0)

            if exit_fill_price is None or exit_quantity <= 0:
                continue

            if record["side"] == "long":
                total_gross_pnl += (exit_fill_price - entry_fill_price) * multiplier * exit_quantity
            else:
                total_gross_pnl += (entry_fill_price - exit_fill_price) * multiplier * exit_quantity

        return round(total_gross_pnl, 2)

    def normalize_fill_quantity(self, shares):
        try:
            if shares is None:
                return None
            quantity = abs(float(shares))
            if quantity <= 0:
                return None
            return quantity
        except Exception:
            return None

    def accumulate_quantity_and_notional(self, record, quantity_field, notional_field, price_field, quantity, price):
        if quantity is None or price is None or quantity <= 0:
            return

        cumulative_quantity = float(record.get(quantity_field) or 0.0) + quantity
        cumulative_notional = float(record.get(notional_field) or 0.0) + (quantity * price)

        record[quantity_field] = cumulative_quantity
        record[notional_field] = cumulative_notional
        record[price_field] = round(cumulative_notional / cumulative_quantity, 10)

    def get_original_parent_quantity(self, record):
        original_parent_quantity = record.get("original_intended_parent_quantity")
        if original_parent_quantity is not None:
            return float(original_parent_quantity or 0.0)
        return float(record.get("intended_parent_quantity") or 0.0)

    def get_timeout_snapshot_quantity(self, record):
        timeout_snapshot_quantity = record.get("partial_entry_timeout_snapshot_quantity")
        if timeout_snapshot_quantity is None:
            return None
        return float(timeout_snapshot_quantity or 0.0)

    def get_parent_remaining_quantity(self, record):
        intended_parent_quantity = self.get_original_parent_quantity(record)
        cumulative_entry_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        remaining_quantity = intended_parent_quantity - cumulative_entry_quantity
        if remaining_quantity <= 0:
            return 0.0
        return remaining_quantity

    def has_realized_parent_entry(self, record):
        realized_entry_quantity = float(record.get("realized_entry_quantity") or 0.0)
        cumulative_entry_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        return realized_entry_quantity > 0 or cumulative_entry_quantity > 0

    def has_partial_entry_fill(self, record):
        cumulative_entry_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        intended_parent_quantity = self.get_original_parent_quantity(record)
        return cumulative_entry_quantity > 0 and intended_parent_quantity > cumulative_entry_quantity

    def start_partial_entry_timeout(self, trade_id, record, fill_time=None):
        if record.get("partial_entry_timeout_started_at") is not None:
            return

        started_at = fill_time if isinstance(fill_time, datetime) else datetime.now(timezone.utc)
        deadline = started_at.timestamp() + PARTIAL_ENTRY_TIMEOUT_SECONDS
        record["partial_entry_timeout_started_at"] = started_at
        record["partial_entry_timeout_last_check"] = started_at
        record["partial_entry_timeout_deadline"] = deadline

        self.append_trade_event(
            trade_id,
            f"PARTIAL ENTRY TIMEOUT STARTED timeout_sec={PARTIAL_ENTRY_TIMEOUT_SECONDS} "
            f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
            f"remaining_quantity={self.get_parent_remaining_quantity(record)} "
            f"deadline_epoch={round(deadline, 3)}"
        )
        logger.warning(
            "PARTIAL ENTRY TIMEOUT STARTED | "
            f"trade_id={trade_id} "
            f"symbol={record['symbol']} "
            f"state={record['state']} "
            f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
            f"intended_parent_quantity={record.get('intended_parent_quantity')} "
            f"remaining_quantity={self.get_parent_remaining_quantity(record)} "
            f"timeout_sec={PARTIAL_ENTRY_TIMEOUT_SECONDS}"
        )

    def clear_partial_entry_timeout(self, record):
        record["partial_entry_timeout_started_at"] = None
        record["partial_entry_timeout_last_check"] = None
        record["partial_entry_timeout_deadline"] = None
        record["partial_entry_parent_finality_pending_since"] = None
        record["partial_entry_parent_finality_last_check"] = None
        record["partial_entry_parent_final_status"] = None
        record["partial_entry_parent_final_quantity"] = None
        record["partial_entry_parent_final_remaining_quantity"] = None
        record["partial_entry_parent_final_authority_quantity"] = None
        record["partial_entry_parent_final_authority_source"] = None
        record["partial_entry_parent_finality_quantity_drift_detected"] = False
        record["partial_entry_parent_finality_drift_quantity"] = None
        self.reset_partial_timeout_parent_finality_candidate(record)

    def is_parent_finality_pending(self, record):
        return bool(
            record and
            record.get("state") == "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL" and
            not record.get("entry_filled")
        )

    def is_partial_timeout_parent_finality_owner_active(self, record):
        return bool(
            record and
            not record.get("entry_filled") and
            record.get("partial_entry_timeout_triggered") and
            record.get("partial_entry_remainder_cancel_requested_at") is not None and
            (
                record.get("state") == "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL" or
                record.get("partial_entry_parent_finality_pending_since") is not None
            )
        )

    def reset_partial_timeout_parent_finality_candidate(self, record):
        record["partial_entry_parent_finality_candidate_quantity"] = None
        record["partial_entry_parent_finality_candidate_status"] = None
        record["partial_entry_parent_finality_candidate_stable_pass_seen"] = False

    def get_timeout_retained_quantity_authority(self, record, cumulative_entry_quantity=None):
        timeout_snapshot_quantity = self.get_timeout_snapshot_quantity(record)
        if timeout_snapshot_quantity is None or timeout_snapshot_quantity <= 0:
            return {
                "ok": False,
                "quantity": None,
                "source": None,
                "reason": "timeout_snapshot_quantity_missing",
            }

        if cumulative_entry_quantity is None:
            cumulative_entry_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        else:
            cumulative_entry_quantity = float(cumulative_entry_quantity or 0.0)

        original_parent_quantity = self.get_original_parent_quantity(record)

        if cumulative_entry_quantity + 1e-9 < timeout_snapshot_quantity:
            return {
                "ok": False,
                "quantity": None,
                "source": None,
                "reason": "timeout_snapshot_quantity_exceeds_cumulative_entry_quantity",
            }

        if abs(cumulative_entry_quantity - timeout_snapshot_quantity) < 1e-9:
            return {
                "ok": True,
                "quantity": timeout_snapshot_quantity,
                "source": "timeout_snapshot_quantity",
                "reason": "timeout_snapshot_quantity_stable",
            }

        if (
            original_parent_quantity > 0 and
            cumulative_entry_quantity + 1e-9 >= original_parent_quantity
        ):
            return {
                "ok": True,
                "quantity": original_parent_quantity,
                "source": "original_parent_quantity_fully_filled_after_timeout",
                "reason": "parent_fully_filled_after_timeout_snapshot",
            }

        return {
            "ok": False,
            "quantity": None,
            "source": None,
            "reason": "timeout_snapshot_quantity_drifted_before_finality",
        }

    def get_partial_timeout_parent_finality_snapshot(self, record):
        parent_order_id = record.get("parent_order_id")
        parent_perm_id = record.get("parent_perm_id")
        open_trade_visible = False
        open_order_visible = False
        parent_status = record.get("parent_last_status")
        parent_remaining_quantity = None
        broker_check_failed = False
        broker_failure = None

        try:
            for trade in self.ib.openTrades():
                order = getattr(trade, "order", None)
                status = getattr(trade, "orderStatus", None)
                order_id = getattr(order, "orderId", None)
                perm_id = getattr(status, "permId", None)
                if order_id == parent_order_id or (parent_perm_id not in (None, 0) and perm_id == parent_perm_id):
                    open_trade_visible = True
                    parent_status = getattr(status, "status", None) or parent_status
                    parent_remaining_quantity = getattr(status, "remaining", None)
                    break

            for order in self.ib.openOrders():
                order_id = getattr(order, "orderId", None)
                perm_id = getattr(order, "permId", None)
                if order_id == parent_order_id or (parent_perm_id not in (None, 0) and perm_id == parent_perm_id):
                    open_order_visible = True
                    if parent_remaining_quantity is None:
                        total_quantity = getattr(order, "totalQuantity", None)
                        try:
                            if total_quantity is not None:
                                parent_remaining_quantity = max(
                                    0.0,
                                    float(total_quantity) - float(record.get("cumulative_entry_quantity") or 0.0)
                                )
                        except Exception:
                            parent_remaining_quantity = None
                    break
        except Exception as exc:
            broker_check_failed = True
            broker_failure = str(exc)

        cumulative_entry_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        intended_parent_quantity = self.get_original_parent_quantity(record)
        authority = self.get_timeout_retained_quantity_authority(
            record,
            cumulative_entry_quantity=cumulative_entry_quantity
        )
        no_longer_visible = not open_trade_visible and not open_order_visible
        fully_filled = intended_parent_quantity > 0 and cumulative_entry_quantity >= intended_parent_quantity
        terminal_status = parent_status in {"Cancelled", "ApiCancelled", "Inactive", "Filled"}

        final = False
        finality_reason = "parent_finality_unresolved"
        if broker_check_failed:
            finality_reason = f"parent_finality_broker_check_failed:{broker_failure}"
        elif not authority["ok"]:
            finality_reason = authority["reason"]
        elif fully_filled:
            final = True
            finality_reason = "parent_fully_filled"
        elif no_longer_visible and terminal_status:
            final = True
            finality_reason = "parent_terminal_and_not_visible"

        return {
            "final": final,
            "reason": finality_reason,
            "parent_status": parent_status,
            "parent_remaining_quantity": parent_remaining_quantity,
            "parent_open_trade_visible": open_trade_visible,
            "parent_open_order_visible": open_order_visible,
            "parent_visible": open_trade_visible or open_order_visible,
            "cumulative_entry_quantity": cumulative_entry_quantity,
            "intended_parent_quantity": intended_parent_quantity,
            "timeout_snapshot_quantity": record.get("partial_entry_timeout_snapshot_quantity"),
            "authority_ok": authority["ok"],
            "authority_quantity": authority["quantity"],
            "authority_source": authority["source"],
            "authority_reason": authority["reason"],
            "broker_check_failed": broker_check_failed,
            "broker_failure": broker_failure,
        }

    def handle_late_parent_fill_after_retained_finalization(self, trade_id, record, previous_finalized_quantity):
        current_cumulative_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        if current_cumulative_quantity <= float(previous_finalized_quantity or 0.0):
            return True

        self.append_trade_event(
            trade_id,
            f"LATE PARENT FILL DETECTED previous_finalized_quantity={previous_finalized_quantity} "
            f"new_cumulative_entry_quantity={current_cumulative_quantity}"
        )
        logger.warning(
            "LATE PARENT FILL DETECTED | "
            f"trade_id={trade_id} "
            f"symbol={record['symbol']} "
            f"previous_finalized_quantity={previous_finalized_quantity} "
            f"new_cumulative_entry_quantity={current_cumulative_quantity}"
        )

        promoted = self.promote_partial_entry_to_live_quantity(
            trade_id,
            record,
            "late_parent_fill_after_retained_finalization"
        )
        if not promoted:
            self.append_anomaly(trade_id, "LATE_PARENT_FILL_PROMOTION_FAILED")
            record["state"] = "INCOMPLETE"
            self.set_execution_validation_status(
                trade_id,
                "validation_incomplete",
                "late_parent_fill_promotion_failed"
            )
            logger.error(
                "LATE PARENT FILL FAIL-CLOSED | "
                f"trade_id={trade_id} "
                f"symbol={record['symbol']} "
                "reason=late_parent_fill_promotion_failed"
            )
            return False

        self.append_trade_event(
            trade_id,
            f"RETAINED CHILD QUANTITY REVALIDATION TRIGGERED previous_finalized_quantity={previous_finalized_quantity} "
            f"updated_finalized_quantity={record.get('realized_entry_quantity')}"
        )
        logger.warning(
            "RETAINED CHILD QUANTITY REVALIDATION TRIGGERED | "
            f"trade_id={trade_id} "
            f"symbol={record['symbol']} "
            f"previous_finalized_quantity={previous_finalized_quantity} "
            f"updated_finalized_quantity={record.get('realized_entry_quantity')}"
        )

        child_snapshot = self.get_timeout_retained_child_snapshot(
            dict(record),
            float(record.get("realized_entry_quantity") or 0.0)
        )
        logger.warning(
            "LATE PARENT FILL CHILD PRECHECK | "
            f"trade_id={trade_id} "
            f"symbol={record['symbol']} "
            f"updated_finalized_quantity={record.get('realized_entry_quantity')} "
            f"coherent={child_snapshot.get('coherent')} "
            f"reason={child_snapshot.get('reason')}"
        )

        reconcile_ok = self.reconcile_timeout_retained_child_protection(trade_id)
        if not reconcile_ok:
            logger.error(
                "LATE PARENT FILL RETAINED PROTECTION FAILED | "
                f"trade_id={trade_id} "
                f"symbol={record['symbol']} "
                f"updated_finalized_quantity={record.get('realized_entry_quantity')} "
                "decision=fail_closed_incomplete"
            )
            return False

        logger.warning(
            "LATE PARENT FILL RETAINED PROTECTION RESULT | "
            f"trade_id={trade_id} "
            f"symbol={record['symbol']} "
            f"updated_finalized_quantity={record.get('realized_entry_quantity')} "
            "decision=continue_protected"
        )
        return True

    def promote_partial_entry_to_live_quantity(self, trade_id, record, reason_label):
        previously_entry_filled = record.get("entry_filled", False)
        realized_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        previous_state = record.get("state")

        if realized_quantity <= 0:
            self.append_anomaly(trade_id, "PARTIAL_ENTRY_PROMOTION_WITHOUT_REALIZED_QTY")
            return False

        record["realized_entry_quantity"] = realized_quantity
        record["planned_position_size"] = realized_quantity
        record["position_size"] = realized_quantity
        record["entry_filled"] = True
        if record.get("state") in {"ENTRY_WORKING", "BROKER_ACK_PENDING", "SUBMITTING", "CANCELLED", "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL"}:
            record["state"] = "ENTRY_FILLED"
        self.clear_partial_entry_timeout(record)

        if not previously_entry_filled:
            self.aggregate_stats["filled_trades"] += 1
            self.get_symbol_stats_bucket(record["symbol"])["filled_trades"] += 1
            if self.is_shadow_test_trade(record):
                self.aggregate_stats["shadow_test_entry_filled_count"] += 1

        self.set_execution_validation_status(trade_id, "entry_filled", reason_label)
        self.append_trade_event(
            trade_id,
            f"REALIZED ENTRY QUANTITY RETAINED reason={reason_label} "
            f"previous_state={previous_state} "
            f"realized_entry_quantity={record['realized_entry_quantity']} "
            f"partial_entry_timeout_triggered={record.get('partial_entry_timeout_triggered')} "
            f"partial_entry_remainder_cancelled={record.get('partial_entry_remainder_cancelled')} "
            f"resulting_state={record['state']}"
        )
        logger.warning(
            "REALIZED ENTRY QUANTITY RETAINED | "
            f"trade_id={trade_id} "
            f"symbol={record['symbol']} "
            f"reason={reason_label} "
            f"previous_state={previous_state} "
            f"realized_entry_quantity={record['realized_entry_quantity']} "
            f"partial_entry_timeout_triggered={record.get('partial_entry_timeout_triggered')} "
            f"partial_entry_remainder_cancelled={record.get('partial_entry_remainder_cancelled')} "
            f"resulting_state={record['state']}"
        )
        return True

    def is_entry_fully_filled(self, record):
        intended_parent_quantity = self.get_original_parent_quantity(record)
        cumulative_entry_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        return intended_parent_quantity > 0 and cumulative_entry_quantity >= intended_parent_quantity

    def get_lifecycle_entry_quantity(self, record):
        realized_entry_quantity = record.get("realized_entry_quantity")
        if realized_entry_quantity is not None:
            return float(realized_entry_quantity or 0.0)
        return float(record.get("cumulative_entry_quantity") or 0.0)

    def is_exit_fully_filled(self, record):
        if not record.get("entry_filled") and float(record.get("cumulative_entry_quantity") or 0.0) <= 0:
            return False
        realized_entry_quantity = self.get_lifecycle_entry_quantity(record)
        cumulative_exit_quantity = float(record.get("cumulative_exit_quantity") or 0.0)
        return realized_entry_quantity > 0 and cumulative_exit_quantity >= realized_entry_quantity

    def derive_completed_exit_reason(self, record):
        tp_exit_quantity = float(record.get("tp_exit_quantity") or 0.0)
        sl_exit_quantity = float(record.get("sl_exit_quantity") or 0.0)

        if tp_exit_quantity > 0 and sl_exit_quantity > 0:
            self.append_anomaly(record["trade_id"], "MIXED_EXIT_CHILD_FILLS")
            return "MIXED_EXIT"
        if tp_exit_quantity > 0:
            return "TP"
        if sl_exit_quantity > 0:
            return "SL"
        return None

    def get_effective_trade_quantity(self, record):
        realized_entry_quantity = record.get("realized_entry_quantity")
        realized_exit_quantity = record.get("realized_exit_quantity")
        planned_position_size = float(record.get("planned_position_size") or 0.0)

        if realized_entry_quantity is not None and realized_exit_quantity is not None:
            return min(realized_entry_quantity, realized_exit_quantity)
        if realized_entry_quantity is not None:
            return realized_entry_quantity
        if realized_exit_quantity is not None:
            return realized_exit_quantity
        return planned_position_size

    def finalize_trade_if_complete(self, trade_id):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return
            if record["summary_logged"]:
                logger.info(
                    "FINALIZE SKIPPED | "
                    f"trade_id={trade_id} reason=already_finalized "
                    f"state={record['state']} closed={record['closed']}"
                )
                return

            ready = False

            if self.is_exit_fully_filled(record) and record["exit_fill_price"] is not None:
                if record["realized_entry_quantity"] is None:
                    record["realized_entry_quantity"] = self.get_lifecycle_entry_quantity(record)
                record["exit_reason"] = self.derive_completed_exit_reason(record)
                ready = record["exit_reason"] in ("TP", "SL", "MIXED_EXIT")
            elif record["entry_filled"] and record["exit_fill_price"] is not None and record["exit_reason"] in ("TP", "SL", "MIXED_EXIT"):
                ready = True
            elif record["state"] == "INCOMPLETE":
                ready = True
            elif not record["entry_filled"] and record["state"] in ("CANCELLED", "REJECTED"):
                ready = True

            if not ready:
                return

            broker_reality = self.get_trade_broker_reality(record)
            release_override = self.should_release_fail_closed_incomplete_on_symbol_ambiguity(
                record,
                broker_reality,
            )
            if broker_reality["broker_real"]:
                if release_override:
                    self.append_trade_event(
                        trade_id,
                        "SYMBOL RELEASE OVERRIDE broker_real=true "
                        "reason=fail_closed_incomplete_with_symbol_level_broker_ambiguity_only"
                    )
                    logger.warning(
                        "SYMBOL RELEASE DECISION | "
                        f"trade_id={trade_id} "
                        f"symbol={record['symbol']} "
                        f"state={record['state']} "
                        "decision=release "
                        "reason=fail_closed_incomplete_with_symbol_level_broker_ambiguity_only "
                        f"open_trade_match={broker_reality['has_open_trade_match']} "
                        f"direct_open_trade_match={broker_reality['has_direct_open_trade_match']} "
                        f"symbol_open_trade_match={broker_reality['has_symbol_open_trade_match']} "
                        f"open_order_match={broker_reality['has_open_order_match']} "
                        f"direct_broker_match={broker_reality['has_direct_broker_match']} "
                        f"position_match={broker_reality['has_position_match']} "
                        f"open_trade_order_ids={broker_reality['matching_open_trade_order_ids']} "
                        f"open_order_ids={broker_reality['matching_open_order_ids']} "
                        f"position_sizes={broker_reality['matching_position_sizes']}"
                    )
                else:
                    self.append_trade_event(
                        trade_id,
                        f"SYMBOL RELEASE BLOCKED broker_real={broker_reality['broker_real']} "
                        f"open_trade_match={broker_reality['has_open_trade_match']} "
                        f"open_order_match={broker_reality['has_open_order_match']} "
                        f"position_match={broker_reality['has_position_match']}"
                    )
                    logger.info(
                        "SYMBOL RELEASE DECISION | "
                        f"trade_id={trade_id} "
                        f"symbol={record['symbol']} "
                        f"state={record['state']} "
                        "decision=hold "
                        "reason=broker_side_activity_remains "
                        f"open_trade_match={broker_reality['has_open_trade_match']} "
                        f"direct_open_trade_match={broker_reality['has_direct_open_trade_match']} "
                        f"symbol_open_trade_match={broker_reality['has_symbol_open_trade_match']} "
                        f"open_order_match={broker_reality['has_open_order_match']} "
                        f"direct_broker_match={broker_reality['has_direct_broker_match']} "
                        f"position_match={broker_reality['has_position_match']} "
                        f"open_trade_order_ids={broker_reality['matching_open_trade_order_ids']} "
                        f"open_order_ids={broker_reality['matching_open_order_ids']} "
                        f"position_sizes={broker_reality['matching_position_sizes']}"
                    )
                    return

            self.append_trade_event(
                trade_id,
                f"SYMBOL RELEASE ALLOWED broker_real={broker_reality['broker_real']} "
                f"lifecycle_ready={ready}"
            )
            logger.info(
                "SYMBOL RELEASE DECISION | "
                f"trade_id={trade_id} "
                f"symbol={record['symbol']} "
                f"state={record['state']} "
                "decision=release "
                f"reason={'fail_closed_incomplete_with_symbol_level_broker_ambiguity_only' if release_override else 'no_broker_activity_and_lifecycle_ready'} "
                f"open_trade_match={broker_reality['has_open_trade_match']} "
                f"direct_open_trade_match={broker_reality['has_direct_open_trade_match']} "
                f"symbol_open_trade_match={broker_reality['has_symbol_open_trade_match']} "
                f"open_order_match={broker_reality['has_open_order_match']} "
                f"direct_broker_match={broker_reality['has_direct_broker_match']} "
                f"position_match={broker_reality['has_position_match']}"
            )

            exit_complete = self.is_exit_fully_filled(record) and record["exit_fill_price"] is not None

            if exit_complete:
                if not record["entry_filled"]:
                    self.append_anomaly(trade_id, "ENTRY_PARTIAL_LIFECYCLE_CLOSED")
                self.set_execution_validation_status(
                    trade_id,
                    "exit_complete",
                    f"exit_reason={record.get('exit_reason')}"
                )
                self.log_exit_cleanup_visibility(trade_id, record, "FINALIZE_EXIT_COMPLETE")
                record["closed"] = True
                record["state"] = "CLOSED"
                record["gross_pnl"] = self.calculate_gross_pnl(record)
                record["net_pnl"] = round(record["gross_pnl"] - record["commission"], 2)
                self.aggregate_stats["closed_trades"] += 1
                self.aggregate_stats["gross_pnl"] = round(self.aggregate_stats["gross_pnl"] + record["gross_pnl"], 2)
                self.aggregate_stats["commission"] = round(self.aggregate_stats["commission"] + record["commission"], 2)
                self.aggregate_stats["net_pnl"] = round(self.aggregate_stats["net_pnl"] + record["net_pnl"], 2)
                if self.is_shadow_test_trade(record):
                    self.adjust_shadow_test_net_pnl(record["net_pnl"])
                symbol_bucket = self.get_symbol_stats_bucket(record["symbol"])
                symbol_bucket["gross_pnl"] = round(symbol_bucket["gross_pnl"] + record["gross_pnl"], 2)
                symbol_bucket["commission"] = round(symbol_bucket["commission"] + record["commission"], 2)
                symbol_bucket["net_pnl"] = round(symbol_bucket["net_pnl"] + record["net_pnl"], 2)
                if record["exit_reason"] == "TP":
                    self.aggregate_stats["tp_count"] += 1
                    symbol_bucket["tp_count"] += 1
                    if self.is_shadow_test_trade(record):
                        self.aggregate_stats["shadow_test_tp_count"] += 1
                elif record["exit_reason"] == "SL":
                    self.aggregate_stats["sl_count"] += 1
                    symbol_bucket["sl_count"] += 1
                    if self.is_shadow_test_trade(record):
                        self.aggregate_stats["shadow_test_sl_count"] += 1
                    if record.get("bot_stage") == "LIVE":
                        self.activate_live_daily_sl_stop(
                            record["trade_id"],
                            record["exit_fill_time"]
                        )
                elif record["exit_reason"] == "MIXED_EXIT":
                    self.aggregate_stats["mixed_exit_count"] += 1
            else:
                if record["entry_filled"] and not exit_complete:
                    self.append_anomaly(trade_id, "INCOMPLETE_ENTRY_WITHOUT_FULL_EXIT")
                    self.set_execution_validation_status(
                        trade_id,
                        "validation_incomplete",
                        "entry_filled_without_full_exit"
                    )
                    record["state"] = "INCOMPLETE"
                if record["state"] == "INCOMPLETE":
                    self.log_exit_cleanup_visibility(trade_id, record, "FINALIZE_INCOMPLETE")
                    self.aggregate_stats["incomplete_count"] += 1
                    if self.is_shadow_test_trade(record):
                        self.aggregate_stats["shadow_test_incomplete_count"] += 1

            record["summary_logged"] = True
            self.completed_trade_ids.add(trade_id)

            if self.active_execution_trade_id == trade_id:
                self.active_execution_trade_id = None

            duration_to_fill = self.seconds_between(record["execution_start_time"], record["entry_fill_time"])
            duration_in_trade = self.seconds_between(record["entry_fill_time"], record["exit_fill_time"])
            execution_metrics = self.calculate_execution_quality_metrics(record)
            timing_metrics = self.build_trade_timing_metrics(record)
            symbol_snapshot = self.build_symbol_stats_snapshot()

            logger.info(
                "TRADE SUMMARY | "
                f"trade_id={record['trade_id']} "
                f"stage={record['bot_stage']} "
                f"symbol={record['symbol']} "
                f"side={record['side']} "
                f"grade={record['grade']} "
                f"execution_lane={record.get('execution_lane')} "
                f"promoted_from_shadow={record.get('promoted_from_shadow')} "
                f"shadow_override_reason={record.get('shadow_override_reason')} "
                f"truth_classification={record.get('truth_classification')} "
                f"det_classification={record['det_classification']} "
                f"primary_reason={record.get('primary_reason')} "
                f"execution_validation_status={record.get('execution_validation_status')} "
                f"planned_position_size={record['planned_position_size']} "
                f"original_intended_parent_quantity={record.get('original_intended_parent_quantity')} "
                f"intended_parent_quantity={record['intended_parent_quantity']} "
                f"cumulative_entry_quantity={record['cumulative_entry_quantity']} "
                f"cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                f"realized_entry_quantity={record['realized_entry_quantity']} "
                f"realized_exit_quantity={record['realized_exit_quantity']} "
                f"allowed_money_risk={record['allowed_money_risk']} "
                f"stop_distance_points={record['stop_distance_points']} "
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
                f"webhook_to_classification_sec={timing_metrics['webhook_to_classification_sec']} "
                f"classification_to_queue_sec={timing_metrics['classification_to_queue_sec']} "
                f"queue_wait_sec={timing_metrics['queue_wait_sec']} "
                f"pickup_to_preflight_sec={timing_metrics['pickup_to_preflight_sec']} "
                f"preflight_to_submit_sec={timing_metrics['preflight_to_submit_sec']} "
                f"submit_to_broker_ack_sec={timing_metrics['submit_to_broker_ack_sec']} "
                f"submit_to_entry_fill_sec={timing_metrics['submit_to_entry_fill_sec']} "
                f"entry_to_exit_sec={timing_metrics['entry_to_exit_sec']} "
                f"total_trade_lifecycle_sec={timing_metrics['total_trade_lifecycle_sec']} "
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
                f"fill_latency_sec={duration_to_fill} "
                f"time_in_trade_sec={duration_in_trade} "
                f"execution_validation_status={record.get('execution_validation_status')}"
            )

            logger.info(
                "AGGREGATE STATS | "
                f"stage={BOT_STAGE} "
                f"total_trades={self.aggregate_stats['total_trades']} "
                f"closed_trades={self.aggregate_stats['closed_trades']} "
                f"filled_trades={self.aggregate_stats['filled_trades']} "
                f"tp_count={self.aggregate_stats['tp_count']} "
                f"sl_count={self.aggregate_stats['sl_count']} "
                f"mixed_exit_count={self.aggregate_stats['mixed_exit_count']} "
                f"cancelled_count={self.aggregate_stats['cancelled_count']} "
                f"rejected_count={self.aggregate_stats['rejected_count']} "
                f"incomplete_count={self.aggregate_stats['incomplete_count']} "
                f"diagnostic_reject_count={self.aggregate_stats['diagnostic_reject_count']} "
                f"diagnostic_shadow_count={self.aggregate_stats['diagnostic_shadow_count']} "
                f"diagnostic_execute_a_count={self.aggregate_stats['diagnostic_execute_a_count']} "
                f"diagnostic_execute_a_plus_count={self.aggregate_stats['diagnostic_execute_a_plus_count']} "
                f"execution_reject_count={self.aggregate_stats['execution_reject_count']} "
                f"execution_shadow_count={self.aggregate_stats['execution_shadow_count']} "
                f"execution_execute_a_count={self.aggregate_stats['execution_execute_a_count']} "
                f"execution_execute_a_plus_count={self.aggregate_stats['execution_execute_a_plus_count']} "
                f"queued_count={self.aggregate_stats['queued_count']} "
                f"shadow_test_queued_count={self.aggregate_stats['shadow_test_queued_count']} "
                f"submitted_count={self.aggregate_stats['submitted_count']} "
                f"shadow_test_submitted_count={self.aggregate_stats['shadow_test_submitted_count']} "
                f"broker_acknowledged_count={self.aggregate_stats['broker_acknowledged_count']} "
                f"broker_live_count={self.aggregate_stats['broker_live_count']} "
                f"shadow_test_entry_filled_count={self.aggregate_stats['shadow_test_entry_filled_count']} "
                f"shadow_test_tp_count={self.aggregate_stats['shadow_test_tp_count']} "
                f"shadow_test_sl_count={self.aggregate_stats['shadow_test_sl_count']} "
                f"shadow_test_incomplete_count={self.aggregate_stats['shadow_test_incomplete_count']} "
                f"gross_pnl={self.aggregate_stats['gross_pnl']} "
                f"commission={self.aggregate_stats['commission']} "
                f"net_pnl={self.aggregate_stats['net_pnl']} "
                f"shadow_test_net_pnl={self.aggregate_stats['shadow_test_net_pnl']} "
                f"symbol_snapshot={json.dumps(symbol_snapshot, sort_keys=True)}"
            )

    def update_trade_from_fill(self, fill):
        try:
            execution = fill.execution
            order_id = getattr(execution, "orderId", None)
            price = getattr(execution, "price", None)
            fill_time = getattr(fill, "time", None)
            realized_quantity = self.normalize_fill_quantity(getattr(execution, "shares", None))

            trade_id, record = self.get_trade_by_order_id(order_id)
            if record is None:
                return

            should_process, execution_identity = self.should_process_execution(execution, fill_time)
            if not should_process:
                self.append_trade_event(
                    trade_id,
                    f"DUPLICATE EXECUTION IGNORED execution_identity={execution_identity} orderId={order_id}"
                )
                logger.info(
                    "DUPLICATE EXECUTION IGNORED | "
                    f"trade_id={trade_id} execution_identity={execution_identity} order_id={order_id}"
                )
                return

            self.append_trade_event(
                trade_id,
                f"FILL execution_identity={execution_identity} orderId={order_id} "
                f"side={getattr(execution, 'side', None)} shares={getattr(execution, 'shares', None)} "
                f"normalized_quantity={realized_quantity} price={price}"
            )

            if order_id == record["parent_order_id"]:
                late_fill_pending = False
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is None:
                        return

                    record["entry_fill_time"] = fill_time
                    previously_entry_filled = record["entry_filled"]
                    previous_finalized_quantity = float(record.get("realized_entry_quantity") or 0.0)
                    self.accumulate_quantity_and_notional(
                        record,
                        "cumulative_entry_quantity",
                        "cumulative_entry_notional",
                        "entry_fill_price",
                        realized_quantity,
                        price
                    )
                    if previous_finalized_quantity > 0:
                        late_fill_pending = True
                    elif self.is_partial_timeout_parent_finality_owner_active(record):
                        timeout_snapshot_quantity = self.get_timeout_snapshot_quantity(record)
                        current_cumulative_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
                        original_parent_quantity = self.get_original_parent_quantity(record)
                        if (
                            timeout_snapshot_quantity is not None and
                            current_cumulative_quantity > timeout_snapshot_quantity + 1e-9
                        ):
                            record["partial_entry_parent_finality_quantity_drift_detected"] = True
                            record["partial_entry_parent_finality_drift_quantity"] = current_cumulative_quantity
                            self.append_trade_event(
                                trade_id,
                                f"PARENT FINALITY OWNERSHIP QUANTITY DRIFT OBSERVED "
                                f"timeout_snapshot_quantity={timeout_snapshot_quantity} "
                                f"cumulative_quantity={current_cumulative_quantity} "
                                f"original_parent_quantity={original_parent_quantity}"
                            )
                            logger.warning(
                                "PARENT_FINALITY_OWNERSHIP_QUANTITY_DRIFT_OBSERVED | "
                                f"trade_id={trade_id} "
                                f"symbol={record['symbol']} "
                                f"timeout_snapshot_quantity={timeout_snapshot_quantity} "
                                f"cumulative_quantity={current_cumulative_quantity} "
                                f"original_parent_quantity={original_parent_quantity}"
                            )
                        record["partial_entry_parent_finality_last_check"] = datetime.now(timezone.utc)
                        self.append_trade_event(
                            trade_id,
                            f"PARENT FINALITY OWNERSHIP ACTIVE cumulative_quantity={record['cumulative_entry_quantity']} "
                            f"timeout_snapshot_quantity={record.get('partial_entry_timeout_snapshot_quantity')} "
                            f"candidate_status={record.get('parent_last_status')} "
                            "decision=preserve_pending_parent_finality"
                        )
                        logger.warning(
                            "PARENT_FINALITY_OWNERSHIP_ACTIVE | "
                            f"trade_id={trade_id} "
                            f"symbol={record['symbol']} "
                            f"cumulative_quantity={record['cumulative_entry_quantity']} "
                            f"timeout_snapshot_quantity={record.get('partial_entry_timeout_snapshot_quantity')} "
                            f"parent_last_status={record.get('parent_last_status')} "
                            "decision=preserve_pending_parent_finality"
                        )
                    elif self.is_entry_fully_filled(record):
                        self.promote_partial_entry_to_live_quantity(
                            trade_id,
                            record,
                            "parent_entry_fully_filled"
                        )
                        if not previously_entry_filled:
                            self.append_trade_event(
                                trade_id,
                                f"ENTRY FULLY FILLED quantity={record['realized_entry_quantity']} "
                                f"avg_fill={record['entry_fill_price']}"
                            )
                            logger.info(
                                "ENTRY FULLY FILLED | "
                                f"trade_id={trade_id} quantity={record['realized_entry_quantity']} "
                                f"original_parent_quantity={record.get('original_intended_parent_quantity')} "
                                f"entry_fill_price={record['entry_fill_price']}"
                            )
                    else:
                        record["state"] = "ENTRY_WORKING"
                        self.start_partial_entry_timeout(trade_id, record, fill_time)
                        self.append_trade_event(
                            trade_id,
                            f"ENTRY PARTIAL FILL cumulative_quantity={record['cumulative_entry_quantity']} "
                            f"original_parent_quantity={record.get('original_intended_parent_quantity')} "
                            f"avg_fill={record['entry_fill_price']}"
                        )
                        logger.info(
                            "ENTRY PARTIAL FILL | "
                            f"trade_id={trade_id} cumulative_quantity={record['cumulative_entry_quantity']} "
                            f"original_parent_quantity={record.get('original_intended_parent_quantity')} "
                            f"entry_fill_price={record['entry_fill_price']}"
                        )

                if late_fill_pending:
                    with self.trade_analysis_lock:
                        record = self.trade_analysis.get(trade_id)
                        if record is None:
                            return
                    late_fill_ok = self.handle_late_parent_fill_after_retained_finalization(
                        trade_id,
                        record,
                        previous_finalized_quantity
                    )
                    if not late_fill_ok:
                        self.finalize_trade_if_complete(trade_id)
                        return
            elif order_id == record["tp_order_id"]:
                record["exit_fill_time"] = fill_time
                self.accumulate_quantity_and_notional(
                    record,
                    "cumulative_exit_quantity",
                    "cumulative_exit_notional",
                    "exit_fill_price",
                    realized_quantity,
                    price
                )
                self.accumulate_quantity_and_notional(
                    record,
                    "tp_exit_quantity",
                    "tp_exit_notional",
                    "tp_exit_fill_price",
                    realized_quantity,
                    price
                )
                if self.is_exit_fully_filled(record):
                    if record["realized_entry_quantity"] is None:
                        record["realized_entry_quantity"] = self.get_lifecycle_entry_quantity(record)
                    record["realized_exit_quantity"] = float(record.get("cumulative_exit_quantity") or 0.0)
                    record["exit_reason"] = self.derive_completed_exit_reason(record)
                    record["state"] = "TP_FILLED" if record["exit_reason"] == "TP" else "CLOSED"
                    self.append_trade_event(
                        trade_id,
                        f"EXIT FULLY FILLED exit_reason={record['exit_reason']} "
                        f"cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                        f"tp_exit_quantity={record['tp_exit_quantity']} sl_exit_quantity={record['sl_exit_quantity']} "
                        f"exit_fill={record['exit_fill_price']}"
                    )
                    self.log_exit_cleanup_visibility(trade_id, record, "TP_EXIT_FILLED")
                    logger.info(
                        "EXIT FULLY FILLED | "
                        f"trade_id={trade_id} exit_reason={record['exit_reason']} "
                        f"cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={record['realized_entry_quantity']} "
                        f"exit_fill_price={record['exit_fill_price']}"
                    )
                else:
                    record["state"] = "EXIT_WORKING"
                    self.append_trade_event(
                        trade_id,
                        f"EXIT PARTIAL FILL cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                        f"tp_exit_quantity={record['tp_exit_quantity']} sl_exit_quantity={record['sl_exit_quantity']} "
                        f"exit_fill={record['exit_fill_price']}"
                    )
                    logger.info(
                        "EXIT PARTIAL FILL | "
                        f"trade_id={trade_id} cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                        f"tp_exit_quantity={record['tp_exit_quantity']} sl_exit_quantity={record['sl_exit_quantity']} "
                        f"exit_fill_price={record['exit_fill_price']}"
                    )
            elif order_id == record["sl_order_id"]:
                record["exit_fill_time"] = fill_time
                self.accumulate_quantity_and_notional(
                    record,
                    "cumulative_exit_quantity",
                    "cumulative_exit_notional",
                    "exit_fill_price",
                    realized_quantity,
                    price
                )
                self.accumulate_quantity_and_notional(
                    record,
                    "sl_exit_quantity",
                    "sl_exit_notional",
                    "sl_exit_fill_price",
                    realized_quantity,
                    price
                )
                if self.is_exit_fully_filled(record):
                    if record["realized_entry_quantity"] is None:
                        record["realized_entry_quantity"] = self.get_lifecycle_entry_quantity(record)
                    record["realized_exit_quantity"] = float(record.get("cumulative_exit_quantity") or 0.0)
                    record["exit_reason"] = self.derive_completed_exit_reason(record)
                    record["state"] = "SL_FILLED" if record["exit_reason"] == "SL" else "CLOSED"
                    self.append_trade_event(
                        trade_id,
                        f"EXIT FULLY FILLED exit_reason={record['exit_reason']} "
                        f"cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                        f"tp_exit_quantity={record['tp_exit_quantity']} sl_exit_quantity={record['sl_exit_quantity']} "
                        f"exit_fill={record['exit_fill_price']}"
                    )
                    self.log_exit_cleanup_visibility(trade_id, record, "SL_EXIT_FILLED")
                    logger.info(
                        "EXIT FULLY FILLED | "
                        f"trade_id={trade_id} exit_reason={record['exit_reason']} "
                        f"cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={record['realized_entry_quantity']} "
                        f"exit_fill_price={record['exit_fill_price']}"
                    )
                else:
                    record["state"] = "EXIT_WORKING"
                    self.append_trade_event(
                        trade_id,
                        f"EXIT PARTIAL FILL cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                        f"tp_exit_quantity={record['tp_exit_quantity']} sl_exit_quantity={record['sl_exit_quantity']} "
                        f"exit_fill={record['exit_fill_price']}"
                    )
                    logger.info(
                        "EXIT PARTIAL FILL | "
                        f"trade_id={trade_id} cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                        f"tp_exit_quantity={record['tp_exit_quantity']} sl_exit_quantity={record['sl_exit_quantity']} "
                        f"exit_fill_price={record['exit_fill_price']}"
                    )

            if record.get("exit_reason") == "MIXED_EXIT":
                self.append_trade_event(
                    trade_id,
                    f"MIXED EXIT DETECTED tp_exit_quantity={record['tp_exit_quantity']} "
                    f"sl_exit_quantity={record['sl_exit_quantity']}"
                )
                logger.warning(
                    "MIXED EXIT DETECTED | "
                    f"trade_id={trade_id} tp_exit_quantity={record['tp_exit_quantity']} "
                    f"sl_exit_quantity={record['sl_exit_quantity']}"
                )

            self.finalize_trade_if_complete(trade_id)
        except Exception:
            logger.exception("TRADE FILL ANALYSIS FAILED")

    def update_trade_commission(self, trade, fill, report=None):
        try:
            order_id = getattr(fill.execution, "orderId", None)
            if report is None:
                report = getattr(fill, "commissionReport", None)
            if report is None:
                return
            trade_id, record = self.get_trade_by_order_id(order_id)
            if record is None:
                return

            should_process, commission_identity = self.should_process_commission(order_id, report)
            if not should_process:
                self.append_trade_event(
                    trade_id,
                    f"DUPLICATE COMMISSION IGNORED orderId={order_id} "
                    f"commission_identity={commission_identity}"
                )
                logger.info(
                    "DUPLICATE COMMISSION IGNORED | "
                    f"trade_id={trade_id} order_id={order_id} commission_identity={commission_identity}"
                )
                return

            commission = float(getattr(report, "commission", 0.0) or 0.0)

            record["commission"] = round(record["commission"] + commission, 2)
            self.append_trade_event(
                trade_id,
                f"COMMISSION orderId={order_id} commission={commission} "
                f"commission_identity={commission_identity}"
            )

            if record["summary_logged"]:
                previous_net_pnl = record["net_pnl"]
                if record["closed"]:
                    record["net_pnl"] = round(record["gross_pnl"] - record["commission"], 2)
                    self.aggregate_stats["commission"] = round(self.aggregate_stats["commission"] + commission, 2)
                    self.aggregate_stats["net_pnl"] = round(
                        self.aggregate_stats["net_pnl"] + (record["net_pnl"] - previous_net_pnl),
                        2
                    )
                    if self.is_shadow_test_trade(record):
                        self.adjust_shadow_test_net_pnl(record["net_pnl"] - previous_net_pnl)
                    symbol_bucket = self.get_symbol_stats_bucket(record["symbol"])
                    symbol_bucket["commission"] = round(symbol_bucket["commission"] + commission, 2)
                    symbol_bucket["net_pnl"] = round(
                        symbol_bucket["net_pnl"] + (record["net_pnl"] - previous_net_pnl),
                        2
                    )
                    self.append_trade_event(
                        trade_id,
                        f"LATE COMMISSION APPLIED commission={commission} "
                        f"net_pnl={record['net_pnl']}"
                    )
                    logger.info(
                        "LATE COMMISSION APPLIED | "
                        f"trade_id={trade_id} commission={commission} "
                        f"commission_identity={commission_identity} net_pnl={record['net_pnl']}"
                    )
                else:
                    self.append_trade_event(
                        trade_id,
                        f"LATE COMMISSION RECORDED commission={commission}"
                    )
                    logger.info(
                        "LATE COMMISSION RECORDED | "
                        f"trade_id={trade_id} commission={commission} "
                        f"commission_identity={commission_identity} closed={record['closed']}"
                    )
                return

            self.finalize_trade_if_complete(trade_id)
        except Exception:
            logger.exception("TRADE COMMISSION ANALYSIS FAILED")

    def cancel_partial_entry_remainder(self, trade_id, record_snapshot):
        parent_order_id = record_snapshot.get("parent_order_id")
        if parent_order_id is None:
            return False, "missing_parent_order_id"

        try:
            parent_trade = None
            parent_order = None

            for trade in self.ib.openTrades():
                order = getattr(trade, "order", None)
                if getattr(order, "orderId", None) == parent_order_id:
                    parent_trade = trade
                    break

            if parent_trade is not None:
                self.ib.cancelOrder(parent_trade.order)
                return True, "cancelled_via_open_trade"

            for order in self.ib.openOrders():
                if getattr(order, "orderId", None) == parent_order_id:
                    parent_order = order
                    break

            if parent_order is not None:
                self.ib.cancelOrder(parent_order)
                return True, "cancelled_via_open_order"
        except Exception as exc:
            logger.exception(
                "PARTIAL ENTRY REMAINDER CANCEL FAILED | "
                f"trade_id={trade_id} parent_order_id={parent_order_id}"
            )
            return False, f"cancel_exception:{exc}"

        return False, "parent_order_not_visible_for_cancel"

    def process_partial_entry_timeouts(self):
        now_dt = datetime.now(timezone.utc)
        with self.trade_analysis_lock:
            timeout_candidates = [
                {
                    "trade_id": record["trade_id"],
                    "symbol": record["symbol"],
                    "state": record["state"],
                    "entry_filled": record["entry_filled"],
                    "summary_logged": record["summary_logged"],
                    "cumulative_entry_quantity": float(record.get("cumulative_entry_quantity") or 0.0),
                    "intended_parent_quantity": float(record.get("intended_parent_quantity") or 0.0),
                    "partial_entry_timeout_started_at": record.get("partial_entry_timeout_started_at"),
                    "partial_entry_timeout_deadline": record.get("partial_entry_timeout_deadline"),
                    "partial_entry_timeout_triggered": record.get("partial_entry_timeout_triggered", False),
                    "partial_entry_remainder_cancelled": record.get("partial_entry_remainder_cancelled", False),
                    "parent_order_id": record.get("parent_order_id"),
                    "tp_order_id": record.get("tp_order_id"),
                    "sl_order_id": record.get("sl_order_id"),
                    "parent_perm_id": record.get("parent_perm_id"),
                    "tp_perm_id": record.get("tp_perm_id"),
                    "sl_perm_id": record.get("sl_perm_id"),
                }
                for record in self.trade_analysis.values()
                if (
                    not record["summary_logged"] and
                    record["state"] == "ENTRY_WORKING" and
                    not record["entry_filled"] and
                    self.has_partial_entry_fill(record)
                )
            ]

        for candidate in timeout_candidates:
            trade_id = candidate["trade_id"]

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if (
                    record is None or
                    record["summary_logged"] or
                    record["state"] != "ENTRY_WORKING" or
                    record["entry_filled"] or
                    not self.has_partial_entry_fill(record)
                ):
                    continue

                if record.get("partial_entry_timeout_started_at") is None:
                    self.start_partial_entry_timeout(trade_id, record)
                    continue

                record["partial_entry_timeout_last_check"] = now_dt
                started_at = record.get("partial_entry_timeout_started_at")
                deadline = record.get("partial_entry_timeout_deadline")
                remaining_quantity = self.get_parent_remaining_quantity(record)
                age_seconds = self.seconds_between(started_at, now_dt)

            logger.info(
                "PARTIAL ENTRY TIMEOUT CHECKED | "
                f"trade_id={trade_id} "
                f"symbol={candidate['symbol']} "
                f"state={candidate['state']} "
                f"age_sec={age_seconds} "
                f"remaining_quantity={remaining_quantity} "
                f"deadline_epoch={round(deadline, 3) if deadline is not None else None}"
            )

            if deadline is None or now_dt.timestamp() < deadline:
                continue

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if (
                    record is None or
                    record["summary_logged"] or
                    record["entry_filled"] or
                    record["state"] != "ENTRY_WORKING"
                ):
                    continue
                remaining_quantity = self.get_parent_remaining_quantity(record)
                realized_quantity = float(record.get("cumulative_entry_quantity") or 0.0)

            logger.warning(
                "PARTIAL ENTRY TIMEOUT TRIGGERED | "
                f"trade_id={trade_id} "
                f"symbol={candidate['symbol']} "
                f"state=ENTRY_WORKING "
                f"realized_quantity={realized_quantity} "
                f"remaining_quantity={remaining_quantity} "
                f"timeout_sec={PARTIAL_ENTRY_TIMEOUT_SECONDS}"
            )

            if realized_quantity <= 0 or remaining_quantity <= 0:
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        self.append_anomaly(trade_id, "PARTIAL_ENTRY_TIMEOUT_STATE_UNCLEAR")
                        record["state"] = "INCOMPLETE"
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    "partial_entry_timeout_unclear_realized_or_remaining_quantity"
                )
                self.finalize_trade_if_complete(trade_id)
                continue

            broker_reality = self.get_trade_broker_reality(candidate)
            if broker_reality["check_failed"]:
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        self.append_anomaly(trade_id, "PARTIAL_ENTRY_TIMEOUT_BROKER_CHECK_FAILED")
                        record["state"] = "INCOMPLETE"
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    "partial_entry_timeout_broker_check_failed"
                )
                self.finalize_trade_if_complete(trade_id)
                continue

            cancel_ok, cancel_reason = self.cancel_partial_entry_remainder(trade_id, candidate)

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if (
                    record is None or
                    record["summary_logged"] or
                    record["entry_filled"] or
                    record["state"] != "ENTRY_WORKING" or
                    not self.has_partial_entry_fill(record)
                ):
                    if record is not None and not record["summary_logged"]:
                        self.append_trade_event(
                            trade_id,
                            f"PARTIAL ENTRY TIMEOUT COMMIT SKIPPED post_cancel_state={record['state']} "
                            f"entry_filled={record['entry_filled']} "
                            f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')}"
                        )
                    continue

                record["partial_entry_timeout_triggered"] = True
                record["partial_entry_timeout_snapshot_quantity"] = realized_quantity
                record["partial_entry_timeout_snapshot_remaining_quantity"] = remaining_quantity
                record["partial_entry_timeout_snapshot_at"] = now_dt
                record["partial_entry_remainder_cancel_requested_at"] = now_dt
                record["partial_entry_remainder_cancel_reason"] = cancel_reason
                self.append_trade_event(
                    trade_id,
                    f"PARTIAL ENTRY REMAINDER CANCEL RESULT cancel_ok={cancel_ok} "
                    f"cancel_reason={cancel_reason}"
                )

                if not cancel_ok:
                    self.append_anomaly(trade_id, "PARTIAL_ENTRY_REMAINDER_CANCEL_FAILED")
                    record["state"] = "INCOMPLETE"
                    self.append_trade_event(
                        trade_id,
                        f"PARTIAL ENTRY TIMEOUT FAIL-CLOSED cancel_reason={cancel_reason} "
                        f"resulting_state={record['state']}"
                    )
                else:
                    record["partial_entry_remainder_cancelled"] = True
                    record["partial_entry_parent_finality_pending_since"] = now_dt
                    record["partial_entry_parent_finality_last_check"] = now_dt
                    record["partial_entry_parent_finality_quantity_drift_detected"] = False
                    record["partial_entry_parent_finality_drift_quantity"] = None
                    record["state"] = "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL"
                    self.append_trade_event(
                        trade_id,
                        f"PARTIAL ENTRY REMAINDER CANCEL REQUESTED cancel_reason={cancel_reason} "
                        f"timeout_snapshot_quantity={realized_quantity} remaining_quantity={remaining_quantity}"
                    )
                    self.append_trade_event(
                        trade_id,
                        f"PARENT FINALITY WAIT ENTERED state={record['state']} "
                        f"timeout_snapshot_quantity={record.get('partial_entry_timeout_snapshot_quantity')} "
                        f"timeout_snapshot_remaining_quantity={record.get('partial_entry_timeout_snapshot_remaining_quantity')}"
                    )
                    logger.warning(
                        "PARTIAL ENTRY REMAINDER CANCEL SUCCEEDED | "
                        f"trade_id={trade_id} "
                        f"symbol={candidate['symbol']} "
                        f"cancel_reason={cancel_reason} "
                        f"timeout_snapshot_quantity={realized_quantity} "
                        f"remaining_quantity={remaining_quantity} "
                        "decision=wait_for_parent_finality"
                    )

            if not cancel_ok:
                logger.error(
                    "PARTIAL ENTRY TIMEOUT FAIL-CLOSED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    f"cancel_reason={cancel_reason} "
                    "decision=fail_closed "
                    "resulting_state=INCOMPLETE"
                )
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    f"partial_entry_timeout_cancel_failed:{cancel_reason}"
                )
                self.finalize_trade_if_complete(trade_id)
                continue

            with self.trade_analysis_lock:
                final_record = self.trade_analysis.get(trade_id)
                final_state = final_record["state"] if final_record is not None else "UNKNOWN"

            logger.warning(
                "PARTIAL ENTRY REMAINDER CANCELLED | "
                f"trade_id={trade_id} "
                f"symbol={candidate['symbol']} "
                f"cancel_reason={cancel_reason} "
                f"timeout_snapshot_quantity={realized_quantity} "
                f"resulting_state={final_state}"
            )

    def process_partial_timeout_parent_finality(self):
        now_dt = datetime.now(timezone.utc)
        with self.trade_analysis_lock:
            pending_candidates = [
                {
                    "trade_id": record["trade_id"],
                    "symbol": record["symbol"],
                    "state": record["state"],
                    "summary_logged": record["summary_logged"],
                    "entry_filled": record["entry_filled"],
                    "parent_order_id": record.get("parent_order_id"),
                    "parent_perm_id": record.get("parent_perm_id"),
                    "parent_last_status": record.get("parent_last_status"),
                    "cumulative_entry_quantity": float(record.get("cumulative_entry_quantity") or 0.0),
                    "intended_parent_quantity": float(record.get("intended_parent_quantity") or 0.0),
                    "timeout_snapshot_quantity": record.get("partial_entry_timeout_snapshot_quantity"),
                    "timeout_snapshot_remaining_quantity": record.get("partial_entry_timeout_snapshot_remaining_quantity"),
                    "partial_entry_remainder_cancel_requested_at": record.get("partial_entry_remainder_cancel_requested_at"),
                }
                for record in self.trade_analysis.values()
                if (
                    not record["summary_logged"] and
                    record["state"] == "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL" and
                    not record["entry_filled"]
                )
            ]

        for candidate in pending_candidates:
            trade_id = candidate["trade_id"]

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is None or record["summary_logged"] or not self.is_parent_finality_pending(record):
                    continue
                record["partial_entry_parent_finality_last_check"] = now_dt
                finality_snapshot = self.get_partial_timeout_parent_finality_snapshot(record)
                record["partial_entry_parent_final_status"] = finality_snapshot.get("parent_status")
                record["partial_entry_parent_final_quantity"] = finality_snapshot.get("cumulative_entry_quantity")
                record["partial_entry_parent_final_remaining_quantity"] = finality_snapshot.get("parent_remaining_quantity")
                record["partial_entry_parent_final_authority_quantity"] = finality_snapshot.get("authority_quantity")
                record["partial_entry_parent_final_authority_source"] = finality_snapshot.get("authority_source")

            logger.warning(
                "PARENT FINALITY CHECK | "
                f"trade_id={trade_id} "
                f"symbol={candidate['symbol']} "
                f"timeout_snapshot_quantity={candidate.get('timeout_snapshot_quantity')} "
                f"parent_status={finality_snapshot.get('parent_status')} "
                f"parent_visible={finality_snapshot.get('parent_visible')} "
                f"parent_remaining_quantity={finality_snapshot.get('parent_remaining_quantity')} "
                f"final_parent_quantity={finality_snapshot.get('cumulative_entry_quantity')} "
                f"authority_quantity={finality_snapshot.get('authority_quantity')} "
                f"authority_source={finality_snapshot.get('authority_source')} "
                f"reason={finality_snapshot.get('reason')}"
            )

            if not finality_snapshot.get("authority_ok"):
                authority_reason = finality_snapshot.get("authority_reason")
                if authority_reason in {
                    "timeout_snapshot_quantity_exceeds_cumulative_entry_quantity",
                    "timeout_snapshot_quantity_drifted_before_finality",
                }:
                    with self.trade_analysis_lock:
                        record = self.trade_analysis.get(trade_id)
                        if record is not None and not record["summary_logged"] and self.is_parent_finality_pending(record):
                            record["partial_entry_parent_finality_quantity_drift_detected"] = True
                            record["partial_entry_parent_finality_drift_quantity"] = float(
                                record.get("cumulative_entry_quantity") or 0.0
                            )
                            self.reset_partial_timeout_parent_finality_candidate(record)
                            self.append_trade_event(
                                trade_id,
                                f"PARENT FINALITY DRIFT PRESERVED authority_reason={authority_reason} "
                                f"timeout_snapshot_quantity={record.get('partial_entry_timeout_snapshot_quantity')} "
                                f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
                                "decision=preserve_pending_parent_finality"
                            )
                    logger.warning(
                        "PARENT FINALITY DRIFT PRESERVED | "
                        f"trade_id={trade_id} "
                        f"symbol={candidate['symbol']} "
                        f"authority_reason={authority_reason} "
                        f"timeout_snapshot_quantity={candidate.get('timeout_snapshot_quantity')} "
                        f"cumulative_entry_quantity={finality_snapshot.get('cumulative_entry_quantity')} "
                        "decision=preserve_pending_parent_finality"
                    )
                    continue
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        self.append_anomaly(trade_id, "PARTIAL_TIMEOUT_PARENT_FINAL_AUTHORITY_AMBIGUOUS")
                        record["state"] = "INCOMPLETE"
                        self.append_trade_event(
                            trade_id,
                            f"PARENT FINALITY FAIL-CLOSED reason={authority_reason} "
                            f"timeout_snapshot_quantity={record.get('partial_entry_timeout_snapshot_quantity')} "
                            f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
                            "resulting_state=INCOMPLETE"
                        )
                logger.error(
                    "PARENT FINALITY FAIL-CLOSED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    f"reason={authority_reason}"
                )
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    authority_reason or "partial_timeout_parent_finality_authority_ambiguous"
                )
                self.finalize_trade_if_complete(trade_id)
                continue

            if not finality_snapshot.get("final"):
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        self.reset_partial_timeout_parent_finality_candidate(record)
                        self.append_trade_event(
                            trade_id,
                            f"PARENT FINALITY CANDIDATE REJECTED reason=not_strong_enough "
                            f"parent_status={finality_snapshot.get('parent_status')} "
                            f"parent_visible={finality_snapshot.get('parent_visible')} "
                            f"candidate_quantity={finality_snapshot.get('cumulative_entry_quantity')} "
                            f"snapshot_reason={finality_snapshot.get('reason')}"
                        )
                logger.warning(
                    "PARENT FINALITY CANDIDATE REJECTED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    "reason=not_strong_enough "
                    f"parent_status={finality_snapshot.get('parent_status')} "
                    f"parent_visible={finality_snapshot.get('parent_visible')} "
                    f"candidate_quantity={finality_snapshot.get('cumulative_entry_quantity')} "
                    f"snapshot_reason={finality_snapshot.get('reason')}"
                )
                continue

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is None or record["summary_logged"] or not self.is_parent_finality_pending(record):
                    continue
                final_parent_quantity = float(finality_snapshot.get("authority_quantity") or 0.0)
                record["partial_entry_parent_final_status"] = finality_snapshot.get("parent_status")
                record["partial_entry_parent_final_quantity"] = float(record.get("cumulative_entry_quantity") or 0.0)
                record["partial_entry_parent_final_remaining_quantity"] = finality_snapshot.get("parent_remaining_quantity")
                record["partial_entry_parent_final_authority_quantity"] = final_parent_quantity
                record["partial_entry_parent_final_authority_source"] = finality_snapshot.get("authority_source")
                previous_candidate_quantity = record.get("partial_entry_parent_finality_candidate_quantity")
                previous_candidate_status = record.get("partial_entry_parent_finality_candidate_status")
                previous_stable_pass_seen = bool(record.get("partial_entry_parent_finality_candidate_stable_pass_seen"))

                logger.warning(
                    "PARENT FINALITY CANDIDATE OBSERVED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    f"candidate_quantity={final_parent_quantity} "
                    f"candidate_status={finality_snapshot.get('parent_status')} "
                    f"previous_candidate_quantity={previous_candidate_quantity} "
                    f"previous_candidate_status={previous_candidate_status} "
                    f"previous_stable_pass_seen={previous_stable_pass_seen} "
                    f"reason={finality_snapshot.get('reason')}"
                )

                if (
                    previous_candidate_quantity == final_parent_quantity
                ):
                    record["partial_entry_parent_finality_candidate_stable_pass_seen"] = True
                    record["partial_entry_parent_finality_candidate_status"] = finality_snapshot.get("parent_status")
                    self.append_trade_event(
                        trade_id,
                        f"PARENT FINALITY STABLE QUANTITY OBSERVED candidate_quantity={final_parent_quantity} "
                        f"previous_candidate_status={previous_candidate_status} "
                        f"current_candidate_status={finality_snapshot.get('parent_status')} "
                        "decision=second_stable_pass_accept"
                    )
                else:
                    record["partial_entry_parent_finality_candidate_quantity"] = final_parent_quantity
                    record["partial_entry_parent_finality_candidate_status"] = finality_snapshot.get("parent_status")
                    record["partial_entry_parent_finality_candidate_stable_pass_seen"] = False
                    self.append_trade_event(
                        trade_id,
                        f"PARENT FINALITY CANDIDATE REJECTED reason=quantity_not_yet_stable_across_two_checks "
                        f"candidate_quantity={final_parent_quantity} "
                        f"candidate_status={finality_snapshot.get('parent_status')} "
                        f"previous_candidate_quantity={previous_candidate_quantity} "
                        f"previous_candidate_status={previous_candidate_status}"
                    )
                    stabilization_ready = False
                if (
                    previous_candidate_quantity == final_parent_quantity
                ):
                    stabilization_ready = True

            if not stabilization_ready:
                logger.warning(
                    "PARENT FINALITY CANDIDATE REJECTED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    "reason=quantity_not_yet_stable_across_two_checks "
                    f"candidate_quantity={final_parent_quantity} "
                    f"candidate_status={finality_snapshot.get('parent_status')} "
                    f"timeout_snapshot_quantity={candidate.get('timeout_snapshot_quantity')}"
                )
                continue

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is None or record["summary_logged"] or not self.is_parent_finality_pending(record):
                    continue
                self.append_trade_event(
                    trade_id,
                    f"PARENT FINALITY ACCEPTED reason=strong_and_stable_second_observation "
                    f"parent_status={finality_snapshot.get('parent_status')} "
                    f"parent_visible={finality_snapshot.get('parent_visible')} "
                    f"parent_final_quantity={final_parent_quantity} "
                    f"parent_remaining_quantity={finality_snapshot.get('parent_remaining_quantity')} "
                    f"timeout_snapshot_quantity={record.get('partial_entry_timeout_snapshot_quantity')} "
                    f"authority_source={finality_snapshot.get('authority_source')} "
                    f"snapshot_reason={finality_snapshot.get('reason')}"
                )
                self.reset_partial_timeout_parent_finality_candidate(record)
                current_cumulative_quantity = float(record.get("cumulative_entry_quantity") or 0.0)

                if final_parent_quantity + 1e-9 < current_cumulative_quantity:
                    record["partial_entry_parent_finality_quantity_drift_detected"] = True
                    record["partial_entry_parent_finality_drift_quantity"] = current_cumulative_quantity
                    self.append_trade_event(
                        trade_id,
                        f"PARENT FINALITY ACCEPTANCE DEFERRED reason=stale_authority_quantity "
                        f"accepted_authority_quantity={final_parent_quantity} "
                        f"current_cumulative_entry_quantity={current_cumulative_quantity} "
                        "decision=preserve_pending_parent_finality"
                    )
                    stale_authority_detected = True
                    fail_reason = None
                elif final_parent_quantity <= 0:
                    self.append_anomaly(trade_id, "PARTIAL_TIMEOUT_PARENT_FINAL_WITHOUT_FILLED_QTY")
                    record["state"] = "INCOMPLETE"
                    fail_reason = "partial_timeout_parent_final_without_filled_quantity"
                    stale_authority_detected = False
                else:
                    stale_authority_detected = False
                    fail_reason = None

            if stale_authority_detected:
                logger.warning(
                    "PARENT FINALITY ACCEPTANCE DEFERRED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    "reason=stale_authority_quantity "
                    f"accepted_authority_quantity={final_parent_quantity} "
                    f"current_cumulative_entry_quantity={current_cumulative_quantity} "
                    "decision=preserve_pending_parent_finality"
                )
                continue

            if fail_reason is not None:
                logger.error(
                    "PARENT FINALITY FAIL-CLOSED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    f"reason={fail_reason}"
                )
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    fail_reason
                )
                self.finalize_trade_if_complete(trade_id)
                continue

            logger.warning(
                "PARENT FINALITY ACCEPTED | "
                f"trade_id={trade_id} "
                f"symbol={candidate['symbol']} "
                "reason=strong_and_stable_second_observation "
                f"parent_status={finality_snapshot.get('parent_status')} "
                f"final_parent_quantity={final_parent_quantity} "
                f"authority_source={finality_snapshot.get('authority_source')} "
                f"parent_remaining_quantity={finality_snapshot.get('parent_remaining_quantity')}"
            )

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is None or record["summary_logged"]:
                    continue
                current_cumulative_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
                if final_parent_quantity + 1e-9 < current_cumulative_quantity:
                    record["partial_entry_parent_finality_quantity_drift_detected"] = True
                    record["partial_entry_parent_finality_drift_quantity"] = current_cumulative_quantity
                    self.append_trade_event(
                        trade_id,
                        f"PARENT FINALITY PROMOTION DEFERRED reason=stale_authority_quantity "
                        f"accepted_authority_quantity={final_parent_quantity} "
                        f"current_cumulative_entry_quantity={current_cumulative_quantity} "
                        "decision=preserve_pending_parent_finality"
                    )
                    promoted = False
                    promotion_deferred = True
                else:
                    promoted = self.promote_partial_entry_to_live_quantity(
                        trade_id,
                        record,
                        f"partial_timeout_parent_final:{finality_snapshot.get('reason')}"
                    )
                    promotion_deferred = False

            if promotion_deferred:
                logger.warning(
                    "PARENT FINALITY PROMOTION DEFERRED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    "reason=stale_authority_quantity "
                    f"accepted_authority_quantity={final_parent_quantity} "
                    f"current_cumulative_entry_quantity={current_cumulative_quantity} "
                    "decision=preserve_pending_parent_finality"
                )
                continue

            if not promoted:
                logger.error(
                    "PARENT FINALITY PROMOTION FAILED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    f"reason={finality_snapshot.get('reason')}"
                )
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        self.append_anomaly(trade_id, "PARTIAL_TIMEOUT_PARENT_FINAL_PROMOTION_FAILED")
                        record["state"] = "INCOMPLETE"
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    "partial_timeout_parent_final_promotion_failed"
                )
                self.finalize_trade_if_complete(trade_id)
                continue

            with self.trade_analysis_lock:
                live_record = self.trade_analysis.get(trade_id)
                if live_record is None or live_record["summary_logged"]:
                    continue
                final_parent_quantity = float(live_record.get("realized_entry_quantity") or 0.0)
                record_snapshot = dict(live_record)
                self.append_trade_event(
                    trade_id,
                    f"FINALIZED RETAINED QUANTITY quantity={final_parent_quantity} "
                    f"timeout_snapshot_quantity={live_record.get('partial_entry_timeout_snapshot_quantity')}"
                )

            precheck_snapshot = self.get_timeout_retained_child_snapshot(
                record_snapshot,
                final_parent_quantity
            )
            logger.warning(
                "PARENT FINALITY CHILD QUANTITY CHECK | "
                f"trade_id={trade_id} "
                f"symbol={candidate['symbol']} "
                f"final_parent_quantity={final_parent_quantity} "
                f"quantity_match_count={precheck_snapshot.get('quantity_match_count')} "
                f"visible_child_count={precheck_snapshot.get('visible_child_count')} "
                f"coherent={precheck_snapshot.get('coherent')} "
                f"reason={precheck_snapshot.get('reason')}"
            )

            reconcile_ok = self.reconcile_timeout_retained_child_protection(trade_id)
            if not reconcile_ok:
                logger.error(
                    "PARENT FINALITY RETAINED PROTECTION FAILED | "
                    f"trade_id={trade_id} "
                    f"symbol={candidate['symbol']} "
                    f"final_parent_quantity={final_parent_quantity} "
                    "decision=fail_closed_incomplete"
                )
                self.finalize_trade_if_complete(trade_id)
                continue

            with self.trade_analysis_lock:
                final_record = self.trade_analysis.get(trade_id)
                final_state = final_record["state"] if final_record is not None else "UNKNOWN"

            logger.warning(
                "PARENT FINALITY RETAINED PROTECTION RESULT | "
                f"trade_id={trade_id} "
                f"symbol={candidate['symbol']} "
                f"final_parent_quantity={final_parent_quantity} "
                f"final_decision_state={final_state}"
            )

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
                    if self.has_realized_parent_entry(record):
                        if self.is_partial_timeout_parent_finality_owner_active(record):
                            self.append_trade_event(
                                trade_id,
                                f"PARENT FINALITY OWNERSHIP ACTIVE reqId={req_id} "
                                "decision=suppress_error_callback_promotion"
                            )
                            logger.warning(
                                "PARENT_FINALITY_OWNERSHIP_ACTIVE | "
                                f"trade_id={trade_id} "
                                f"symbol={record['symbol']} "
                                f"req_id={req_id} "
                                "decision=suppress_error_callback_promotion"
                            )
                        else:
                            promotion_reason = (
                                "parent_cancelled_after_timeout_remainder_error"
                                if record.get("partial_entry_timeout_triggered")
                                else "parent_cancelled_after_partial_fill"
                            )
                            self.promote_partial_entry_to_live_quantity(
                                trade_id,
                                record,
                                promotion_reason
                            )
                            self.append_trade_event(
                                trade_id,
                                f"PARENT CANCEL AFTER PARTIAL FILL PRESERVED reqId={req_id} "
                                f"promotion_reason={promotion_reason}"
                            )
                            logger.warning(
                                "PARENT CANCEL ERROR PRESERVED REALIZED ENTRY | "
                                f"trade_id={trade_id} "
                                f"symbol={record['symbol']} "
                                f"req_id={req_id} "
                                f"promotion_reason={promotion_reason} "
                                f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                                f"resulting_state={record['state']}"
                            )
                    else:
                        record["state"] = "CANCELLED"
                        self.aggregate_stats["cancelled_count"] += 1
                        logger.warning(
                            "PARENT CANCEL ERROR ORDINARY NON-ENTRY | "
                            f"trade_id={trade_id} "
                            f"symbol={record['symbol']} "
                            f"req_id={req_id} "
                            "cancelled_count_incremented=true resulting_state=CANCELLED"
                        )
                        self.finalize_trade_if_complete(trade_id)
                else:
                    self.append_anomaly(trade_id, f"CHILD_CANCELLED_{req_id}")
            elif error_code == 202 and req_id == record["parent_order_id"] and record["entry_filled"]:
                logger.info(
                    "PARENT CANCEL ERROR OBSERVED AFTER ENTRY LOCKED | "
                    f"trade_id={trade_id} "
                    f"symbol={record['symbol']} "
                    f"req_id={req_id} "
                    f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                    f"state_preserved={record['state']}"
                )

            if error_code == 202 and record["state"] in ACTIVE_TRADE_STATES:
                self.get_active_trade_candidates("ERROR_202_CANCEL_CLEANUP")
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
        for field in ("candidate_side", "side", "direction"):
            explicit_side = self._safe_str(payload.get(field), default="").lower()
            if explicit_side in {"long", "buy", "bull", "up"}:
                return "long"
            if explicit_side in {"short", "sell", "bear", "down"}:
                return "short"

        long_score = 0
        short_score = 0

        sweep_side = self._safe_str(payload.get("sweep_side"), default="").lower()
        if sweep_side == "down":
            long_score += 2
        elif sweep_side == "up":
            short_score += 2

        if "above_vwap" in payload:
            above_vwap = self._safe_bool(payload.get("above_vwap"), default=None)
            if above_vwap is True:
                long_score += 1
            elif above_vwap is False:
                short_score += 1

        vwap_slope_1m = payload.get("vwap_slope_1m")
        if isinstance(vwap_slope_1m, (int, float)):
            if vwap_slope_1m > 0:
                long_score += 1
            elif vwap_slope_1m < 0:
                short_score += 1

        vwap_slope_5m = payload.get("vwap_slope_5m")
        if isinstance(vwap_slope_5m, (int, float)):
            if vwap_slope_5m > 0:
                long_score += 1
            elif vwap_slope_5m < 0:
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

    def normalize_strategy_family_hint(self, payload):
        raw_hint = payload.get("strategy_family_hint")
        hint_field = "strategy_family_hint"

        if raw_hint is None:
            raw_hint = payload.get("strategy_family")
            hint_field = "strategy_family"

        if raw_hint is None:
            return {
                "hint_present": False,
                "hint_field": hint_field,
                "raw_hint": None,
                "normalized_hint": None,
            }

        hint_text = str(raw_hint).strip()
        if not hint_text:
            return {
                "hint_present": False,
                "hint_field": hint_field,
                "raw_hint": raw_hint,
                "normalized_hint": None,
            }

        normalized_text = hint_text.upper().replace("-", "_").replace(" ", "_")

        if normalized_text in RECOGNIZED_STRATEGY_FAMILIES:
            normalized_hint = normalized_text
        else:
            normalized_hint = "UNKNOWN"

        return {
            "hint_present": True,
            "hint_field": hint_field,
            "raw_hint": raw_hint,
            "normalized_hint": normalized_hint,
        }

    def has_minimal_det_routing_structure(self, payload):
        det_like_keys = (
            "trigger_bar_high",
            "trigger_bar_low",
            "structure_anchor_low",
            "structure_anchor_high",
            "rejection_detected",
            "sweep_detected",
            "vwap_slope_1m",
            "vwap_slope_5m",
            "htf_trend_up",
            "htf_trend_down",
            "htf_vwap_up",
            "htf_vwap_down",
        )
        return any(key in payload for key in det_like_keys)

    def has_usable_det_context_observation(self, normalized):
        return any((
            normalized.get("above_vwap") is not None,
            normalized.get("vwap_price") is not None,
            normalized.get("vwap_slope_1m") is not None,
            normalized.get("vwap_slope_5m") is not None,
            normalized.get("vwap_distance_points") is not None,
            normalized.get("vwap_distance_atr") is not None,
            normalized.get("distance_from_vwap_atr_value") is not None,
            normalized.get("distance_from_vwap_atr") is not None,
            normalized.get("bias_5m") not in (None, "", "unknown"),
        ))

    def has_usable_det_trigger_structure_observation(self, normalized):
        return any((
            normalized.get("trigger_bar_high") is not None,
            normalized.get("trigger_bar_low") is not None,
            normalized.get("structure_anchor_low") is not None,
            normalized.get("structure_anchor_high") is not None,
            normalized.get("rejection_detected") is True,
            normalized.get("sweep_detected") is True,
            normalized.get("structure_1m_ok") is True,
            normalized.get("reacceleration_1m_ok") is True,
        ))

    def has_sufficient_det_family_basis(self, normalized):
        if normalized.get("payload_format") != "enriched_candidate":
            return {
                "sufficient": False,
                "reason": "det_family_requires_enriched_candidate_payload",
                "context_basis_ok": False,
                "trigger_structure_basis_ok": False,
            }

        if normalized.get("side") not in {"long", "short"}:
            return {
                "sufficient": False,
                "reason": "det_family_requires_canonical_side",
                "context_basis_ok": False,
                "trigger_structure_basis_ok": False,
            }

        context_basis_ok = self.has_usable_det_context_observation(normalized)
        trigger_structure_basis_ok = self.has_usable_det_trigger_structure_observation(normalized)

        if not context_basis_ok and not trigger_structure_basis_ok:
            reason = "det_family_requires_context_and_trigger_structure_basis"
        elif not context_basis_ok:
            reason = "det_family_requires_context_basis"
        elif not trigger_structure_basis_ok:
            reason = "det_family_requires_trigger_structure_basis"
        else:
            reason = "det_family_basis_sufficient"

        return {
            "sufficient": context_basis_ok and trigger_structure_basis_ok,
            "reason": reason,
            "context_basis_ok": context_basis_ok,
            "trigger_structure_basis_ok": trigger_structure_basis_ok,
        }

    def extract_pine_strategy_family_observability(self, payload):
        hint_info = self.normalize_strategy_family_hint(payload)
        return {
            "pine_family_hint_present": hint_info["hint_present"],
            "pine_strategy_family_hint_field": hint_info["hint_field"],
            "pine_strategy_family_hint": hint_info["raw_hint"],
            "pine_strategy_family_normalized": hint_info["normalized_hint"] or "UNKNOWN",
        }

    def resolve_strategy_family_bot_side(self, normalized, pine_family_info=None):
        if pine_family_info is None:
            pine_family_info = self.extract_pine_strategy_family_observability(
                normalized.get("raw_payload", {})
            )

        pine_hint = pine_family_info.get("pine_strategy_family_normalized")
        det_family_basis = self.has_sufficient_det_family_basis(normalized)

        if det_family_basis["sufficient"]:
            bot_side_family_candidate = "DET"
            bot_side_family_confidence = "high"
            bot_side_family_reason = det_family_basis["reason"]
            bot_side_family_resolution_source = "bot_det_family_basis_gate"
            strategy_family = "DET"
            family_consistency_ok = True
            routing_status = "ROUTED"
            routing_reason_code = det_family_basis["reason"]
        else:
            bot_side_family_candidate = "UNKNOWN"
            bot_side_family_confidence = "low"
            bot_side_family_reason = det_family_basis["reason"]
            bot_side_family_resolution_source = "bot_fail_closed_family_basis_gate"
            strategy_family = "UNKNOWN"
            family_consistency_ok = False
            routing_status = "ROUTING_UNKNOWN"
            routing_reason_code = det_family_basis["reason"]

        bot_side_family_hint_conflict = bool(
            pine_family_info.get("pine_family_hint_present") and
            pine_hint not in (None, "", "UNKNOWN") and
            pine_hint != strategy_family
        )

        return {
            "bot_side_family_candidate": bot_side_family_candidate,
            "bot_side_family_confidence": bot_side_family_confidence,
            "bot_side_family_reason": bot_side_family_reason,
            "bot_side_family_resolution_source": bot_side_family_resolution_source,
            "bot_side_family_hint_conflict": bot_side_family_hint_conflict,
            "strategy_family": strategy_family,
            "routing_status": routing_status,
            "routing_reason_code": routing_reason_code,
            "family_consistency_ok": family_consistency_ok,
            "det_family_context_basis_ok": det_family_basis["context_basis_ok"],
            "det_family_trigger_structure_basis_ok": det_family_basis["trigger_structure_basis_ok"],
        }

    def should_route_to_det_evaluator(self, normalized):
        return (
            normalized.get("strategy_family") == "DET" and
            normalized.get("routing_status") == "ROUTED" and
            normalized.get("family_consistency_ok") is True
        )

    def get_det_routing_block_status(self, normalized):
        routing_status = normalized.get("routing_status")
        if routing_status == "ROUTING_UNKNOWN":
            return "routing_unknown_no_execution"
        if routing_status == "ROUTING_INCONSISTENT":
            return "routing_inconsistent_no_execution"
        if routing_status == "ROUTING_DENIED":
            return "routing_denied_no_execution"
        if normalized.get("strategy_family") != "DET":
            return "non_det_family_no_execution"
        return "routing_inconsistent_no_execution"

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
            "strategy_family_hint",
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
        pine_family_info = self.extract_pine_strategy_family_observability(payload)

        symbol = self._normalize_symbol(payload.get("symbol"))
        side = self._normalize_side(payload)
        raw_session_name = payload.get("session_name")
        session_name = None if raw_session_name is None else str(raw_session_name).strip()
        if session_name == "":
            session_name = None

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
            "pine_family_hint_present": pine_family_info["pine_family_hint_present"],
            "pine_strategy_family_hint_field": pine_family_info["pine_strategy_family_hint_field"],
            "pine_strategy_family_hint": pine_family_info["pine_strategy_family_hint"],
            "pine_strategy_family_normalized": pine_family_info["pine_strategy_family_normalized"],
            "strategy_family": "UNKNOWN",
            "routing_status": "ROUTING_UNKNOWN",
            "routing_reason_code": "bot_side_family_pending",
            "family_consistency_ok": False,
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
            "setup_valid": self._safe_bool(payload.get("setup_valid"), default=None),
            "trigger_valid": self._safe_bool(payload.get("trigger_valid"), default=None),
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
            "session_name": session_name,
            "minutes_from_open": self._safe_int(payload.get("minutes_from_open"), default=None),
            "vwap_price": self._safe_float(payload.get("vwap_price"), default=None),
            "vwap_distance_points": self._safe_float(payload.get("vwap_distance_points"), default=None),
            "vwap_distance_atr": self._safe_float(payload.get("vwap_distance_atr"), default=None),
            "vwap_slope_1m": self._safe_float(payload.get("vwap_slope_1m"), default=None),
            "vwap_slope_5m": self._safe_float(payload.get("vwap_slope_5m"), default=None),
            "above_vwap": self._safe_bool(payload.get("above_vwap"), default=None),
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
            "htf_trend_up": self._safe_bool(payload.get("htf_trend_up"), default=None),
            "htf_trend_down": self._safe_bool(payload.get("htf_trend_down"), default=None),
            "htf_vwap_up": self._safe_bool(payload.get("htf_vwap_up"), default=None),
            "htf_vwap_down": self._safe_bool(payload.get("htf_vwap_down"), default=None),
            "htf_vwap_not_flat": self._safe_bool(payload.get("htf_vwap_not_flat"), default=None),
            "htf_not_choppy": self._safe_bool(payload.get("htf_not_choppy"), default=None),
            "htf_not_late_long": self._safe_bool(payload.get("htf_not_late_long"), default=None),
            "htf_not_late_short": self._safe_bool(payload.get("htf_not_late_short"), default=None),
            "htf_moved_away_long": self._safe_bool(payload.get("htf_moved_away_long"), default=None),
            "htf_moved_away_short": self._safe_bool(payload.get("htf_moved_away_short"), default=None),
            "htf_structure_long": self._safe_bool(payload.get("htf_structure_long"), default=None),
            "htf_structure_short": self._safe_bool(payload.get("htf_structure_short"), default=None),
            "htf_setup_long": self._safe_bool(payload.get("htf_setup_long"), default=None),
            "htf_setup_short": self._safe_bool(payload.get("htf_setup_short"), default=None),
            "distance_from_vwap_atr": self._safe_float(payload.get("distance_from_vwap_atr"), default=None),
            "htf_ema_spread_atr": self._safe_float(payload.get("htf_ema_spread_atr"), default=None),
            "body_strength": self._safe_float(payload.get("body_strength"), default=None),
        }

        bot_side_routing_info = self.resolve_strategy_family_bot_side(
            normalized,
            pine_family_info,
        )
        normalized["bot_side_family_candidate"] = bot_side_routing_info["bot_side_family_candidate"]
        normalized["bot_side_family_confidence"] = bot_side_routing_info["bot_side_family_confidence"]
        normalized["bot_side_family_reason"] = bot_side_routing_info["bot_side_family_reason"]
        normalized["bot_side_family_resolution_source"] = bot_side_routing_info["bot_side_family_resolution_source"]
        normalized["bot_side_family_hint_conflict"] = bot_side_routing_info["bot_side_family_hint_conflict"]
        normalized["strategy_family"] = bot_side_routing_info["strategy_family"]
        normalized["routing_status"] = bot_side_routing_info["routing_status"]
        normalized["routing_reason_code"] = bot_side_routing_info["routing_reason_code"]
        normalized["family_consistency_ok"] = bot_side_routing_info["family_consistency_ok"]
        normalized["det_family_context_basis_ok"] = bot_side_routing_info["det_family_context_basis_ok"]
        normalized["det_family_trigger_structure_basis_ok"] = bot_side_routing_info["det_family_trigger_structure_basis_ok"]

        normalized["bot_observations"] = {
            "symbol": normalized["symbol"],
            "side": normalized["side"],
            "entry_price": normalized["entry_price"],
            "price": normalized["price"],
            "entry_reference_price": normalized["entry_reference_price"],
            "trigger_bar_high": normalized["trigger_bar_high"],
            "trigger_bar_low": normalized["trigger_bar_low"],
            "structure_anchor_low": normalized["structure_anchor_low"],
            "structure_anchor_high": normalized["structure_anchor_high"],
            "above_vwap": normalized["above_vwap"],
            "vwap_price": normalized["vwap_price"],
            "vwap_distance_points": normalized["vwap_distance_points"],
            "vwap_distance_atr": normalized["vwap_distance_atr"],
            "distance_from_vwap_atr_value": normalized["distance_from_vwap_atr_value"],
            "distance_from_vwap_atr": normalized["distance_from_vwap_atr"],
            "vwap_slope_1m": normalized["vwap_slope_1m"],
            "vwap_slope_5m": normalized["vwap_slope_5m"],
            "trend_strength_5m": normalized["trend_strength_5m"],
            "overlap_ratio_5m": normalized["overlap_ratio_5m"],
            "range_state_5m": normalized["range_state_5m"],
            "body_strength_value": normalized["body_strength_value"],
            "body_strength": normalized["body_strength"],
            "ema_spread_atr_value": normalized["ema_spread_atr_value"],
            "htf_ema_spread_atr": normalized["htf_ema_spread_atr"],
            "pullback_depth_1m": normalized["pullback_depth_1m"],
            "rejection_detected": normalized["rejection_detected"],
            "rejection_wick_ratio": normalized["rejection_wick_ratio"],
            "sweep_detected": normalized["sweep_detected"],
            "sweep_side": normalized["sweep_side"],
            "spread_estimate_ticks": normalized["spread_estimate_ticks"],
            "rr_estimate": normalized["rr_estimate"],
            "session_name": normalized["session_name"],
            "minutes_from_open": normalized["minutes_from_open"],
            "timestamp_utc": normalized["timestamp_utc"],
            "bar_time_unix_ms": normalized["bar_time_unix_ms"],
        }

        normalized["weak_transition_hints"] = {
            "bias_5m": normalized["bias_5m"],
            "htf_trend_up": normalized["htf_trend_up"],
            "htf_trend_down": normalized["htf_trend_down"],
            "htf_vwap_up": normalized["htf_vwap_up"],
            "htf_vwap_down": normalized["htf_vwap_down"],
            "htf_vwap_not_flat": normalized["htf_vwap_not_flat"],
            "structure_1m_ok": normalized["structure_1m_ok"],
            "reacceleration_1m_ok": normalized["reacceleration_1m_ok"],
            "htf_structure_long": normalized["htf_structure_long"],
            "htf_structure_short": normalized["htf_structure_short"],
            "htf_setup_long": normalized["htf_setup_long"],
            "htf_setup_short": normalized["htf_setup_short"],
        }

        normalized["pine_semantic_non_authority"] = {
            "session_valid": normalized["session_valid"],
            "not_choppy_flag": normalized["not_choppy_flag"],
            "late_filter_flag": normalized["late_filter_flag"],
            "moved_away_flag": normalized["moved_away_flag"],
            "structure_flag": normalized["structure_flag"],
            "setup_flag": normalized["setup_flag"],
            "trigger_flag": normalized["trigger_flag"],
            "vwap_bias_valid": normalized["vwap_bias_valid"],
            "body_strength_valid": normalized["body_strength_valid"],
            "structure_valid": normalized["structure_valid"],
            "setup_valid": normalized["setup_valid"],
            "trigger_valid": normalized["trigger_valid"],
            "execution_candidate_valid": normalized["execution_candidate_valid"],
            "blocker": normalized["blocker"],
            "primary_blocker": normalized["primary_blocker"],
            "blocker_count": normalized["blocker_count"],
            "pine_pass_count": normalized["pine_pass_count"],
            "pine_fail_count": normalized["pine_fail_count"],
            "pine_detector_score": normalized["pine_detector_score"],
            "candidate_grade": normalized["candidate_grade"],
            "grade": normalized["grade"],
            "tv_score": normalized["tv_score"],
            "reason_flags": normalized["reason_flags"],
            "execution_quality_hint": normalized["execution_quality_hint"],
        }

        normalized["candidate_side"] = normalized["candidate_side"] or normalized["side"]
        normalized["side_hint"] = normalized["candidate_side"] or normalized["side"]
        normalized["observations"] = normalized["bot_observations"]
        normalized["pine_legacy_debug"] = normalized["pine_semantic_non_authority"]
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
            "pine_family_hint_present": normalized["pine_family_hint_present"],
            "pine_strategy_family_hint_field": normalized["pine_strategy_family_hint_field"],
            "pine_strategy_family_hint": normalized["pine_strategy_family_hint"],
            "pine_strategy_family_normalized": normalized["pine_strategy_family_normalized"],
            "bot_side_family_candidate": normalized["bot_side_family_candidate"],
            "bot_side_family_confidence": normalized["bot_side_family_confidence"],
            "bot_side_family_reason": normalized["bot_side_family_reason"],
            "bot_side_family_resolution_source": normalized["bot_side_family_resolution_source"],
            "bot_side_family_hint_conflict": normalized["bot_side_family_hint_conflict"],
            "strategy_family": normalized["strategy_family"],
            "det_family_context_basis_ok": normalized["det_family_context_basis_ok"],
            "det_family_trigger_structure_basis_ok": normalized["det_family_trigger_structure_basis_ok"],
            "routing_status": normalized["routing_status"],
            "routing_reason_code": normalized["routing_reason_code"],
            "family_consistency_ok": normalized["family_consistency_ok"],
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
            "minutes_from_open": normalized["minutes_from_open"],
            "above_vwap": normalized["above_vwap"],
            "vwap_slope_1m": normalized["vwap_slope_1m"],
            "vwap_slope_5m": normalized["vwap_slope_5m"],
            "bias_5m": normalized["bias_5m"],
            "reason_flags": normalized["reason_flags"],
            "execution_ready": normalized["execution_ready"],
            "trend_strength_5m": normalized["trend_strength_5m"],
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
        summary["bot_observations"] = normalized["bot_observations"]
        summary["weak_transition_hints"] = normalized["weak_transition_hints"]
        summary["pine_semantic_non_authority"] = normalized["pine_semantic_non_authority"]
        return summary

    def log_normalized_signal(self, normalized):
        raw_payload = normalized["raw_payload"]
        field_presence = {
            field: {
                "present": field in raw_payload,
                "value": normalized.get(field),
            }
            for field in (
                "session_name",
                "minutes_from_open",
                "above_vwap",
                "vwap_slope_1m",
                "vwap_slope_5m",
                "htf_trend_up",
                "htf_trend_down",
                "htf_vwap_up",
                "htf_vwap_down",
                "htf_vwap_not_flat",
                "htf_structure_long",
                "htf_structure_short",
                "htf_setup_long",
                "htf_setup_short",
            )
        }

        logger.info(f"RAW PAYLOAD RECEIVED | {json.dumps(normalized['raw_payload'], sort_keys=True)}")
        logger.info(f"PINE_OBSERVATIONS | {json.dumps(normalized['bot_observations'], sort_keys=True)}")
        logger.info(f"PINE_WEAK_TRANSITION_HINTS | {json.dumps(normalized['weak_transition_hints'], sort_keys=True)}")
        logger.info(f"PINE_SEMANTIC_NON_AUTHORITY | {json.dumps(normalized['pine_semantic_non_authority'], sort_keys=True)}")
        logger.info(f"NORMALIZED SIGNAL | {json.dumps(self.build_normalized_summary(normalized), sort_keys=True)}")
        logger.info(f"PAYLOAD FIELD PRESENCE | {json.dumps(field_presence, sort_keys=True)}")
        logger.info(
            "STRATEGY ROUTING | "
            f"payload_format={normalized['payload_format']} "
            f"signal_id={normalized['signal_id']} "
            f"symbol={normalized['symbol']} "
            f"pine_family_hint_present={normalized['pine_family_hint_present']} "
            f"pine_strategy_family_hint_field={normalized['pine_strategy_family_hint_field']} "
            f"pine_strategy_family_hint={normalized['pine_strategy_family_hint']} "
            f"pine_strategy_family_normalized={normalized['pine_strategy_family_normalized']} "
            f"bot_side_family_candidate={normalized['bot_side_family_candidate']} "
            f"bot_side_family_confidence={normalized['bot_side_family_confidence']} "
            f"bot_side_family_reason={normalized['bot_side_family_reason']} "
            f"bot_side_family_resolution_source={normalized['bot_side_family_resolution_source']} "
            f"bot_side_family_hint_conflict={normalized['bot_side_family_hint_conflict']} "
            f"strategy_family={normalized['strategy_family']} "
            f"det_family_context_basis_ok={normalized['det_family_context_basis_ok']} "
            f"det_family_trigger_structure_basis_ok={normalized['det_family_trigger_structure_basis_ok']} "
            f"routing_status={normalized['routing_status']} "
            f"routing_reason_code={normalized['routing_reason_code']} "
            f"family_consistency_ok={normalized['family_consistency_ok']} "
            "routing_authority=bot_side_strategy_family"
        )
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

    def build_observation_layer(self, normalized_signal):
        symbol = normalized_signal["symbol"]
        market_profile = symbol or "unknown"
        setup_profile = "default"

        return {
            "market_profile": market_profile,
            "setup_profile": setup_profile,
            "symbol": normalized_signal["symbol"],
            "timeframe": normalized_signal["timeframe"],
            "session_name": normalized_signal["session_name"],
            "minutes_from_open": normalized_signal["minutes_from_open"],
            "price": normalized_signal["price"],
            "observations": normalized_signal["observations"],
            "side_hint": normalized_signal["side_hint"],
            "pine_legacy_debug": normalized_signal["pine_legacy_debug"],
            "normalized_signal": normalized_signal,
        }

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
            if stage in {"TEST", "PAPER", "LIVE"}:
                policy_branch = "det_gated_execution_enabled"
                allow_queue = True
                policy_reason = (
                    "DET-gated execution is enabled for TEST, PAPER, and LIVE; "
                    "enriched candidates may execute if DET gate permits"
                )
            else:
                policy_branch = "det_gated_execution_unknown_stage_disabled"
                allow_queue = False
                policy_reason = (
                    "DET-gated execution disabled because stage is not recognized for execution"
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

        policy = {
            "stage": stage,
            "policy_branch": policy_branch,
            "allow_queue": allow_queue,
            "execution_enabled": allow_queue,
            "reason": policy_reason,
        }
        logger.info(
            "STAGE EXECUTION POLICY | "
            f"stage={policy['stage']} "
            f"payload_format={payload_format} "
            f"execution_enabled={policy['execution_enabled']} "
            f"policy_branch={policy['policy_branch']} "
            f"reason={policy['reason']}"
        )
        return policy

    def assess_det_context(self, normalized):
        """Bot-side context assessment derived from observations."""
        obs = normalized["bot_observations"]
        hints = normalized["weak_transition_hints"]
        semantic = normalized["pine_semantic_non_authority"]
        
        side = obs["side"]
        
        # Session state - primary: obs session/timing data
        minutes_from_open = obs.get("minutes_from_open")
        session_name = self._safe_str(obs.get("session_name"), default="unknown").lower()
        session_state = "valid"
        if session_name in {"closed", "outside", "afterhours", "after-hours", "premarket", "pre-market", "preopen"}:
            session_state = "invalid"
        elif minutes_from_open is not None and minutes_from_open < 0:
            session_state = "invalid"
        
        # Bias state - primary: obs above_vwap, vwap slopes, distance, trend strength
        above_vwap = obs.get("above_vwap")
        vwap_slope_1m = obs.get("vwap_slope_1m")
        vwap_slope_5m = obs.get("vwap_slope_5m")
        trend_strength_5m = obs.get("trend_strength_5m")
        distance_from_vwap_atr = obs.get("distance_from_vwap_atr_value")
        if distance_from_vwap_atr is None:
            distance_from_vwap_atr = obs.get("distance_from_vwap_atr")

        long_evidence = 0
        short_evidence = 0

        if above_vwap is True:
            long_evidence += 1
        elif above_vwap is False:
            short_evidence += 1

        if isinstance(vwap_slope_1m, (int, float)):
            if vwap_slope_1m > 0:
                long_evidence += 1
            elif vwap_slope_1m < 0:
                short_evidence += 1

        if isinstance(vwap_slope_5m, (int, float)):
            if vwap_slope_5m > 0:
                long_evidence += 1
            elif vwap_slope_5m < 0:
                short_evidence += 1

        if isinstance(trend_strength_5m, (int, float)):
            if trend_strength_5m > 0.2:
                long_evidence += 1
            elif trend_strength_5m < -0.2:
                short_evidence += 1

        if distance_from_vwap_atr is not None and distance_from_vwap_atr > 1.75:
            if above_vwap is True:
                long_evidence += 1
            elif above_vwap is False:
                short_evidence += 1

        if side == "long":
            if long_evidence >= 3 and short_evidence <= 1:
                bias_state = "aligned"
            elif short_evidence >= 2 and short_evidence > long_evidence:
                bias_state = "conflicting"
            else:
                bias_state = "mixed"
        elif side == "short":
            if short_evidence >= 3 and long_evidence <= 1:
                bias_state = "aligned"
            elif long_evidence >= 2 and long_evidence > short_evidence:
                bias_state = "conflicting"
            else:
                bias_state = "mixed"
        else:
            if abs(long_evidence - short_evidence) >= 3:
                bias_state = "aligned"
            else:
                bias_state = "mixed"

        # Weak hints may break ties only when primary evidence is inconclusive
        if bias_state == "mixed" and long_evidence == short_evidence:
            bias_5m = hints.get("bias_5m", "").lower()
            if bias_5m in {"long", "bull", "up"}:
                bias_state = "mixed"  # Keep mixed, don't override
            elif bias_5m in {"short", "bear", "down"}:
                bias_state = "mixed"  # Keep mixed, don't override

        # Chop state - primary: obs overlap_ratio_5m, trend_strength_5m, vwap slopes
        overlap_ratio_5m = obs.get("overlap_ratio_5m")
        chop_state = "not_choppy"
        if overlap_ratio_5m is not None and overlap_ratio_5m > 0.6:
            chop_state = "choppy"
        elif isinstance(trend_strength_5m, (int, float)) and abs(trend_strength_5m) < 0.25:
            if isinstance(vwap_slope_1m, (int, float)) and abs(vwap_slope_1m) < 0.0001:
                chop_state = "choppy"
            elif isinstance(vwap_slope_5m, (int, float)) and abs(vwap_slope_5m) < 0.0001:
                chop_state = "choppy"

        # Late state - primary: obs extension/distance data
        late_distance = obs.get("distance_from_vwap_atr_value")
        if late_distance is None:
            late_distance = obs.get("distance_from_vwap_atr")
        if late_distance is not None and late_distance >= 1.8:
            late_state = "late"
        else:
            late_state = "not_late"

        # Departure state - primary: obs VWAP relation + extension proxies
        departure_state = "not_departed"
        if side == "long" and above_vwap is True:
            if late_distance is not None and late_distance > 1.2:
                departure_state = "departed"
        elif side == "short" and above_vwap is False:
            if late_distance is not None and late_distance > 1.2:
                departure_state = "departed"

        # VWAP state - primary: obs above_vwap and related price context
        if above_vwap is True:
            vwap_state = "above"
        elif above_vwap is False:
            vwap_state = "below"
        else:
            vwap_state = "unknown"

        context_state = {
            "session_state": session_state,
            "bias_state": bias_state,
            "chop_state": chop_state,
            "late_state": late_state,
            "departure_state": departure_state,
            "vwap_state": vwap_state,
        }

        logger.info(f"BOT_CONTEXT_STATE | {json.dumps(context_state, sort_keys=True)}")
        return context_state

    def assess_det_structure(self, normalized, context_state):
        """Bot-side structure assessment derived from observations."""
        obs = normalized["bot_observations"]
        hints = normalized["weak_transition_hints"]
        semantic = normalized["pine_semantic_non_authority"]
        
        side = obs["side"]
        
        structure_anchor_low = obs.get("structure_anchor_low")
        structure_anchor_high = obs.get("structure_anchor_high")
        trigger_bar_high = obs.get("trigger_bar_high")
        trigger_bar_low = obs.get("trigger_bar_low")
        sweep_detected = obs.get("sweep_detected", False)
        rejection_detected = obs.get("rejection_detected", False)
        rejection_wick_ratio = obs.get("rejection_wick_ratio")
        
        # Basic anchor quality
        has_anchors = structure_anchor_low is not None and structure_anchor_high is not None
        has_trigger_bar = trigger_bar_low is not None and trigger_bar_high is not None
        
        # Structure quality: more DET-meaningful assessment
        structure_quality = "poor"
        
        # Strong: Clear structural support with anchor respect and sweep/rejection evidence
        if has_anchors and has_trigger_bar:
            anchor_respected = False
            if side == "long":
                anchor_respected = trigger_bar_low >= structure_anchor_low
            elif side == "short":
                anchor_respected = trigger_bar_high <= structure_anchor_high
            
            if anchor_respected and sweep_detected:
                structure_quality = "strong"
            elif anchor_respected and rejection_detected and rejection_wick_ratio and rejection_wick_ratio > 0.6:
                structure_quality = "strong"
            elif anchor_respected:
                structure_quality = "moderate"
            else:
                structure_quality = "weak"
        elif has_anchors:
            # Anchors present but no trigger bar - moderate support
            structure_quality = "moderate"
        elif rejection_detected and rejection_wick_ratio and rejection_wick_ratio > 0.5:
            # No anchors but strong rejection - moderate support
            structure_quality = "moderate"
        elif sweep_detected:
            # Sweep without anchors - weak but some support
            structure_quality = "weak"
        
        structure_state = {
            "structure_quality": structure_quality,
            "has_anchors": has_anchors,
            "has_trigger_bar": has_trigger_bar,
            "sweep_detected": sweep_detected,
            "rejection_detected": rejection_detected,
        }
        
        logger.info(f"BOT STRUCTURE ASSESSMENT | state={structure_state} side={side}")
        return structure_state

    def assess_det_trigger(self, normalized, context_state, structure_state):
        """Bot-side trigger assessment derived from observations."""
        obs = normalized["bot_observations"]
        hints = normalized["weak_transition_hints"]
        semantic = normalized["pine_semantic_non_authority"]
        
        side = obs["side"]
        
        body_strength_value = obs.get("body_strength_value")
        if body_strength_value is None:
            body_strength_value = obs.get("body_strength")
        trigger_bar_high = obs.get("trigger_bar_high")
        trigger_bar_low = obs.get("trigger_bar_low")
        reacceleration_1m_ok = hints.get("reacceleration_1m_ok", False)
        structure_1m_ok = hints.get("structure_1m_ok", False)
        rejection_detected = obs.get("rejection_detected", False)
        rejection_wick_ratio = obs.get("rejection_wick_ratio")
        pullback_depth_1m = obs.get("pullback_depth_1m")
        price = obs.get("price")
        entry_reference_price = obs.get("entry_reference_price")
        
        # Trigger quality
        trigger_quality = "weak"
        
        strong_signals = 0
        if body_strength_value is not None and body_strength_value >= 0.85:
            strong_signals += 1
        if rejection_detected and rejection_wick_ratio is not None and rejection_wick_ratio > 0.7:
            strong_signals += 1
        if pullback_depth_1m is not None and pullback_depth_1m < 0.25:
            strong_signals += 1

        if strong_signals >= 2:
            trigger_quality = "strong"
        elif strong_signals == 1:
            if reacceleration_1m_ok or structure_1m_ok:
                trigger_quality = "strong"
            else:
                trigger_quality = "moderate"
        elif body_strength_value is not None and body_strength_value >= 0.70:
            trigger_quality = "moderate"

        # Price relationship
        price_alignment = "neutral"
        if price is not None and entry_reference_price is not None:
            diff = abs(price - entry_reference_price)
            if diff < 0.001:
                price_alignment = "tight"
            elif diff < 0.01:
                price_alignment = "close"

        trigger_state = {
            "trigger_quality": trigger_quality,
            "body_strength_value": body_strength_value,
            "reacceleration_1m_ok": reacceleration_1m_ok,
            "structure_1m_ok": structure_1m_ok,
            "rejection_detected": rejection_detected,
            "rejection_wick_ratio": rejection_wick_ratio,
            "pullback_depth_1m": pullback_depth_1m,
            "price_alignment": price_alignment,
            "strong_signals": strong_signals,
        }

        logger.info(f"BOT_TRIGGER_STATE | {json.dumps(trigger_state, sort_keys=True)}")
        return trigger_state

    def assess_det_signal(self, normalized):
        observation_layer = self.build_observation_layer(normalized)
        return self.assess_truth(
            observation_layer,
            observation_layer["market_profile"],
            observation_layer["setup_profile"]
        )

    def derive_truth_plan_models(self, normalized, derived_side):
        obs = normalized["bot_observations"]
        entry_reference_price = obs.get("entry_reference_price")
        price = obs.get("price")

        if entry_reference_price is not None:
            derived_entry_model = "reference_price"
            entry_basis_price = entry_reference_price
        elif price is not None:
            derived_entry_model = "price"
            entry_basis_price = price
        else:
            derived_entry_model = None
            entry_basis_price = None

        anchor_value = None
        trigger_value = None
        anchor_relevant = False
        trigger_relevant = False

        if derived_side == "long":
            anchor_value = obs.get("structure_anchor_low")
            trigger_value = obs.get("trigger_bar_low")
            if anchor_value is not None and entry_basis_price is not None and anchor_value < entry_basis_price:
                anchor_relevant = True
            if trigger_value is not None and entry_basis_price is not None and trigger_value < entry_basis_price:
                trigger_relevant = True
        elif derived_side == "short":
            anchor_value = obs.get("structure_anchor_high")
            trigger_value = obs.get("trigger_bar_high")
            if anchor_value is not None and entry_basis_price is not None and anchor_value > entry_basis_price:
                anchor_relevant = True
            if trigger_value is not None and entry_basis_price is not None and trigger_value > entry_basis_price:
                trigger_relevant = True

        if anchor_relevant and trigger_relevant:
            derived_invalidation_model = "structure_anchor_or_trigger_bar"
            derived_stop_model = "anchor_based"
            invalidation_basis_quality = "strong"
            stop_basis_quality = "strong"
        elif anchor_relevant:
            derived_invalidation_model = "structure_anchor"
            derived_stop_model = "anchor_based"
            invalidation_basis_quality = "strong"
            stop_basis_quality = "strong"
        elif trigger_relevant:
            derived_invalidation_model = "trigger_bar"
            derived_stop_model = "trigger_bar_based"
            invalidation_basis_quality = "acceptable"
            stop_basis_quality = "acceptable"
        else:
            derived_invalidation_model = "fallback_invalidation"
            derived_stop_model = "fallback_stop"
            invalidation_basis_quality = "weak"
            stop_basis_quality = "weak"

        if derived_stop_model == "fallback_stop":
            derived_target_model = "fallback_2r"
            target_basis_quality = "weak"
        else:
            derived_target_model = "fixed_2r"
            target_basis_quality = "acceptable"

        logger.info(
            "BOT PLAN MODELS | "
            f"side={derived_side} "
            f"entry_model={derived_entry_model} "
            f"invalidation_model={derived_invalidation_model} "
            f"stop_model={derived_stop_model} "
            f"target_model={derived_target_model} "
            f"anchor_relevant={anchor_relevant} "
            f"trigger_relevant={trigger_relevant} "
            f"entry_basis_price={entry_basis_price} "
            f"anchor_value={anchor_value} "
            f"trigger_value={trigger_value}"
        )

        return {
            "derived_entry_model": derived_entry_model,
            "derived_invalidation_model": derived_invalidation_model,
            "derived_stop_model": derived_stop_model,
            "derived_target_model": derived_target_model,
            "entry_basis_price": entry_basis_price,
            "anchor_value": anchor_value,
            "trigger_value": trigger_value,
            "anchor_relevant": anchor_relevant,
            "trigger_relevant": trigger_relevant,
            "invalidation_basis_quality": invalidation_basis_quality,
            "stop_basis_quality": stop_basis_quality,
            "target_basis_quality": target_basis_quality,
        }

    def assess_truth(self, observation_layer, market_profile, setup_profile):
        normalized = observation_layer["normalized_signal"]
        obs = normalized["bot_observations"]
        hints = normalized["weak_transition_hints"]
        semantic = normalized["pine_semantic_non_authority"]
        
        side = obs["side"]
        hard_blockers = []
        soft_blockers = []
        secondary_reasons = []

        # Bot-side DET assessments derived from categorized normalized data
        context_state = self.assess_det_context(normalized)
        structure_state = self.assess_det_structure(normalized, context_state)
        trigger_state = self.assess_det_trigger(normalized, context_state, structure_state)

        if context_state["session_state"] == "invalid":
            hard_blockers.append("session_invalid")

        if context_state["chop_state"] == "choppy":
            hard_blockers.append("chop")

        if context_state["late_state"] == "late":
            hard_blockers.append("late")

        if context_state["departure_state"] == "departed":
            soft_blockers.append("moved_away_from_vwap")

        if context_state["bias_state"] == "conflicting":
            hard_blockers.append("conflicting_bias")

        if structure_state["structure_quality"] == "poor":
            hard_blockers.append("poor_structure")

        if semantic.get("blocker"):
            secondary_reasons.append(f"pine_blocker:{semantic['blocker']}")

        if semantic.get("reason_flags"):
            secondary_reasons.extend([f"reason_flag:{flag}" for flag in semantic["reason_flags"]])

        distance_from_vwap_atr = obs.get("distance_from_vwap_atr_value")
        if distance_from_vwap_atr is None:
            distance_from_vwap_atr = obs.get("distance_from_vwap_atr")
        if distance_from_vwap_atr is not None:
            if distance_from_vwap_atr >= 1.5:
                soft_blockers.append("extended_from_vwap")
            if distance_from_vwap_atr >= 2.0 and "late" not in hard_blockers:
                hard_blockers.append("late_extension_proxy")

        body_strength_value = trigger_state.get("body_strength_value")
        if body_strength_value is not None and body_strength_value < 0.60:
            soft_blockers.append("weak_body_strength")

        ema_spread_atr = obs.get("ema_spread_atr_value")
        if ema_spread_atr is None:
            ema_spread_atr = obs.get("htf_ema_spread_atr")
        if ema_spread_atr is not None and ema_spread_atr < 0.20:
            soft_blockers.append("low_ema_spread")

        if context_state["bias_state"] == "mixed":
            soft_blockers.append("mixed_bias")

        primary_reason = (
            hard_blockers[0]
            if hard_blockers else
            (soft_blockers[0] if soft_blockers else "det_v2_evaluation")
        )

        pine_setup_valid = semantic.get("setup_valid")
        pine_trigger_valid = semantic.get("trigger_valid")
        pine_structure_valid = semantic.get("structure_valid")
        pine_body_strength_valid = semantic.get("body_strength_valid")

        derived_side = side or observation_layer["side_hint"]
        plan_models = self.derive_truth_plan_models(normalized, derived_side)
        has_entry_basis = plan_models["derived_entry_model"] is not None
        has_invalidation_basis = plan_models["derived_invalidation_model"] != "fallback_invalidation"

        if context_state["session_state"] == "invalid" or context_state["chop_state"] == "choppy":
            regime_truth = "invalid"
        elif (
            context_state["bias_state"] == "aligned" and
            context_state["late_state"] == "not_late" and
            context_state["departure_state"] == "not_departed"
        ):
            regime_truth = "strong"
        else:
            regime_truth = "usable"

        if context_state["vwap_state"] == "unknown" or context_state["bias_state"] == "conflicting":
            vwap_truth = "weak"
        elif (
            context_state["bias_state"] == "aligned" and
            context_state["vwap_state"] in ("above", "below") and
            context_state["departure_state"] == "not_departed"
        ):
            vwap_truth = "strong"
        else:
            vwap_truth = "aligned"

        if context_state["chop_state"] == "choppy" or context_state["bias_state"] == "conflicting":
            context_5m_truth = "dirty"
        elif (
            context_state["bias_state"] == "aligned" and
            context_state["late_state"] == "not_late" and
            context_state["departure_state"] == "not_departed"
        ):
            context_5m_truth = "strong"
        elif context_state["bias_state"] == "aligned":
            context_5m_truth = "clean"
        else:
            context_5m_truth = "mixed"

        if regime_truth == "invalid" or context_5m_truth == "dirty":
            context_truth = "false"
        elif (
            regime_truth == "strong" and
            vwap_truth == "strong" and
            context_5m_truth == "strong"
        ):
            context_truth = "strong"
        elif (
            regime_truth in {"usable", "strong"} and
            vwap_truth in {"aligned", "strong"} and
            context_5m_truth in {"clean", "strong"}
        ):
            context_truth = "valid"
        else:
            context_truth = "weak"

        minutes_from_open = observation_layer["minutes_from_open"]
        if context_state["late_state"] == "late":
            timing_truth = "late"
        elif minutes_from_open is not None and 0 <= minutes_from_open <= 45:
            timing_truth = "early"
        else:
            timing_truth = "acceptable"

        if (
            structure_state["structure_quality"] == "strong" and
            structure_state.get("has_anchors") and
            structure_state.get("has_trigger_bar")
        ):
            structure_1m_truth = "strong"
        elif structure_state["structure_quality"] == "strong":
            structure_1m_truth = "clean"
        elif structure_state["structure_quality"] == "moderate":
            structure_1m_truth = "usable"
        elif structure_state["structure_quality"] == "weak":
            structure_1m_truth = "weak"
        else:
            structure_1m_truth = "weak"

        trigger_effectively_absent = (
            trigger_state["trigger_quality"] == "weak" and
            trigger_state.get("strong_signals", 0) == 0 and
            not trigger_state.get("reacceleration_1m_ok") and
            not trigger_state.get("structure_1m_ok") and
            not trigger_state.get("rejection_detected")
        )

        if trigger_effectively_absent:
            trigger_truth = "false"
        elif trigger_state["trigger_quality"] == "strong":
            trigger_truth = "strong"
        elif trigger_state["trigger_quality"] == "moderate":
            trigger_truth = "valid"
        else:
            trigger_truth = "weak"

        if (
            regime_truth == "invalid" or
            not derived_side or
            context_5m_truth == "dirty" or
            structure_1m_truth == "weak" or
            trigger_truth == "false"
        ):
            setup_truth = "false"
        elif (
            regime_truth == "strong" and
            vwap_truth == "strong" and
            context_5m_truth == "strong" and
            structure_1m_truth in {"clean", "strong"} and
            trigger_truth == "strong"
        ):
            setup_truth = "strong"
        elif (
            regime_truth in {"usable", "strong"} and
            vwap_truth in {"aligned", "strong"} and
            context_5m_truth in {"mixed", "clean", "strong"} and
            structure_1m_truth in {"usable", "clean", "strong"} and
            trigger_truth in {"valid", "strong"}
        ):
            setup_truth = "valid"
        else:
            setup_truth = "weak"

        has_stop_model = plan_models["derived_stop_model"] in {"anchor_based", "trigger_bar_based", "fallback_stop"}
        has_target_model = plan_models["derived_target_model"] in {"fixed_2r", "fallback_2r"}
        coherent_plan = bool(derived_side and has_entry_basis and has_stop_model and has_target_model)
        fallback_driven_plan = (
            plan_models["derived_invalidation_model"] == "fallback_invalidation" or
            plan_models["derived_stop_model"] == "fallback_stop" or
            plan_models["derived_target_model"] == "fallback_2r" or
            plan_models["invalidation_basis_quality"] == "weak" or
            plan_models["stop_basis_quality"] == "weak" or
            plan_models["target_basis_quality"] == "weak"
        )

        weak_plan_support = (
            coherent_plan and
            setup_truth in {"valid", "strong"} and
            regime_truth in {"usable", "strong"} and
            vwap_truth in {"aligned", "strong"} and
            context_5m_truth in {"mixed", "clean", "strong"} and
            structure_1m_truth in {"usable", "clean", "strong"} and
            trigger_truth in {"valid", "strong"} and
            timing_truth in {"early", "acceptable"}
        )

        if not coherent_plan:
            trade_plan_truth = "false"
            trade_plan_reason = "missing_or_incoherent_plan_basis"
        elif (
            setup_truth == "strong" and
            plan_models["invalidation_basis_quality"] == "strong" and
            plan_models["stop_basis_quality"] == "strong" and
            plan_models["target_basis_quality"] == "acceptable"
        ):
            trade_plan_truth = "strong"
            trade_plan_reason = "clean_anchor_or_trigger_supported_plan"
        elif (
            setup_truth in {"valid", "strong"} and
            plan_models["invalidation_basis_quality"] in {"acceptable", "strong"} and
            plan_models["stop_basis_quality"] in {"acceptable", "strong"} and
            plan_models["target_basis_quality"] == "acceptable"
        ):
            trade_plan_truth = "valid"
            trade_plan_reason = "structurally_usable_plan"
        elif (
            weak_plan_support and
            setup_truth in {"valid", "strong"} and
            plan_models["invalidation_basis_quality"] in {"acceptable", "strong"} and
            plan_models["stop_basis_quality"] in {"acceptable", "strong"} and
            plan_models["target_basis_quality"] == "weak"
        ):
            trade_plan_truth = "valid"
            trade_plan_reason = "coherent_plan_with_fallback_target_basis"
        elif fallback_driven_plan and weak_plan_support:
            trade_plan_truth = "weak"
            trade_plan_reason = "fallback_driven_but_coherent_plan"
        elif weak_plan_support:
            trade_plan_truth = "weak"
            trade_plan_reason = "coherent_plan_with_lower_quality_support"
        else:
            trade_plan_truth = "weak"
            trade_plan_reason = "coherent_plan_with_weak_supporting_quality"

        weak_count = sum(
            1 for layer_truth in (
                context_truth,
                setup_truth,
                trigger_truth,
                trade_plan_truth,
            )
            if layer_truth == "weak"
        )

        logger.info(
            "TRADE_PLAN_REASON | "
            f"trade_plan_truth={trade_plan_truth} "
            f"reason={trade_plan_reason} "
            f"coherent_plan={coherent_plan} "
            f"fallback_driven={fallback_driven_plan} "
            f"derived_side={derived_side} "
            f"has_entry_basis={has_entry_basis} "
            f"has_invalidation_basis={has_invalidation_basis} "
            f"has_stop_model={has_stop_model} "
            f"has_target_model={has_target_model} "
            f"entry_model={plan_models['derived_entry_model']} "
            f"invalidation_model={plan_models['derived_invalidation_model']} "
            f"stop_model={plan_models['derived_stop_model']} "
            f"target_model={plan_models['derived_target_model']} "
            f"invalidation_quality={plan_models['invalidation_basis_quality']} "
            f"stop_quality={plan_models['stop_basis_quality']} "
            f"target_quality={plan_models['target_basis_quality']} "
            f"weak_plan_support={weak_plan_support}"
        )

        if (
            trade_plan_truth == "false" or
            trigger_truth == "false" or
            setup_truth == "false" or
            regime_truth == "invalid"
        ):
            r_truth = "poor"
            r_truth_reason = "incoherent_plan_or_absent_trigger"
        elif (
            trade_plan_truth == "strong" and
            setup_truth == "strong" and
            structure_1m_truth == "strong" and
            trigger_truth == "strong" and
            plan_models["stop_basis_quality"] == "strong" and
            plan_models["target_basis_quality"] == "acceptable" and
            timing_truth in {"early", "acceptable"} and
            vwap_truth == "strong"
        ):
            r_truth = "efficient"
            r_truth_reason = "premium_det_economics"
        elif (
            trade_plan_truth in {"valid", "strong"} and
            coherent_plan and
            regime_truth in {"usable", "strong"} and
            vwap_truth in {"aligned", "strong"} and
            context_5m_truth in {"mixed", "clean", "strong"} and
            structure_1m_truth in {"usable", "clean", "strong"} and
            setup_truth in {"valid", "strong"} and
            trigger_truth in {"valid", "strong"} and
            timing_truth in {"early", "acceptable"}
        ):
            r_truth = "acceptable"
            r_truth_reason = "coherent_and_economically_usable"
        elif (
            trade_plan_truth == "weak" and
            weak_plan_support and
            coherent_plan and
            regime_truth in {"usable", "strong"} and
            vwap_truth in {"aligned", "strong"} and
            context_5m_truth in {"mixed", "clean", "strong"} and
            structure_1m_truth in {"usable", "clean", "strong"} and
            setup_truth in {"valid", "strong"} and
            trigger_truth in {"valid", "strong"} and
            timing_truth in {"early", "acceptable"}
        ):
            r_truth = "acceptable"
            r_truth_reason = "coherent_fallback_plan_still_usable"
        else:
            r_truth = "poor"
            r_truth_reason = "weak_or_inefficient_economics"

        logger.info(
            "R_TRUTH_REASON | "
            f"r_truth={r_truth} "
            f"reason={r_truth_reason} "
            f"trade_plan_truth={trade_plan_truth} "
            f"setup_truth={setup_truth} "
            f"structure_1m_truth={structure_1m_truth} "
            f"trigger_truth={trigger_truth} "
            f"timing_truth={timing_truth} "
            f"vwap_truth={vwap_truth} "
            f"coherent_plan={coherent_plan} "
            f"fallback_driven={fallback_driven_plan} "
            f"weak_plan_support={weak_plan_support}"
        )

        return {
            "market_profile": market_profile,
            "setup_profile": setup_profile,
            "context_truth": context_truth,
            "regime_truth": regime_truth,
            "vwap_truth": vwap_truth,
            "context_5m_truth": context_5m_truth,
            "structure_1m_truth": structure_1m_truth,
            "setup_truth": setup_truth,
            "trigger_truth": trigger_truth,
            "timing_truth": timing_truth,
            "trade_plan_truth": trade_plan_truth,
            "trade_plan_reason": trade_plan_reason,
            "r_truth": r_truth,
            "r_truth_reason": r_truth_reason,
            "weak_count": weak_count,
            "derived_side": derived_side,
            "derived_entry_model": plan_models["derived_entry_model"],
            "derived_invalidation_model": plan_models["derived_invalidation_model"],
            "derived_stop_model": plan_models["derived_stop_model"],
            "derived_target_model": plan_models["derived_target_model"],
            "coherent_plan": coherent_plan,
            "fallback_driven_plan": fallback_driven_plan,
            "weak_plan_support": weak_plan_support,
            "hard_blockers": list(dict.fromkeys(hard_blockers)),
            "soft_blockers": list(dict.fromkeys(soft_blockers)),
            "primary_reason": primary_reason,
            "secondary_reasons": list(dict.fromkeys(secondary_reasons)),
            "bot_context_state": context_state,
            "bot_structure_state": structure_state,
            "bot_trigger_state": trigger_state,
            "session_valid": context_state["session_state"] == "valid",
            "vwap_bias_valid": context_state["bias_state"] != "conflicting",
            "structure_valid": structure_state["structure_quality"] != "poor",
            "pine_setup_valid": pine_setup_valid,
            "pine_trigger_valid": pine_trigger_valid,
            "pine_structure_valid": pine_structure_valid,
            "pine_body_strength_valid": pine_body_strength_valid,
        }

    def classify_truth(self, truth_assessment):
        hard_blockers = truth_assessment["hard_blockers"]
        soft_blockers = truth_assessment["soft_blockers"]
        truth_primary_reason = truth_assessment["primary_reason"]
        reject_reason = None
        shadow_reason = None
        a_exception_case = "none"
        formal_truth_classification = None
        final_truth_classification = None
        secondary_downgrade_applied = False
        secondary_downgrade_reason = ""

        if (
            truth_assessment["context_truth"] == "false" or
            truth_assessment["setup_truth"] == "false" or
            truth_assessment["trigger_truth"] == "false" or
            truth_assessment["trade_plan_truth"] == "false" or
            truth_assessment["r_truth"] == "poor"
        ):
            reject_reason = (
                "context_false" if truth_assessment["context_truth"] == "false" else
                "setup_false" if truth_assessment["setup_truth"] == "false" else
                "trigger_false" if truth_assessment["trigger_truth"] == "false" else
                "trade_plan_false" if truth_assessment["trade_plan_truth"] == "false" else
                "r_poor"
            )
            formal_truth_classification = "REJECT"
            truth_primary_reason = reject_reason
        elif (
            truth_assessment["context_truth"] == "strong" and
            truth_assessment["setup_truth"] == "strong" and
            truth_assessment["trigger_truth"] in {"valid", "strong"} and
            truth_assessment["trade_plan_truth"] in {"valid", "strong"} and
            truth_assessment["r_truth"] == "efficient" and
            truth_assessment["weak_count"] == 0
        ):
            formal_truth_classification = "A+"
            truth_primary_reason = "formal_a_plus_alignment"
        elif (
            truth_assessment["context_truth"] in {"valid", "strong"} and
            truth_assessment["setup_truth"] in {"valid", "strong"} and
            truth_assessment["trigger_truth"] in {"valid", "strong"} and
            truth_assessment["trade_plan_truth"] in {"valid", "strong"} and
            truth_assessment["r_truth"] in {"acceptable", "efficient"} and
            truth_assessment["weak_count"] == 0
        ):
            formal_truth_classification = "A"
            truth_primary_reason = "formal_a_alignment"
        elif (
            truth_assessment["weak_count"] == 1 and
            truth_assessment["trade_plan_truth"] == "weak" and
            truth_assessment["context_truth"] == "strong" and
            truth_assessment["setup_truth"] in {"valid", "strong"} and
            truth_assessment["trigger_truth"] in {"valid", "strong"} and
            truth_assessment["r_truth"] in {"acceptable", "efficient"} and
            truth_assessment.get("coherent_plan", False)
        ):
            formal_truth_classification = "A"
            a_exception_case = "weak_trade_plan_exception"
            truth_primary_reason = a_exception_case
        elif (
            truth_assessment["weak_count"] == 1 and
            truth_assessment["trigger_truth"] == "weak" and
            truth_assessment["context_truth"] == "strong" and
            truth_assessment["setup_truth"] == "strong" and
            truth_assessment["trade_plan_truth"] in {"valid", "strong"} and
            truth_assessment["r_truth"] == "efficient"
        ):
            formal_truth_classification = "A"
            a_exception_case = "weak_trigger_exception"
            truth_primary_reason = a_exception_case
        else:
            formal_truth_classification = "SHADOW"
            shadow_reason = (
                "context_weak_never_a" if truth_assessment["context_truth"] == "weak" else
                "setup_weak_never_a" if truth_assessment["setup_truth"] == "weak" else
                "multiple_weak_layers" if truth_assessment["weak_count"] >= 2 else
                "trigger_weak_not_exception_eligible" if truth_assessment["trigger_truth"] == "weak" else
                "trade_plan_weak_not_exception_eligible" if truth_assessment["trade_plan_truth"] == "weak" else
                "insufficient_formal_alignment"
            )
            truth_primary_reason = shadow_reason

        final_truth_classification = formal_truth_classification

        if formal_truth_classification == "A+":
            if truth_assessment["trigger_truth"] != "strong":
                secondary_downgrade_applied = True
                secondary_downgrade_reason = "a_plus_trigger_not_strong"
                final_truth_classification = "A"
            elif truth_assessment["trade_plan_truth"] != "strong":
                secondary_downgrade_applied = True
                secondary_downgrade_reason = "a_plus_trade_plan_not_strong"
                final_truth_classification = "A"
            elif soft_blockers:
                secondary_downgrade_applied = True
                secondary_downgrade_reason = "a_plus_soft_blockers_present"
                final_truth_classification = "A"
            elif truth_assessment.get("fallback_driven_plan", False):
                secondary_downgrade_applied = True
                secondary_downgrade_reason = "a_plus_fallback_driven_plan"
                final_truth_classification = "A"
        elif formal_truth_classification == "A":
            if (
                a_exception_case != "none" and
                len(soft_blockers) >= 2
            ):
                secondary_downgrade_applied = True
                secondary_downgrade_reason = "a_exception_with_semantic_tension"
                final_truth_classification = "SHADOW"
            elif (
                a_exception_case == "none" and
                truth_assessment["weak_count"] == 0 and
                truth_assessment["context_truth"] == "valid" and
                truth_assessment["setup_truth"] == "valid" and
                truth_assessment["trigger_truth"] == "valid" and
                truth_assessment["trade_plan_truth"] == "valid" and
                len(soft_blockers) >= 3
            ):
                secondary_downgrade_applied = True
                secondary_downgrade_reason = "a_semantically_too_marginal"
                final_truth_classification = "SHADOW"

        if final_truth_classification == "REJECT":
            truth_primary_reason = reject_reason or truth_primary_reason
        elif final_truth_classification == "SHADOW":
            if secondary_downgrade_applied and secondary_downgrade_reason:
                shadow_reason = secondary_downgrade_reason
            truth_primary_reason = shadow_reason or truth_primary_reason
        elif secondary_downgrade_applied and secondary_downgrade_reason:
            truth_primary_reason = secondary_downgrade_reason

        truth_assessment["formal_truth_classification"] = formal_truth_classification
        truth_assessment["secondary_downgrade_applied"] = secondary_downgrade_applied
        truth_assessment["secondary_downgrade_reason"] = secondary_downgrade_reason
        truth_assessment["final_truth_classification"] = final_truth_classification
        truth_assessment["a_exception_case"] = a_exception_case
        truth_assessment["reject_reason"] = reject_reason
        truth_assessment["shadow_reason"] = shadow_reason
        truth_assessment["final_primary_reason"] = truth_primary_reason
        truth_assessment["truth_classification"] = final_truth_classification
        truth_assessment["truth_primary_reason"] = truth_primary_reason
        logger.info(
            "BOT TRUTH CLASSIFICATION | "
            f"context_truth={truth_assessment['context_truth']} "
            f"regime_truth={truth_assessment['regime_truth']} "
            f"vwap_truth={truth_assessment['vwap_truth']} "
            f"context_5m_truth={truth_assessment['context_5m_truth']} "
            f"structure_1m_truth={truth_assessment['structure_1m_truth']} "
            f"setup_truth={truth_assessment['setup_truth']} "
            f"trigger_truth={truth_assessment['trigger_truth']} "
            f"trade_plan_truth={truth_assessment['trade_plan_truth']} "
            f"trade_plan_reason={truth_assessment.get('trade_plan_reason', '')} "
            f"r_truth={truth_assessment['r_truth']} "
            f"r_reason={truth_assessment.get('r_truth_reason', '')} "
            f"weak_count={truth_assessment['weak_count']} "
            f"formal_truth_classification={formal_truth_classification} "
            f"a_exception_case={a_exception_case} "
            f"secondary_downgrade_applied={secondary_downgrade_applied} "
            f"secondary_downgrade_reason={secondary_downgrade_reason} "
            f"final_truth_classification={final_truth_classification} "
            f"reject_reason={reject_reason} "
            f"shadow_reason={shadow_reason} "
            f"final_primary_reason={truth_primary_reason} "
            f"hard_blockers={truth_assessment['hard_blockers']} "
            f"soft_blockers={truth_assessment['soft_blockers']}"
        )
        return final_truth_classification

    def map_det_truth_to_generic_classification(self, final_truth_classification):
        if final_truth_classification == "A+":
            return "A_PLUS"
        if final_truth_classification in {"A", "SHADOW", "REJECT"}:
            return final_truth_classification
        return "REJECT"

    def resolve_risk_profile(self, classification):
        risk_map = {
            "REJECT": ("NO_EXECUTION_RISK", 0.0),
            "SHADOW": ("NO_EXECUTION_RISK", 0.0),
            "A": ("STANDARD_EXECUTION_RISK", 0.003),
            "A_PLUS": ("PREMIUM_EXECUTION_RISK", 0.005),
            "O": ("STANDARD_EXECUTION_RISK", 0.003),
            "V": ("STANDARD_EXECUTION_RISK", 0.003),
        }
        risk_profile, risk_percent = risk_map.get(
            classification,
            ("NO_EXECUTION_RISK", 0.0),
        )
        allowed_money_risk = round(EXECUTION_CAPITAL_BASE * risk_percent, 10)
        return {
            "classification": classification,
            "risk_profile": risk_profile,
            "risk_percent": risk_percent,
            "allowed_money_risk": allowed_money_risk,
        }

    def resolve_execution_intent(
        self,
        strategy_family,
        classification,
        risk_profile,
        queue_allowed,
    ):
        executable_classifications = {"A", "A_PLUS", "O", "V"}
        executable_risk_profiles = {
            "STANDARD_EXECUTION_RISK",
            "PREMIUM_EXECUTION_RISK",
        }

        if strategy_family not in RECOGNIZED_STRATEGY_FAMILIES:
            logger.info(
                "EXECUTION INTENT FAMILY POLICY | "
                f"strategy_family={strategy_family} "
                "recognized=false "
                "runtime_active=false "
                "decision=deny_execution_intent"
            )
            return {
                "execution_intent_status": "NO_EXECUTION_INTENT",
                "execution_intent_reason": "strategy_family_not_recognized",
                "execution_candidate": False,
                "generic_execution_permission": False,
            }

        if strategy_family not in RUNTIME_ACTIVE_EXECUTION_STRATEGY_FAMILIES:
            logger.info(
                "EXECUTION INTENT FAMILY POLICY | "
                f"strategy_family={strategy_family} "
                "recognized=true "
                "runtime_active=false "
                "decision=deny_execution_intent"
            )
            return {
                "execution_intent_status": "NO_EXECUTION_INTENT",
                "execution_intent_reason": "strategy_family_runtime_inactive",
                "execution_candidate": False,
                "generic_execution_permission": False,
            }

        if classification in {"REJECT", "SHADOW"}:
            return {
                "execution_intent_status": "NO_EXECUTION_INTENT",
                "execution_intent_reason": "classification_not_executable",
                "execution_candidate": False,
                "generic_execution_permission": False,
            }

        if (
            classification in executable_classifications and
            risk_profile in executable_risk_profiles and
            queue_allowed
        ):
            return {
                "execution_intent_status": "EXECUTION_INTENT_READY",
                "execution_intent_reason": "queue_allowed_for_execution",
                "execution_candidate": True,
                "generic_execution_permission": True,
            }

        if (
            classification in executable_classifications and
            risk_profile in executable_risk_profiles
        ):
            return {
                "execution_intent_status": "EXECUTION_INTENT_DENIED",
                "execution_intent_reason": "queue_blocked_for_execution",
                "execution_candidate": False,
                "generic_execution_permission": False,
            }

        return {
            "execution_intent_status": "NO_EXECUTION_INTENT",
            "execution_intent_reason": "risk_profile_not_executable",
            "execution_candidate": False,
            "generic_execution_permission": False,
        }

    def resolve_execution_intent_from_evaluator_result(
        self,
        formal_contract,
        gate_result,
    ):
        return self.resolve_execution_intent(
            formal_contract.get("strategy_family"),
            formal_contract["classification"],
            formal_contract["risk_profile"],
            gate_result["queue_allowed"],
        )

    def build_formal_evaluator_contract(self, normalized, assessment):
        final_truth_classification = assessment.get("final_truth_classification")
        if final_truth_classification is None:
            final_truth_classification = self.classify_truth(assessment)

        if final_truth_classification == "A+":
            det_classification = "EXECUTE_A_PLUS"
        elif final_truth_classification == "A":
            det_classification = "EXECUTE_A"
        else:
            det_classification = final_truth_classification

        execution_permission = final_truth_classification in ("A", "A+")
        execution_grade = ""

        if final_truth_classification == "A+":
            execution_grade = "A+"
        elif final_truth_classification == "A":
            execution_grade = "A"

        classification = self.map_det_truth_to_generic_classification(
            final_truth_classification
        )
        risk_profile_result = self.resolve_risk_profile(classification)

        strategy_family = normalized["strategy_family"]
        if strategy_family not in RECOGNIZED_STRATEGY_FAMILIES:
            strategy_family = "UNKNOWN"

        return {
            "strategy_family": strategy_family,
            "classification": classification,
            "truth_classification": final_truth_classification,
            "det_classification": det_classification,
            "execution_permission": execution_permission,
            "execution_grade": execution_grade,
            "risk_profile": risk_profile_result["risk_profile"],
            "risk_percent": risk_profile_result["risk_percent"],
            "allowed_money_risk": risk_profile_result["allowed_money_risk"],
            "execution_intent_status": "NO_EXECUTION_INTENT",
            "execution_intent_reason": "execution_intent_pending_queue_decision",
            "execution_candidate": False,
            "generic_execution_permission": False,
            "primary_reason": assessment.get("final_primary_reason", assessment.get("truth_primary_reason", assessment["primary_reason"])),
        }

    def build_det_detail(self, normalized, assessment):
        final_truth_classification = assessment.get("final_truth_classification")
        if final_truth_classification is None:
            final_truth_classification = self.classify_truth(assessment)

        return {
            "strategy_family": normalized["strategy_family"],
            "truth_classification": final_truth_classification,
            "formal_truth_classification": assessment.get("formal_truth_classification"),
            "final_truth_classification": final_truth_classification,
            "secondary_reasons": assessment["secondary_reasons"],
            "hard_blockers": assessment["hard_blockers"],
            "soft_blockers": assessment["soft_blockers"],
            "pine_blocker": normalized["blocker"],
            "bot_primary_reason": assessment["primary_reason"],
            "context_truth": assessment["context_truth"],
            "regime_truth": assessment["regime_truth"],
            "vwap_truth": assessment["vwap_truth"],
            "context_5m_truth": assessment["context_5m_truth"],
            "structure_1m_truth": assessment["structure_1m_truth"],
            "setup_truth": assessment["setup_truth"],
            "trigger_truth": assessment["trigger_truth"],
            "trade_plan_truth": assessment["trade_plan_truth"],
            "trade_plan_reason": assessment.get("trade_plan_reason", ""),
            "r_truth": assessment["r_truth"],
            "r_truth_reason": assessment.get("r_truth_reason", ""),
            "weak_count": assessment.get("weak_count", 0),
            "a_exception_case": assessment.get("a_exception_case", "none"),
            "secondary_downgrade_applied": assessment.get("secondary_downgrade_applied", False),
            "secondary_downgrade_reason": assessment.get("secondary_downgrade_reason", ""),
            "reject_reason": assessment.get("reject_reason"),
            "shadow_reason": assessment.get("shadow_reason"),
            "final_primary_reason": assessment.get("final_primary_reason", assessment.get("truth_primary_reason", assessment["primary_reason"])),
            "coherent_plan": assessment.get("coherent_plan", False),
            "fallback_driven_plan": assessment.get("fallback_driven_plan", False),
            "weak_plan_support": assessment.get("weak_plan_support", False),
            "assessment": assessment,
        }

    def evaluate_det_candidate(self, normalized):
        observation_layer = self.build_observation_layer(normalized)
        assessment = self.assess_truth(
            observation_layer,
            observation_layer["market_profile"],
            observation_layer["setup_profile"]
        )
        assessment["truth_classification"] = self.classify_truth(assessment)
        formal_contract = self.build_formal_evaluator_contract(normalized, assessment)
        det_detail = self.build_det_detail(normalized, assessment)

        logger.info(
            "DET EVALUATOR RESULT | "
            f"strategy_family={formal_contract['strategy_family']} "
            f"classification={formal_contract['classification']} "
            f"risk_profile={formal_contract['risk_profile']} "
            f"truth_classification={formal_contract['truth_classification']} "
            f"det_classification={formal_contract['det_classification']} "
            f"execution_grade={formal_contract['execution_grade']} "
            f"primary_reason={formal_contract['primary_reason']}"
        )

        return {
            "formal_contract": formal_contract,
            "det_detail": det_detail,
        }

    def log_det_result(self, normalized, formal_contract, det_detail):
        assessment = det_detail["assessment"]
        assessment_summary = {
            "payload_format": normalized["payload_format"],
            "signal_id": normalized["signal_id"],
            "symbol": normalized["symbol"],
            "side": assessment["derived_side"],
            "market_profile": assessment["market_profile"],
            "setup_profile": assessment["setup_profile"],
            "context_truth": assessment["context_truth"],
            "regime_truth": assessment["regime_truth"],
            "vwap_truth": assessment["vwap_truth"],
            "context_5m_truth": assessment["context_5m_truth"],
            "structure_1m_truth": assessment["structure_1m_truth"],
            "setup_truth": assessment["setup_truth"],
            "trigger_truth": assessment["trigger_truth"],
            "timing_truth": assessment["timing_truth"],
            "trade_plan_truth": assessment["trade_plan_truth"],
            "trade_plan_reason": assessment.get("trade_plan_reason", ""),
            "r_truth": assessment["r_truth"],
            "r_truth_reason": assessment.get("r_truth_reason", ""),
            "weak_count": assessment.get("weak_count", 0),
            "coherent_plan": assessment.get("coherent_plan", False),
            "fallback_driven_plan": assessment.get("fallback_driven_plan", False),
            "weak_plan_support": assessment.get("weak_plan_support", False),
            "hard_blockers": assessment["hard_blockers"],
            "soft_blockers": assessment["soft_blockers"],
        }
        logged_det_detail = {
            key: value
            for key, value in det_detail.items()
            if key != "assessment"
        }

        logger.info(f"DET ASSESSMENT | {json.dumps(assessment_summary, sort_keys=True)}")
        logger.info(f"FORMAL EVALUATOR CONTRACT | {json.dumps(formal_contract, sort_keys=True)}")
        logger.info(f"DET DETAIL | {json.dumps(logged_det_detail, sort_keys=True)}")
        logger.info(
            "DET REASON COMPARE | "
            f"pine_blocker={det_detail['pine_blocker']} "
            f"bot_primary_reason={det_detail['bot_primary_reason']}"
        )

    def evaluate_shadow_test_promotion(self, formal_contract, det_detail):
        stage = BOT_STAGE
        truth_classification = formal_contract["truth_classification"]
        reason_candidates = {
            formal_contract.get("primary_reason"),
            det_detail.get("final_primary_reason"),
            det_detail.get("bot_primary_reason"),
        }

        if stage != "TEST":
            return False, "shadow_test_override_denied_stage_not_test"
        if truth_classification != "SHADOW":
            return False, "shadow_test_override_denied_truth_not_shadow"
        if det_detail.get("hard_blockers"):
            return False, "shadow_test_override_denied_hard_blockers_present"
        if det_detail.get("context_truth") not in {"valid", "strong"}:
            return False, "shadow_test_override_denied_context_not_strong_enough"
        if det_detail.get("setup_truth") not in {"valid", "strong"}:
            return False, "shadow_test_override_denied_setup_not_strong_enough"
        if det_detail.get("trigger_truth") not in {"valid", "strong"}:
            return False, "shadow_test_override_denied_trigger_not_strong_enough"
        if det_detail.get("trade_plan_truth") not in {"valid", "strong"}:
            return False, "shadow_test_override_denied_trade_plan_not_strong_enough"
        if det_detail.get("r_truth") not in {"acceptable", "efficient"}:
            return False, "shadow_test_override_denied_r_not_acceptable"
        if not det_detail.get("coherent_plan", False):
            return False, "shadow_test_override_denied_plan_not_coherent"

        prohibited_match = next(
            (reason for reason in reason_candidates if reason in SHADOW_TEST_PROHIBITED_REASONS),
            None
        )
        if prohibited_match is not None:
            return False, f"shadow_test_override_denied_prohibited_reason:{prohibited_match}"

        return True, "shadow_test_override_allowed_strong_context_shadow"

    def evaluate_det_execution_gate(self, formal_contract, det_detail):
        det_classification = formal_contract["det_classification"]
        truth_classification = formal_contract["truth_classification"]
        stage = BOT_STAGE

        gate_eligible = det_classification in ("EXECUTE_A_PLUS", "EXECUTE_A")
        gate_enabled = stage in {"TEST", "PAPER", "LIVE"}
        gate_branch = "det_execution_gate"
        queue_allowed = gate_enabled and gate_eligible
        execution_lane = "standard"
        promoted_from_shadow = False
        execution_grade = formal_contract["execution_grade"]
        shadow_override_allowed, shadow_override_reason = self.evaluate_shadow_test_promotion(
            formal_contract,
            det_detail,
        )

        if queue_allowed:
            shadow_override_reason = "shadow_test_override_not_applicable_normal_execute_lane"
        elif shadow_override_allowed:
            gate_eligible = True
            queue_allowed = True
            gate_branch = "det_execution_gate_shadow_test_override"
            execution_lane = "shadow_test"
            promoted_from_shadow = True
            execution_grade = "A"

        reason = (
            "DET execution gate evaluated; "
            f"classification={det_classification} truth_classification={truth_classification} "
            f"eligible={gate_eligible} enabled={gate_enabled} stage={stage} "
            f"execution_lane={execution_lane} promoted_from_shadow={promoted_from_shadow} "
            f"shadow_override_reason={shadow_override_reason}"
        )

        logger.info(
            "DET FINAL STATE | "
            f"stage={stage} "
            f"context_truth={det_detail.get('context_truth')} "
            f"regime_truth={det_detail.get('regime_truth')} "
            f"vwap_truth={det_detail.get('vwap_truth')} "
            f"context_5m_truth={det_detail.get('context_5m_truth')} "
            f"structure_1m_truth={det_detail.get('structure_1m_truth')} "
            f"setup_truth={det_detail.get('setup_truth')} "
            f"trigger_truth={det_detail.get('trigger_truth')} "
            f"trade_plan_truth={det_detail.get('trade_plan_truth')} "
            f"trade_plan_reason={det_detail.get('trade_plan_reason', '')} "
            f"r_truth={det_detail.get('r_truth')} "
            f"r_reason={det_detail.get('r_truth_reason', '')} "
            f"weak_count={det_detail.get('weak_count', 0)} "
            f"coherent_plan={det_detail.get('coherent_plan', False)} "
            f"fallback_driven_plan={det_detail.get('fallback_driven_plan', False)} "
            f"weak_plan_support={det_detail.get('weak_plan_support', False)} "
            f"formal_truth_classification={det_detail.get('formal_truth_classification')} "
            f"truth_classification={truth_classification} "
            f"a_exception_case={det_detail.get('a_exception_case', 'none')} "
            f"secondary_downgrade_applied={det_detail.get('secondary_downgrade_applied', False)} "
            f"secondary_downgrade_reason={det_detail.get('secondary_downgrade_reason', '')} "
            f"det_classification={det_classification} "
            f"execution_permission={formal_contract['execution_permission']} "
            f"execution_lane={execution_lane} "
            f"promoted_from_shadow={promoted_from_shadow} "
            f"execution_grade={execution_grade} "
            f"queue_allowed={queue_allowed} "
            f"shadow_override_reason={shadow_override_reason} "
            f"reject_reason={det_detail.get('reject_reason')} "
            f"shadow_reason={det_detail.get('shadow_reason')} "
            f"primary_reason={formal_contract['primary_reason']} "
            f"final_primary_reason={det_detail.get('final_primary_reason', formal_contract['primary_reason'])} "
            f"hard_blockers={det_detail['hard_blockers']} "
            f"soft_blockers={det_detail['soft_blockers']}"
        )

        return {
            "gate_branch": gate_branch,
            "gate_enabled": gate_enabled,
            "gate_eligible": gate_eligible,
            "queue_allowed": queue_allowed,
            "det_classification": det_classification,
            "stage": stage,
            "execution_lane": execution_lane,
            "promoted_from_shadow": promoted_from_shadow,
            "execution_grade": execution_grade,
            "shadow_override_reason": shadow_override_reason,
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
            self.update_trade_commission(trade, fill, report)

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

        for sym in QUALIFIED_FUTURE_SYMBOLS:
            try:
                base = self.build_base_contract(sym)
                details = self.ib.reqContractDetails(base)

                detail = self.select_front_month_detail(details)
                contract = detail.contract

                self.contract_cache[sym] = contract
                self.contract_min_ticks[sym] = float(getattr(detail, "minTick", self.get_instrument_spec(sym)["tick_size"]))

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
        required = set(QUALIFIED_FUTURE_SYMBOLS)
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
            spec = self.get_instrument_spec(symbol)
            if spec["broker_type"] == "forex":
                self.contract_cache[symbol] = self.build_base_contract(symbol)
                self.contract_min_ticks[symbol] = spec["tick_size"]
                logger.info(f"CONTRACT CACHE RECOVERED | symbol={symbol}")
                return True

            base = self.build_base_contract(symbol)
            details = self.ib.reqContractDetails(base)
            detail = self.select_front_month_detail(details)
            contract = detail.contract

            self.contract_cache[symbol] = contract
            self.contract_min_ticks[symbol] = float(getattr(detail, "minTick", spec["tick_size"]))

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

    def allocate_exit_order_ids(self):
        with self.order_id_lock:
            if self.next_order_id is None:
                self.next_order_id = self.ib.client.getReqId()

            tp_id = self.next_order_id
            sl_id = tp_id + 1
            self.next_order_id += 2

        return tp_id, sl_id

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

    def set_execution_validation_status(self, trade_id, validation_status, reason=None):
        valid_statuses = {
            "submitted_to_ib",
            "broker_acknowledged",
            "broker_live",
            "entry_filled",
            "exit_complete",
            "validation_incomplete",
        }
        if validation_status not in valid_statuses:
            return

        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return

            previous_status = record.get("execution_validation_status")
            if previous_status == validation_status:
                return

            record["execution_validation_status"] = validation_status
            now_dt = datetime.now(timezone.utc)
            if validation_status == "broker_acknowledged" and record.get("broker_acknowledged_at") is None:
                record["broker_acknowledged_at"] = now_dt
                if not record.get("broker_acknowledged_counted"):
                    record["broker_acknowledged_counted"] = True
                    self.aggregate_stats["broker_acknowledged_count"] += 1
            if validation_status == "broker_live" and record.get("broker_live_at") is None:
                record["broker_live_at"] = now_dt
                if not record.get("broker_live_counted"):
                    record["broker_live_counted"] = True
                    self.aggregate_stats["broker_live_count"] += 1
                if record.get("broker_acknowledged_at") is None:
                    record["broker_acknowledged_at"] = now_dt
                if not record.get("broker_acknowledged_counted"):
                    record["broker_acknowledged_counted"] = True
                    self.aggregate_stats["broker_acknowledged_count"] += 1

        self.append_trade_event(
            trade_id,
            f"EXECUTION VALIDATION STATUS {previous_status} -> {validation_status}"
            + (f" reason={reason}" if reason else "")
        )

    def log_exit_cleanup_visibility(self, trade_id, record, trigger_label):
        order_ids = {
            "parent": record.get("parent_order_id"),
            "tp": record.get("tp_order_id"),
            "sl": record.get("sl_order_id"),
        }
        visibility = {
            "parent_in_open_trades": False,
            "tp_in_open_trades": False,
            "sl_in_open_trades": False,
            "parent_in_open_orders": False,
            "tp_in_open_orders": False,
            "sl_in_open_orders": False,
        }
        status_by_leg = {"parent": None, "tp": None, "sl": None}
        quantity_by_leg = {"parent": None, "tp": None, "sl": None}

        try:
            for trade in self.ib.openTrades():
                order = getattr(trade, "order", None)
                status = getattr(trade, "orderStatus", None)
                order_id = getattr(order, "orderId", None)
                for leg, expected_id in order_ids.items():
                    if expected_id is not None and order_id == expected_id:
                        visibility[f"{leg}_in_open_trades"] = True
                        status_by_leg[leg] = getattr(status, "status", None)
                        quantity_by_leg[leg] = getattr(order, "totalQuantity", None)

            for order in self.ib.openOrders():
                order_id = getattr(order, "orderId", None)
                for leg, expected_id in order_ids.items():
                    if expected_id is not None and order_id == expected_id:
                        visibility[f"{leg}_in_open_orders"] = True
                        if quantity_by_leg[leg] is None:
                            quantity_by_leg[leg] = getattr(order, "totalQuantity", None)
        except Exception:
            logger.exception(
                "EXIT CLEANUP VISIBILITY CHECK FAILED | "
                f"trade_id={trade_id} trigger={trigger_label}"
            )
            return

        target_active = visibility["tp_in_open_trades"] or visibility["tp_in_open_orders"]
        stop_active = visibility["sl_in_open_trades"] or visibility["sl_in_open_orders"]
        parent_active = visibility["parent_in_open_trades"] or visibility["parent_in_open_orders"]

        orphan_tp = target_active and record.get("exit_reason") == "SL"
        orphan_sl = stop_active and record.get("exit_reason") == "TP"

        snapshot = {
            "trigger": trigger_label,
            "trade_id": trade_id,
            "entry_filled": record.get("entry_filled"),
            "exit_reason": record.get("exit_reason"),
            "target_active": target_active,
            "stop_active": stop_active,
            "parent_active": parent_active,
            "orphan_tp": orphan_tp,
            "orphan_sl": orphan_sl,
            "status_by_leg": status_by_leg,
            "quantity_by_leg": quantity_by_leg,
            "visibility": visibility,
        }

        with self.trade_analysis_lock:
            live_record = self.trade_analysis.get(trade_id)
            if live_record is not None:
                live_record["exit_cleanup_last_snapshot"] = snapshot

        logger.info(f"EXIT CLEANUP VISIBILITY | {json.dumps(snapshot, sort_keys=True)}")

    def get_timeout_retained_child_snapshot(self, record, retained_quantity, allow_parentless_children=None, child_order_ids=None):
        terminal_statuses = {"Cancelled", "ApiCancelled", "Inactive", "Filled"}
        expected_parent_id = record.get("parent_order_id")
        child_action = "SELL" if record.get("side") == "long" else "BUY"
        target_price = record.get("target_price")
        stop_price = record.get("stop_price")
        if allow_parentless_children is None:
            allow_parentless_children = bool(record.get("timeout_retained_replacement_parentless_allowed"))
        if child_order_ids is None:
            child_order_ids = {
                "tp": record.get("tp_order_id"),
                "sl": record.get("sl_order_id"),
            }
        child_orders = {
            "tp": {
                "configured_order_id": child_order_ids.get("tp"),
                "order_id": child_order_ids.get("tp"),
                "visible_in_open_trades": False,
                "visible_in_open_orders": False,
                "status": None,
                "perm_id": None,
                "parent_id": None,
                "quantity": None,
                "candidate_order_ids": [],
                "candidate_count": 0,
                "matched_by_signature": False,
                "resolved_match_source": None,
                "exact_match_found": False,
                "signature_candidates": {},
            },
            "sl": {
                "configured_order_id": child_order_ids.get("sl"),
                "order_id": child_order_ids.get("sl"),
                "visible_in_open_trades": False,
                "visible_in_open_orders": False,
                "status": None,
                "perm_id": None,
                "parent_id": None,
                "quantity": None,
                "candidate_order_ids": [],
                "candidate_count": 0,
                "matched_by_signature": False,
                "resolved_match_source": None,
                "exact_match_found": False,
                "signature_candidates": {},
            },
        }
        expected_ids = {
            leg_data["configured_order_id"]
            for leg_data in child_orders.values()
            if leg_data["configured_order_id"] is not None
        }

        def price_matches(left, right):
            try:
                if left is None or right is None:
                    return False
                return abs(float(left) - float(right)) < 1e-9
            except Exception:
                return False

        def leg_matches_signature(leg_name, order, contract):
            if order is None or contract is None or not self.contract_matches_symbol(contract, record["symbol"]):
                return False

            if getattr(order, "action", None) != child_action:
                return False

            parent_id = getattr(order, "parentId", None)
            if parent_id not in (expected_parent_id, None, 0):
                return False

            order_type = str(getattr(order, "orderType", "") or "").upper()
            if leg_name == "tp":
                return order_type == "LMT" and price_matches(getattr(order, "lmtPrice", None), target_price)

            return order_type == "STP" and price_matches(getattr(order, "auxPrice", None), stop_price)

        def update_resolved_leg_data(leg_data, order, status=None, source=None, matched_by_signature=False):
            leg_data["order_id"] = getattr(order, "orderId", None)
            leg_data["matched_by_signature"] = matched_by_signature
            leg_data["resolved_match_source"] = (
                "signature_fallback" if matched_by_signature else "configured_order_id"
            )
            if source == "open_trade":
                leg_data["visible_in_open_trades"] = True
            elif source == "open_order":
                leg_data["visible_in_open_orders"] = True
            if status is not None:
                leg_data["status"] = getattr(status, "status", None)
                if leg_data["perm_id"] in (None, 0):
                    leg_data["perm_id"] = getattr(status, "permId", None)
            if leg_data["parent_id"] is None:
                leg_data["parent_id"] = getattr(order, "parentId", None)
            if leg_data["quantity"] is None:
                leg_data["quantity"] = getattr(order, "totalQuantity", None)

        def register_candidate(leg_name, order, status=None, source=None, matched_by_signature=False):
            if order is None:
                return

            leg_data = child_orders[leg_name]
            order_id = getattr(order, "orderId", None)
            if order_id is None:
                return

            if order_id not in leg_data["candidate_order_ids"]:
                leg_data["candidate_order_ids"].append(order_id)

            if not matched_by_signature:
                leg_data["exact_match_found"] = True
                update_resolved_leg_data(
                    leg_data,
                    order,
                    status=status,
                    source=source,
                    matched_by_signature=False
                )
                return

            signature_candidates = leg_data["signature_candidates"]
            candidate = signature_candidates.get(order_id)
            if candidate is None:
                signature_candidates[order_id] = {
                    "order": order,
                    "status": status,
                    "source": source,
                }
                return

            if candidate.get("source") != "open_trade" and source == "open_trade":
                candidate["order"] = order
                candidate["source"] = source

            if candidate.get("status") is None and status is not None:
                candidate["status"] = status

        try:
            for trade in self.ib.openTrades():
                order = getattr(trade, "order", None)
                status = getattr(trade, "orderStatus", None)
                contract = getattr(trade, "contract", None)
                order_id = getattr(order, "orderId", None)

                if order_id in expected_ids:
                    for leg_name, leg_data in child_orders.items():
                        if leg_data["configured_order_id"] == order_id:
                            register_candidate(
                                leg_name,
                                order,
                                status=status,
                                source="open_trade",
                                matched_by_signature=False
                            )
                            break
                    continue

                for leg_name in ("tp", "sl"):
                    if child_orders[leg_name]["exact_match_found"]:
                        continue
                    if leg_matches_signature(leg_name, order, contract):
                        register_candidate(
                            leg_name,
                            order,
                            status=status,
                            source="open_trade",
                            matched_by_signature=True
                        )
                        break

            for order in self.ib.openOrders():
                order_id = getattr(order, "orderId", None)
                contract = getattr(order, "contract", None)

                if order_id in expected_ids:
                    for leg_name, leg_data in child_orders.items():
                        if leg_data["configured_order_id"] == order_id:
                            register_candidate(
                                leg_name,
                                order,
                                source="open_order",
                                matched_by_signature=False
                            )
                            break
                    continue

                for leg_name in ("tp", "sl"):
                    if child_orders[leg_name]["exact_match_found"]:
                        continue
                    if leg_matches_signature(leg_name, order, contract):
                        register_candidate(
                            leg_name,
                            order,
                            source="open_order",
                            matched_by_signature=True
                        )
                        break
        except Exception as exc:
            return {
                "ok": False,
                "coherent": False,
                "reason": f"broker_child_snapshot_failed:{exc}",
                "replace_required": True,
                "child_orders": child_orders,
            }

        retained_quantity = float(retained_quantity or 0.0)
        visible_child_count = 0
        quantity_match_count = 0
        link_match_count = 0
        terminal_child_count = 0

        for leg_data in child_orders.values():
            if not leg_data["exact_match_found"]:
                signature_candidates = leg_data["signature_candidates"]
                unique_signature_order_ids = sorted(signature_candidates.keys())
                leg_data["signature_candidate_order_ids"] = unique_signature_order_ids
                leg_data["signature_candidate_count"] = len(unique_signature_order_ids)

                if len(unique_signature_order_ids) == 1:
                    signature_candidate = signature_candidates[unique_signature_order_ids[0]]
                    update_resolved_leg_data(
                        leg_data,
                        signature_candidate["order"],
                        status=signature_candidate["status"],
                        source=signature_candidate["source"],
                        matched_by_signature=True
                    )
                elif len(unique_signature_order_ids) > 1:
                    leg_data["order_id"] = None
                    leg_data["matched_by_signature"] = False
                    leg_data["resolved_match_source"] = None
                    leg_data["visible_in_open_trades"] = False
                    leg_data["visible_in_open_orders"] = False
                    leg_data["status"] = None
                    leg_data["perm_id"] = None
                    leg_data["parent_id"] = None
                    leg_data["quantity"] = None
            else:
                leg_data["signature_candidate_order_ids"] = []
                leg_data["signature_candidate_count"] = 0

            leg_data["candidate_order_ids"] = sorted(set(leg_data["candidate_order_ids"]))
            leg_data["candidate_count"] = len(leg_data["candidate_order_ids"])
            leg_data["visible"] = (
                leg_data["candidate_count"] > 0 or
                leg_data["visible_in_open_trades"] or
                leg_data["visible_in_open_orders"]
            )
            if leg_data["visible"]:
                visible_child_count += 1

            quantity_value = leg_data["quantity"]
            try:
                quantity_value = None if quantity_value is None else float(quantity_value)
            except Exception:
                quantity_value = None
            leg_data["quantity"] = quantity_value
            leg_data["quantity_matches"] = (
                quantity_value is not None and
                abs(quantity_value - retained_quantity) < 1e-9
            )
            if leg_data["quantity_matches"]:
                quantity_match_count += 1

            if allow_parentless_children:
                leg_data["link_ok"] = leg_data["parent_id"] in (expected_parent_id, None, 0)
            else:
                leg_data["link_ok"] = leg_data["parent_id"] == expected_parent_id
            if leg_data["link_ok"]:
                link_match_count += 1

            leg_data["terminal_status"] = leg_data["status"] in terminal_statuses
            if leg_data["terminal_status"]:
                terminal_child_count += 1

        candidate_count_match = all(
            leg_data["candidate_count"] == 1
            for leg_data in child_orders.values()
        )

        if candidate_count_match and visible_child_count == 2 and quantity_match_count == 2 and link_match_count == 2 and terminal_child_count == 0:
            coherent = True
            reason = "timeout_retained_children_match_retained_quantity"
        elif not candidate_count_match or visible_child_count != 2:
            coherent = False
            reason = "timeout_retained_children_missing_or_ambiguous"
        elif quantity_match_count != 2:
            coherent = False
            reason = "timeout_retained_children_quantity_mismatch"
        elif link_match_count != 2:
            coherent = False
            reason = "timeout_retained_children_link_mismatch"
        elif terminal_child_count > 0:
            coherent = False
            reason = "timeout_retained_children_terminal_status"
        else:
            coherent = False
            reason = "timeout_retained_children_not_coherent"

        return {
            "ok": True,
            "coherent": coherent,
            "reason": reason,
            "replace_required": not coherent,
            "visible_child_count": visible_child_count,
            "quantity_match_count": quantity_match_count,
            "link_match_count": link_match_count,
            "terminal_child_count": terminal_child_count,
            "retained_quantity": retained_quantity,
            "child_orders": child_orders,
        }

    def cancel_timeout_retained_child_orders(self, trade_id, record, child_snapshot):
        cancel_results = {}
        cancel_failures = []

        for leg_name in ("tp", "sl"):
            leg_data = child_snapshot["child_orders"][leg_name]
            order_ids = sorted(set(leg_data.get("candidate_order_ids") or []))

            if not order_ids:
                cancel_results[leg_name] = {
                    "requested": False,
                    "ok": True,
                    "reason": "not_visible_no_cancel_needed",
                    "order_results": [],
                }
                continue

            leg_cancel_failures = []
            leg_order_results = []

            for order_id in order_ids:
                self.append_trade_event(
                    trade_id,
                    f"TIMEOUT RETAINED CHILD CANCEL REQUESTED leg={leg_name} order_id={order_id}"
                )
                logger.warning(
                    "TIMEOUT RETAINED CHILD CANCEL REQUESTED | "
                    f"trade_id={trade_id} "
                    f"symbol={record['symbol']} "
                    f"leg={leg_name} "
                    f"order_id={order_id}"
                )

                cancel_target = None
                cancel_reason = None
                try:
                    for trade in self.ib.openTrades():
                        order = getattr(trade, "order", None)
                        if getattr(order, "orderId", None) == order_id:
                            cancel_target = order
                            cancel_reason = "cancelled_via_open_trade"
                            break

                    if cancel_target is None:
                        for order in self.ib.openOrders():
                            if getattr(order, "orderId", None) == order_id:
                                cancel_target = order
                                cancel_reason = "cancelled_via_open_order"
                                break

                    if cancel_target is None:
                        order_result = {
                            "order_id": order_id,
                            "requested": True,
                            "ok": False,
                            "reason": "visible_child_not_found_for_cancel",
                        }
                        leg_order_results.append(order_result)
                        leg_cancel_failures.append(f"{leg_name}:{order_id}:visible_child_not_found_for_cancel")
                    else:
                        self.ib.cancelOrder(cancel_target)
                        order_result = {
                            "order_id": order_id,
                            "requested": True,
                            "ok": True,
                            "reason": cancel_reason,
                        }
                        leg_order_results.append(order_result)
                except Exception as exc:
                    order_result = {
                        "order_id": order_id,
                        "requested": True,
                        "ok": False,
                        "reason": f"cancel_exception:{exc}",
                    }
                    leg_order_results.append(order_result)
                    leg_cancel_failures.append(f"{leg_name}:{order_id}:cancel_exception:{exc}")

                self.append_trade_event(
                    trade_id,
                    f"TIMEOUT RETAINED CHILD CANCEL PER ORDER RESULT leg={leg_name} order_id={order_id} "
                    f"ok={order_result['ok']} reason={order_result['reason']}"
                )

            logger.warning(
                "TIMEOUT RETAINED CHILD CANCEL RESULT | "
                f"trade_id={trade_id} "
                f"symbol={record['symbol']} "
                f"leg={leg_name} "
                f"requested={bool(leg_order_results)} "
                f"ok={not leg_cancel_failures} "
                f"reason={'all_cancel_requests_submitted' if not leg_cancel_failures else ','.join(leg_cancel_failures)}"
            )

            cancel_results[leg_name] = {
                "requested": bool(leg_order_results),
                "ok": not leg_cancel_failures,
                "reason": "all_cancel_requests_submitted" if not leg_cancel_failures else ",".join(leg_cancel_failures),
                "order_results": leg_order_results,
            }
            cancel_failures.extend(leg_cancel_failures)

        return {
            "ok": not cancel_failures,
            "failures": cancel_failures,
            "results": cancel_results,
        }

    def fail_closed_timeout_retained_child_reconcile(self, trade_id, reason):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return False
            self.append_anomaly(trade_id, "TIMEOUT_RETAINED_CHILD_RECONCILE_FAILED")
            record["state"] = "INCOMPLETE"

        self.set_execution_validation_status(
            trade_id,
            "validation_incomplete",
            reason
        )
        self.append_trade_event(
            trade_id,
            f"TIMEOUT RETAINED CHILD RECONCILE FINAL DECISION fail_closed_incomplete reason={reason}"
        )
        logger.error(
            "TIMEOUT RETAINED CHILD RECONCILE FINAL DECISION | "
            f"trade_id={trade_id} "
            "decision=fail_closed_incomplete "
            f"reason={reason}"
        )
        self.log_exit_cleanup_visibility(
            trade_id,
            self.trade_analysis.get(trade_id, {}),
            "TIMEOUT_RETAINED_CHILD_RECONCILE_FAIL_CLOSED"
        )
        return False

    def place_timeout_retained_replacement_protection(self, trade_id, record, retained_quantity):
        symbol = record["symbol"]
        side = record["side"]
        child_action = "SELL" if side == "long" else "BUY"
        target_price = record["target_price"]
        stop_price = record["stop_price"]
        tp_id, sl_id = self.allocate_exit_order_ids()
        oca_group = f"{trade_id}_timeout_retained_{tp_id}"
        contract = self.get_contract(symbol)

        tp = LimitOrder(child_action, retained_quantity, target_price)
        tp.orderId = tp_id
        tp.transmit = False
        tp.tif = "GTC"
        tp.ocaGroup = oca_group
        tp.ocaType = 1

        sl = StopOrder(child_action, retained_quantity, stop_price)
        sl.orderId = sl_id
        sl.transmit = True
        sl.tif = "GTC"
        sl.ocaGroup = oca_group
        sl.ocaType = 1

        self.append_trade_event(
            trade_id,
            f"TIMEOUT RETAINED REPLACEMENT TP SUBMIT REQUESTED order_id={tp_id} quantity={retained_quantity} price={target_price}"
        )
        self.append_trade_event(
            trade_id,
            f"TIMEOUT RETAINED REPLACEMENT SL SUBMIT REQUESTED order_id={sl_id} quantity={retained_quantity} price={stop_price}"
        )
        logger.warning(
            "TIMEOUT RETAINED REPLACEMENT TP SUBMIT REQUESTED | "
            f"trade_id={trade_id} symbol={symbol} order_id={tp_id} "
            f"quantity={retained_quantity} target_price={target_price}"
        )
        logger.warning(
            "TIMEOUT RETAINED REPLACEMENT SL SUBMIT REQUESTED | "
            f"trade_id={trade_id} symbol={symbol} order_id={sl_id} "
            f"quantity={retained_quantity} stop_price={stop_price}"
        )

        cleanup_snapshot = {
            "child_orders": {
                "tp": {
                    "candidate_order_ids": [tp_id],
                },
                "sl": {
                    "candidate_order_ids": [sl_id],
                },
            }
        }

        try:
            tp_trade = self.ib.placeOrder(contract, tp)
            sl_trade = self.ib.placeOrder(contract, sl)
            self.log_trade_snapshot("TIMEOUT RETAINED REPLACEMENT TP", tp_trade)
            self.log_trade_snapshot("TIMEOUT RETAINED REPLACEMENT SL", sl_trade)
            self.ib.sleep(0.20)
        except Exception as exc:
            logger.exception(
                "TIMEOUT RETAINED REPLACEMENT PROTECTION SUBMIT FAILED | "
                f"trade_id={trade_id} symbol={symbol}"
            )
            cleanup_result = self.cancel_timeout_retained_child_orders(
                trade_id,
                record,
                cleanup_snapshot
            )
            self.append_trade_event(
                trade_id,
                f"TIMEOUT RETAINED REPLACEMENT CLEANUP AFTER SUBMIT FAILURE ok={cleanup_result['ok']} failures={cleanup_result['failures']}"
            )
            return {
                "ok": False,
                "reason": f"replacement_submit_exception:{exc}",
            }

        with self.trade_analysis_lock:
            live_record = self.trade_analysis.get(trade_id)
            if live_record is None:
                cleanup_result = self.cancel_timeout_retained_child_orders(
                    trade_id,
                    record,
                    cleanup_snapshot
                )
                self.append_trade_event(
                    trade_id,
                    f"TIMEOUT RETAINED REPLACEMENT CLEANUP AFTER TRADE MISSING ok={cleanup_result['ok']} failures={cleanup_result['failures']}"
                )
                return {
                    "ok": False,
                    "reason": "trade_missing_after_replacement_submit",
                }
            retired_tp_order_id = live_record.get("tp_order_id")
            retired_sl_order_id = live_record.get("sl_order_id")
            live_record["tp_order_id"] = tp_id
            live_record["sl_order_id"] = sl_id
            live_record["tp_perm_id"] = None
            live_record["sl_perm_id"] = None
            live_record["state"] = "EXIT_WORKING"
            live_record["timeout_retained_replacement_parentless_allowed"] = True
            if (
                retired_tp_order_id is not None and
                retired_tp_order_id != tp_id and
                self.order_to_trade.get(retired_tp_order_id) == trade_id
            ):
                self.order_to_trade.pop(retired_tp_order_id, None)
            if (
                retired_sl_order_id is not None and
                retired_sl_order_id != sl_id and
                self.order_to_trade.get(retired_sl_order_id) == trade_id
            ):
                self.order_to_trade.pop(retired_sl_order_id, None)
            self.order_to_trade[tp_id] = trade_id
            self.order_to_trade[sl_id] = trade_id
            self.append_trade_event(
                trade_id,
                f"TIMEOUT RETAINED REPLACEMENT PROTECTION REGISTERED tp_order_id={tp_id} sl_order_id={sl_id} quantity={retained_quantity}"
            )
            self.append_trade_event(
                trade_id,
                f"TIMEOUT RETAINED REPLACEMENT RETIRED CHILD ROUTES retired_tp_order_id={retired_tp_order_id} "
                f"retired_sl_order_id={retired_sl_order_id}"
            )

        replacement_snapshot = self.get_timeout_retained_child_snapshot(
            self.trade_analysis.get(trade_id),
            retained_quantity,
            allow_parentless_children=True
        )
        logger.warning(
            "TIMEOUT RETAINED REPLACEMENT PROTECTION RESULT | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"coherent={replacement_snapshot.get('coherent')} "
            f"reason={replacement_snapshot.get('reason')} "
            f"child_orders={json.dumps(replacement_snapshot.get('child_orders', {}), sort_keys=True)}"
        )

        if not replacement_snapshot.get("ok") or not replacement_snapshot.get("coherent"):
            cleanup_result = self.cancel_timeout_retained_child_orders(
                trade_id,
                self.trade_analysis.get(trade_id, record),
                cleanup_snapshot
            )
            self.append_trade_event(
                trade_id,
                f"TIMEOUT RETAINED REPLACEMENT CLEANUP AFTER INCOHERENT RECHECK ok={cleanup_result['ok']} failures={cleanup_result['failures']}"
            )
            logger.error(
                "TIMEOUT RETAINED REPLACEMENT PROTECTION INCOHERENT | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"cleanup_ok={cleanup_result['ok']} "
                f"cleanup_failures={cleanup_result['failures']} "
                f"reason={replacement_snapshot.get('reason')}"
            )
            return {
                "ok": False,
                "reason": f"replacement_protection_not_coherent:{replacement_snapshot.get('reason')}",
            }

        return {
            "ok": True,
            "reason": "replacement_protection_coherent",
            "tp_order_id": tp_id,
            "sl_order_id": sl_id,
        }

    def reconcile_timeout_retained_child_protection(self, trade_id):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return False
            retained_quantity = float(record.get("realized_entry_quantity") or 0.0)
            if retained_quantity <= 0 or not record.get("entry_filled") or not record.get("partial_entry_timeout_triggered"):
                return self.fail_closed_timeout_retained_child_reconcile(
                    trade_id,
                    "timeout_retained_child_reconcile_preconditions_not_met"
                )
            record_snapshot = dict(record)

        self.append_trade_event(
            trade_id,
            f"TIMEOUT RETAINED CHILD RECONCILE STARTED retained_quantity={retained_quantity}"
        )
        logger.warning(
            "TIMEOUT RETAINED CHILD RECONCILE STARTED | "
            f"trade_id={trade_id} "
            f"symbol={record_snapshot['symbol']} "
            f"retained_quantity={retained_quantity}"
        )

        child_snapshot = self.get_timeout_retained_child_snapshot(record_snapshot, retained_quantity)
        logger.warning(
            "TIMEOUT RETAINED CHILD RECONCILE PRECHECK | "
            f"trade_id={trade_id} "
            f"symbol={record_snapshot['symbol']} "
            f"retained_quantity={retained_quantity} "
            f"coherent={child_snapshot.get('coherent')} "
            f"reason={child_snapshot.get('reason')} "
            f"child_orders={json.dumps(child_snapshot.get('child_orders', {}), sort_keys=True)}"
        )

        if not child_snapshot.get("ok"):
            return self.fail_closed_timeout_retained_child_reconcile(
                trade_id,
                child_snapshot.get("reason") or "timeout_retained_child_snapshot_failed"
            )

        if child_snapshot.get("coherent"):
            with self.trade_analysis_lock:
                live_record = self.trade_analysis.get(trade_id)
                if live_record is not None:
                    live_record["state"] = "EXIT_WORKING"
                    self.append_trade_event(
                        trade_id,
                        "TIMEOUT RETAINED CHILD RECONCILE FINAL DECISION continue_protected reason=child_reality_already_coherent"
                    )
            logger.warning(
                "TIMEOUT RETAINED CHILD RECONCILE FINAL DECISION | "
                f"trade_id={trade_id} "
                "decision=continue_protected "
                "reason=child_reality_already_coherent"
            )
            self.log_exit_cleanup_visibility(
                trade_id,
                self.trade_analysis.get(trade_id, {}),
                "TIMEOUT_RETAINED_CHILD_RECONCILE_PASSED"
            )
            return True

        cancel_result = self.cancel_timeout_retained_child_orders(
            trade_id,
            record_snapshot,
            child_snapshot
        )
        if not cancel_result["ok"]:
            return self.fail_closed_timeout_retained_child_reconcile(
                trade_id,
                f"timeout_retained_child_cancel_failed:{','.join(cancel_result['failures'])}"
            )

        stale_child_order_ids = {
            "tp": record_snapshot.get("tp_order_id"),
            "sl": record_snapshot.get("sl_order_id"),
        }
        self.ib.sleep(0.20)
        post_cancel_snapshot = self.get_timeout_retained_child_snapshot(
            record_snapshot,
            retained_quantity,
            child_order_ids=stale_child_order_ids,
        )
        logger.warning(
            "TIMEOUT RETAINED CHILD POST-CANCEL CHECK | "
            f"trade_id={trade_id} "
            f"symbol={record_snapshot['symbol']} "
            f"visible_child_count={post_cancel_snapshot.get('visible_child_count')} "
            f"coherent={post_cancel_snapshot.get('coherent')} "
            f"reason={post_cancel_snapshot.get('reason')} "
            f"child_orders={json.dumps(post_cancel_snapshot.get('child_orders', {}), sort_keys=True)}"
        )
        if not post_cancel_snapshot.get("ok"):
            return self.fail_closed_timeout_retained_child_reconcile(
                trade_id,
                post_cancel_snapshot.get("reason") or "timeout_retained_child_post_cancel_snapshot_failed"
            )
        if post_cancel_snapshot.get("visible_child_count", 0) > 0:
            return self.fail_closed_timeout_retained_child_reconcile(
                trade_id,
                f"timeout_retained_child_stale_visibility_after_cancel:{post_cancel_snapshot.get('reason')}"
            )

        replacement_result = self.place_timeout_retained_replacement_protection(
            trade_id,
            record_snapshot,
            retained_quantity
        )
        if not replacement_result["ok"]:
            return self.fail_closed_timeout_retained_child_reconcile(
                trade_id,
                replacement_result["reason"]
            )

        with self.trade_analysis_lock:
            live_record = self.trade_analysis.get(trade_id)
            if live_record is not None:
                live_record["state"] = "EXIT_WORKING"
                self.append_trade_event(
                    trade_id,
                    "TIMEOUT RETAINED CHILD RECONCILE FINAL DECISION continue_protected reason=replacement_protection_coherent"
                )

        logger.warning(
            "TIMEOUT RETAINED CHILD RECONCILE FINAL DECISION | "
            f"trade_id={trade_id} "
            "decision=continue_protected "
            "reason=replacement_protection_coherent"
        )
        self.log_exit_cleanup_visibility(
            trade_id,
            self.trade_analysis.get(trade_id, {}),
            "TIMEOUT_RETAINED_CHILD_RECONCILE_REPLACED"
        )
        return True

    def assess_broker_bracket_confirmation(self, parent_id, tp_id, sl_id):
        terminal_statuses = {"Cancelled", "ApiCancelled", "Inactive"}
        leg_definitions = {
            "parent": {"orderId": parent_id, "expected_parent_id": None},
            "tp": {"orderId": tp_id, "expected_parent_id": parent_id},
            "sl": {"orderId": sl_id, "expected_parent_id": parent_id},
        }
        expected_ids = {
            leg_data["orderId"]
            for leg_data in leg_definitions.values()
            if leg_data["orderId"] is not None
        }
        broker_orders = {
            leg_name: {
                "orderId": leg_data["orderId"],
                "expected_parent_id": leg_data["expected_parent_id"],
                "parentId": None,
                "permId": None,
                "status": None,
                "visible_in_open_trades": False,
                "visible_in_open_orders": False,
                "link_ok": False,
                "submitted_to_ib": False,
                "broker_acknowledged": False,
                "broker_live": False,
            }
            for leg_name, leg_data in leg_definitions.items()
        }

        try:
            for trade in self.ib.openTrades():
                order = getattr(trade, "order", None)
                status = getattr(trade, "orderStatus", None)
                order_id = getattr(order, "orderId", None)
                if order_id not in expected_ids:
                    continue
                for leg_name, leg_data in broker_orders.items():
                    if leg_data["orderId"] == order_id:
                        leg_data["visible_in_open_trades"] = True
                        leg_data["parentId"] = getattr(order, "parentId", None)
                        leg_data["permId"] = getattr(status, "permId", None)
                        leg_data["status"] = getattr(status, "status", None)
                        break

            for order in self.ib.openOrders():
                order_id = getattr(order, "orderId", None)
                if order_id not in expected_ids:
                    continue
                for leg_name, leg_data in broker_orders.items():
                    if leg_data["orderId"] == order_id:
                        leg_data["visible_in_open_orders"] = True
                        if leg_data["parentId"] is None:
                            leg_data["parentId"] = getattr(order, "parentId", None)
                        if leg_data["permId"] in (None, 0):
                            leg_data["permId"] = getattr(order, "permId", None)
                        break
        except Exception:
            logger.exception(
                "BROKER BRACKET CONFIRMATION CHECK FAILED | "
                f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id}"
            )
            return {
                "outcome": "BRACKET_INCOMPLETE",
                "confirmed": False,
                "reason": "broker_state_check_failed",
                "visible_count": 0,
                "any_visible": False,
                "submitted_unacknowledged": False,
                "broker_state_category": "BROKEN_OR_TERMINAL",
                "visible_order_ids": [],
                "parent_visible": False,
                "tp_visible": False,
                "sl_visible": False,
                "parent_link_ok": False,
                "tp_link_ok": False,
                "sl_link_ok": False,
                "all_perm_ids_assigned": False,
                "pending_broker_ack": False,
                "all_statuses_pending_ack": False,
                "all_statuses_visible_ack": False,
                "all_live_ack_statuses": False,
                "all_broker_acknowledged": False,
                "all_broker_live": False,
                "any_terminal_status": False,
                "ambiguous_broker_state": False,
                "broken_or_terminal_state": True,
                "visible_statuses": [],
                "broker_orders": {},
            }

        for leg_name, leg_data in broker_orders.items():
            if leg_name == "parent":
                leg_data["link_ok"] = leg_data["parentId"] in (0, None)
            else:
                leg_data["link_ok"] = leg_data["parentId"] == leg_data["expected_parent_id"]

            status_value = leg_data["status"]
            has_status_evidence = (
                status_value not in (None, "") and (
                    status_value in BRACKET_PENDING_ACK_STATUSES or
                    status_value in BRACKET_CONFIRM_STATUSES or
                    status_value in terminal_statuses
                )
            )
            leg_data["submitted_to_ib"] = (
                leg_data["visible_in_open_trades"] or
                leg_data["visible_in_open_orders"] or
                has_status_evidence
            )
            leg_data["broker_acknowledged"] = (
                leg_data["permId"] not in (None, 0) or
                leg_data["status"] in {"PreSubmitted", "Submitted", "Filled"}
            )
            leg_data["broker_live"] = leg_data["status"] in BRACKET_LIVE_ACK_STATUSES

        parent_visible = broker_orders["parent"]["submitted_to_ib"]
        tp_visible = broker_orders["tp"]["submitted_to_ib"]
        sl_visible = broker_orders["sl"]["submitted_to_ib"]

        parent_link_ok = parent_visible and broker_orders["parent"]["link_ok"]
        tp_link_ok = tp_visible and broker_orders["tp"]["link_ok"]
        sl_link_ok = sl_visible and broker_orders["sl"]["link_ok"]

        any_visible = any(
            leg_data["submitted_to_ib"] for leg_data in broker_orders.values()
        )
        all_visible = parent_visible and tp_visible and sl_visible
        all_links_ok = parent_link_ok and tp_link_ok and sl_link_ok
        all_perm_ids_assigned = all(
            leg_data.get("permId") not in (None, 0)
            for leg_data in broker_orders.values()
        ) and all_visible
        visible_statuses = [
            leg_data.get("status")
            for leg_data in broker_orders.values()
            if leg_data["submitted_to_ib"]
        ]
        all_statuses_pending_ack = (
            all_visible and
            all(status in BRACKET_PENDING_ACK_STATUSES for status in visible_statuses)
        )
        all_statuses_visible_ack = (
            all_visible and
            all(
                status in BRACKET_CONFIRM_STATUSES
                for status in visible_statuses
            )
        )
        all_live_ack_statuses = (
            all_visible and
            all(status in BRACKET_LIVE_ACK_STATUSES for status in visible_statuses)
        )
        all_broker_acknowledged = (
            all_visible and
            all_links_ok and
            all(leg_data["broker_acknowledged"] for leg_data in broker_orders.values())
        )
        any_broker_acknowledged = any(
            leg_data["broker_acknowledged"] for leg_data in broker_orders.values()
        )
        all_broker_live = (
            all_visible and
            all_links_ok and
            all(leg_data["broker_live"] for leg_data in broker_orders.values())
        )
        any_terminal_status = any(
            status in terminal_statuses
            for status in visible_statuses
        )
        if all_broker_live:
            broker_state_category = "ACKNOWLEDGED"
            outcome = "BRACKET_BROKER_ACKNOWLEDGED"
            reason = "broker_live_three_leg_chain"
        elif all_broker_acknowledged:
            broker_state_category = "ACKNOWLEDGED"
            outcome = "BRACKET_BROKER_ACKNOWLEDGED"
            if all_perm_ids_assigned:
                reason = "broker_acknowledged_three_leg_chain"
            else:
                reason = "broker_status_acknowledged_three_leg_chain"
        elif any_terminal_status:
            broker_state_category = "BROKEN_OR_TERMINAL"
            outcome = "BRACKET_BROKER_STATE_BROKEN_OR_TERMINAL"
            reason = "broker_terminal_or_inactive_visibility"
        elif all_visible and all_links_ok and not any_broker_acknowledged:
            broker_state_category = "WAITABLE_ACK"
            outcome = "BRACKET_SUBMITTED_UNACKNOWLEDGED"
            reason = "broker_visible_pending_broker_ack"
        elif any_visible:
            broker_state_category = "AMBIGUOUS_ACK"
            outcome = "BRACKET_BROKER_STATE_AMBIGUOUS"
            if not all_visible:
                reason = "broker_partial_visibility_after_submit"
            elif not all_links_ok:
                reason = "broker_bracket_linkage_unresolved"
            elif any_broker_acknowledged:
                reason = "broker_partial_acknowledgement_requires_lock"
            else:
                reason = "broker_visible_state_requires_quarantine"
        else:
            broker_state_category = "BROKEN_OR_TERMINAL"
            outcome = "BRACKET_BROKER_STATE_BROKEN_OR_TERMINAL"
            reason = "broker_no_visible_bracket_state"

        confirmed = broker_state_category == "ACKNOWLEDGED"
        pending_broker_ack = broker_state_category == "WAITABLE_ACK"
        ambiguous_broker_state = broker_state_category == "AMBIGUOUS_ACK"
        broken_or_terminal_state = broker_state_category == "BROKEN_OR_TERMINAL"
        submitted_unacknowledged = broker_state_category == "WAITABLE_ACK"

        leg_validation = {
            leg_name: {
                "orderId": leg_data["orderId"],
                "parentId": leg_data["parentId"],
                "expected_parent_id": leg_data["expected_parent_id"],
                "status": leg_data["status"],
                "permId": leg_data["permId"],
                "visible_in_open_trades": leg_data["visible_in_open_trades"],
                "visible_in_open_orders": leg_data["visible_in_open_orders"],
                "submitted_to_ib": leg_data["submitted_to_ib"],
                "broker_acknowledged": leg_data["broker_acknowledged"],
                "broker_live": leg_data["broker_live"],
                "link_ok": leg_data["link_ok"],
            }
            for leg_name, leg_data in broker_orders.items()
        }

        logger.info(
            f"BRACKET VALIDATION SUMMARY | {json.dumps({'outcome': outcome, 'reason': reason, 'broker_state_category': broker_state_category, 'any_visible': any_visible, 'all_visible': all_visible, 'all_links_ok': all_links_ok, 'all_broker_acknowledged': all_broker_acknowledged, 'all_broker_live': all_broker_live, 'any_terminal_status': any_terminal_status, 'pending_broker_ack': pending_broker_ack, 'ambiguous_broker_state': ambiguous_broker_state, 'broken_or_terminal_state': broken_or_terminal_state, 'visible_order_ids': sorted(leg_data['orderId'] for leg_data in broker_orders.values() if leg_data['submitted_to_ib']), 'leg_validation': leg_validation}, sort_keys=True)}"
        )

        return {
            "outcome": outcome,
            "broker_state_category": broker_state_category,
            "confirmed": confirmed,
            "pending_broker_ack": pending_broker_ack,
            "submitted_unacknowledged": submitted_unacknowledged,
            "reason": reason,
            "visible_count": len([
                leg_data for leg_data in broker_orders.values() if leg_data["submitted_to_ib"]
            ]),
            "any_visible": any_visible,
            "visible_order_ids": sorted(
                leg_data["orderId"]
                for leg_data in broker_orders.values()
                if leg_data["submitted_to_ib"]
            ),
            "parent_visible": parent_visible,
            "tp_visible": tp_visible,
            "sl_visible": sl_visible,
            "parent_link_ok": parent_link_ok,
            "tp_link_ok": tp_link_ok,
            "sl_link_ok": sl_link_ok,
            "all_perm_ids_assigned": all_perm_ids_assigned,
            "all_statuses_pending_ack": all_statuses_pending_ack,
            "all_statuses_visible_ack": all_statuses_visible_ack,
            "all_live_ack_statuses": all_live_ack_statuses,
            "all_broker_acknowledged": all_broker_acknowledged,
            "all_broker_live": all_broker_live,
            "any_terminal_status": any_terminal_status,
            "ambiguous_broker_state": ambiguous_broker_state,
            "broken_or_terminal_state": broken_or_terminal_state,
            "visible_statuses": visible_statuses,
            "broker_orders": leg_validation,
        }

    # ==========================================================
    # CONTRACTS
    # ==========================================================

    def build_base_contract(self, symbol):
        spec = self.get_instrument_spec(symbol)
        if spec["broker_type"] == "future":
            contract_kwargs = {
                "symbol": spec["symbol"],
                "exchange": spec["exchange"],
                "currency": spec["currency"],
            }
            if spec["trading_class"]:
                contract_kwargs["tradingClass"] = spec["trading_class"]
            return Future(**contract_kwargs)
        if spec["broker_type"] == "forex":
            return Forex(spec["symbol"])
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

        spec = self.get_instrument_spec(symbol)
        if spec["broker_type"] == "forex":
            return self.build_base_contract(symbol)

        raise ValueError(f"{symbol} not cached")

    # ==========================================================
    # SIGNAL HANDLING
    # ==========================================================

    def build_det_execution_job(self, normalized, formal_contract, gate_result):
        """Build execution job with DET planning details.
        P050: Includes new enriched fields for structure-aware stop derivation.
        P049: Moved stop/target derivation to place_bracket_order() after final executable entry.
        This job now carries planning info only; final R is computed at execution time.
        """
        execution_grade = gate_result["execution_grade"]
        reference_price = self.get_preferred_reference_price(normalized)
        symbol = normalized["symbol"]
        side = normalized["side"]
        planned_executable_entry = self.derive_entry_price_from_candidate(normalized)

        return {
            "symbol": symbol,
            "side": side,
            "bot_stage": BOT_STAGE,
            "entry": reference_price,
            "signal_time": normalized["timestamp_utc"] or normalized["time"],
            "enqueue_time": self.utc_now_iso(),
            "grade": execution_grade,
            "strategy_family": formal_contract["strategy_family"],
            "classification": formal_contract["classification"],
            "risk_profile": formal_contract["risk_profile"],
            "execution_intent_reason": formal_contract["execution_intent_reason"],
            "execution_intent_status": formal_contract["execution_intent_status"],
            "generic_execution_permission": formal_contract["generic_execution_permission"],
            "intended_risk_percent": formal_contract["risk_percent"],
            "allowed_money_risk": formal_contract["allowed_money_risk"],
            "truth_classification": formal_contract["truth_classification"],
            "det_classification": formal_contract["det_classification"],
            "primary_reason": formal_contract["primary_reason"],
            "execution_lane": gate_result["execution_lane"],
            "promoted_from_shadow": gate_result["promoted_from_shadow"],
            "shadow_override_reason": gate_result["shadow_override_reason"],
            "reference_price": reference_price,
            "planned_executable_entry": planned_executable_entry,
            "normalized_signal": normalized,
            "raw_payload": normalized["raw_payload"],
            "payload_format": normalized["payload_format"],
            "schema_version": normalized["schema_version"],
            "signal_id": normalized["signal_id"],
        }

    def derive_entry_price_from_candidate(self, normalized):
        if normalized["payload_format"] == "enriched_candidate":
            entry_plan = self.derive_executable_entry_plan_from_normalized(normalized)
            if entry_plan is None:
                return None
            return entry_plan["final_entry"]

        return normalized["entry_price"] if normalized["entry_price"] is not None else normalized["price"]

    def handle_webhook_signal(self, data):
        webhook_received_time = datetime.now(timezone.utc)
        logger.info(f"WEBHOOK RECEIVED: {data}")

        if not isinstance(data, dict):
            logger.error(f"INVALID PAYLOAD TYPE: {type(data)}")
            return "invalid_payload_type"

        normalized = self.normalize_signal(data)
        self.log_normalized_signal(normalized)

        if normalized["payload_format"] == "diagnostic":
            self.log_diagnostic_signal(data)
            evaluator_result = self.evaluate_det_candidate(normalized)
            formal_contract = evaluator_result["formal_contract"]
            det_detail = evaluator_result["det_detail"]
            classification_completed_time = datetime.now(timezone.utc)
            self.increment_classification_stats(normalized["payload_format"], formal_contract)
            self.log_det_result(normalized, formal_contract, det_detail)
            logger.info(
                "QUEUE DECISION | "
                f"path=classification_only "
                    f"payload_format={normalized['payload_format']} "
                    f"queued=false "
                    f"det_classification={formal_contract['det_classification']} "
                    f"execution_permission={formal_contract['execution_permission']}"
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
            if not self.should_route_to_det_evaluator(normalized):
                routing_block_status = self.get_det_routing_block_status(normalized)
                logger.info(
                    "ROUTING BLOCK | "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"pine_family_hint_present={normalized['pine_family_hint_present']} "
                    f"pine_strategy_family_hint_field={normalized['pine_strategy_family_hint_field']} "
                    f"pine_strategy_family_hint={normalized['pine_strategy_family_hint']} "
                    f"pine_strategy_family_normalized={normalized['pine_strategy_family_normalized']} "
                    f"bot_side_family_candidate={normalized['bot_side_family_candidate']} "
                    f"bot_side_family_confidence={normalized['bot_side_family_confidence']} "
                    f"bot_side_family_reason={normalized['bot_side_family_reason']} "
                    f"bot_side_family_resolution_source={normalized['bot_side_family_resolution_source']} "
                    f"bot_side_family_hint_conflict={normalized['bot_side_family_hint_conflict']} "
                    f"strategy_family={normalized['strategy_family']} "
                    f"det_family_context_basis_ok={normalized['det_family_context_basis_ok']} "
                    f"det_family_trigger_structure_basis_ok={normalized['det_family_trigger_structure_basis_ok']} "
                    f"routing_status={normalized['routing_status']} "
                    f"routing_reason_code={normalized['routing_reason_code']} "
                    f"family_consistency_ok={normalized['family_consistency_ok']} "
                    "routing_authority=bot_side_family_resolution "
                    "blocked_by=routing_layer"
                )
                return routing_block_status

            logger.info(
                "ROUTING DECISION | "
                f"payload_format={normalized['payload_format']} "
                f"signal_id={normalized['signal_id']} "
                f"symbol={normalized['symbol']} "
                f"pine_family_hint_present={normalized['pine_family_hint_present']} "
                f"pine_strategy_family_hint_field={normalized['pine_strategy_family_hint_field']} "
                f"pine_strategy_family_hint={normalized['pine_strategy_family_hint']} "
                f"pine_strategy_family_normalized={normalized['pine_strategy_family_normalized']} "
                f"bot_side_family_candidate={normalized['bot_side_family_candidate']} "
                f"bot_side_family_confidence={normalized['bot_side_family_confidence']} "
                f"bot_side_family_reason={normalized['bot_side_family_reason']} "
                f"bot_side_family_resolution_source={normalized['bot_side_family_resolution_source']} "
                f"bot_side_family_hint_conflict={normalized['bot_side_family_hint_conflict']} "
                f"strategy_family={normalized['strategy_family']} "
                f"det_family_context_basis_ok={normalized['det_family_context_basis_ok']} "
                f"det_family_trigger_structure_basis_ok={normalized['det_family_trigger_structure_basis_ok']} "
                f"routing_status={normalized['routing_status']} "
                f"routing_reason_code={normalized['routing_reason_code']} "
                "routing_authority=bot_side_family_resolution "
                "decision=route_to_det_evaluator"
            )

            evaluator_result = self.evaluate_det_candidate(normalized)
            formal_contract = evaluator_result["formal_contract"]
            det_detail = evaluator_result["det_detail"]
            classification_completed_time = datetime.now(timezone.utc)
            self.increment_classification_stats(normalized["payload_format"], formal_contract)
            logger.info(
                "GENERIC CLASSIFICATION LAYER | "
                f"strategy_family={formal_contract['strategy_family']} "
                f"classification={formal_contract['classification']} "
                f"risk_profile={formal_contract['risk_profile']} "
                f"risk_percent={formal_contract['risk_percent']} "
                f"allowed_money_risk={formal_contract['allowed_money_risk']} "
                f"truth_classification={formal_contract['truth_classification']} "
                f"det_classification={formal_contract['det_classification']}"
            )
            self.log_det_result(normalized, formal_contract, det_detail)

            gate_result = self.evaluate_det_execution_gate(formal_contract, det_detail)
            execution_intent_result = self.resolve_execution_intent_from_evaluator_result(
                formal_contract,
                gate_result,
            )
            formal_contract.update(execution_intent_result)
            logger.info(
                "DET EXECUTION GATE DECISION | "
                f"gate_branch={gate_result['gate_branch']} "
                f"truth_classification={formal_contract['truth_classification']} "
                f"det_classification={gate_result['det_classification']} "
                f"gate_enabled={gate_result['gate_enabled']} "
                f"gate_eligible={gate_result['gate_eligible']} "
                f"execution_lane={gate_result['execution_lane']} "
                f"promoted_from_shadow={gate_result['promoted_from_shadow']} "
                f"execution_grade={gate_result['execution_grade']} "
                f"queue_allowed={gate_result['queue_allowed']} "
                f"shadow_override_reason={gate_result['shadow_override_reason']} "
                f"reason={gate_result['reason']}"
            )
            logger.info(
                "GENERIC EXECUTION INTENT | "
                f"strategy_family={formal_contract['strategy_family']} "
                f"classification={formal_contract['classification']} "
                f"risk_profile={formal_contract['risk_profile']} "
                f"execution_intent_status={formal_contract['execution_intent_status']} "
                f"execution_intent_reason={formal_contract['execution_intent_reason']} "
                f"execution_candidate={formal_contract['execution_candidate']} "
                f"generic_execution_permission={formal_contract['generic_execution_permission']} "
                f"det_classification={formal_contract['det_classification']}"
            )

            if not policy["allow_queue"]:
                logger.info(
                    "QUEUE DECISION | "
                    f"path={policy['policy_branch']} "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"truth_classification={formal_contract['truth_classification']} "
                    f"det_classification={formal_contract['det_classification']} "
                    f"execution_lane={gate_result['execution_lane']} "
                    f"promoted_from_shadow={gate_result['promoted_from_shadow']} "
                    f"execution_intent_status={formal_contract['execution_intent_status']} "
                    f"generic_execution_permission={formal_contract['generic_execution_permission']} "
                    f"queued=false "
                    "blocked_by=stage_execution_policy "
                    f"reason={policy['reason']}"
                )
                return "enriched_candidate_classified_no_execution"

            if not gate_result["queue_allowed"]:
                logger.info(
                    "QUEUE DECISION | "
                    f"path={gate_result['gate_branch']} "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"truth_classification={formal_contract['truth_classification']} "
                    f"det_classification={formal_contract['det_classification']} "
                    f"execution_lane={gate_result['execution_lane']} "
                    f"promoted_from_shadow={gate_result['promoted_from_shadow']} "
                    f"execution_intent_status={formal_contract['execution_intent_status']} "
                    f"generic_execution_permission={formal_contract['generic_execution_permission']} "
                    f"queued=false "
                    "blocked_by=det_execution_gate "
                    f"reason={gate_result['reason']}"
                )
                return "enriched_candidate_classified_no_execution"

            entry_for_job = self.derive_entry_price_from_candidate(normalized)
            if entry_for_job is None:
                logger.info(
                    "QUEUE DECISION | "
                    f"path=det_gated_execution "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"det_classification={formal_contract['det_classification']} "
                    f"execution_intent_status={formal_contract['execution_intent_status']} "
                    f"generic_execution_permission={formal_contract['generic_execution_permission']} "
                    f"queued=false "
                    "blocked_by=missing_price_reference "
                    f"reason=missing_price_reference"
                )
                return "enriched_candidate_missing_price"

            symbol = normalized["symbol"]
            side = normalized["side"]
            entry = entry_for_job
            execution_grade = gate_result["execution_grade"]
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
                    f"queued=false "
                    "blocked_by=candidate_entry_derivation"
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
                    f"queued=false "
                    "blocked_by=entry_band_validation"
                )
                return "enriched_candidate_entry_band_rejected"

            now = time.time()
            key = f"{symbol}-{side}-{round(entry, 8)}"

            if key == self.last_signal and now - self.last_signal_time < 5:
                logger.info("Duplicate ignored")
                return "duplicate_ignored"

            self.last_signal = key
            self.last_signal_time = now

            job = self.build_det_execution_job(normalized, formal_contract, gate_result)
            job["webhook_received_time"] = webhook_received_time
            job["classification_completed_time"] = classification_completed_time

            logger.info(
                "QUEUE PUT | "
                f"symbol={job['symbol']} "
                f"side={job['side']} "
                f"reference_price={job['reference_price']} "
                f"planned_executable_entry={job.get('planned_executable_entry')} "
                f"grade={job['grade']} "
                f"intended_risk_pct={job['intended_risk_percent']*100:.1f}% "
                f"truth_classification={job['truth_classification']} "
                f"det_classification={job['det_classification']} "
                f"execution_lane={job['execution_lane']} "
                f"promoted_from_shadow={job['promoted_from_shadow']} "
                f"execution_intent_status={job['execution_intent_status']} "
                f"generic_execution_permission={job['generic_execution_permission']} "
                f"shadow_override_reason={job['shadow_override_reason']} "
                f"payload_format={job['payload_format']} "
                f"signal_id={job['signal_id']}"
            )

            queue_put_time = datetime.now(timezone.utc)
            job["queue_put_time"] = queue_put_time
            job["enqueue_time"] = queue_put_time
            self.execution_queue.put(job)
            self.aggregate_stats["queued_count"] += 1
            if job.get("promoted_from_shadow"):
                self.aggregate_stats["shadow_test_queued_count"] += 1
            logger.info(
                "QUEUE DECISION | "
                    f"path=det_gated_execution "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"truth_classification={formal_contract['truth_classification']} "
                    f"det_classification={formal_contract['det_classification']} "
                    f"execution_lane={job['execution_lane']} "
                    f"promoted_from_shadow={job['promoted_from_shadow']} "
                    f"execution_intent_status={formal_contract['execution_intent_status']} "
                    f"generic_execution_permission={formal_contract['generic_execution_permission']} "
                    f"shadow_override_reason={job['shadow_override_reason']} "
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
            job = None
            try:
                with self.execution_lock:
                    self.process_partial_entry_timeouts()
                    self.process_partial_timeout_parent_finality()

                job = self.execution_queue.get(timeout=1.0)
                job["worker_pickup_time"] = datetime.now(timezone.utc)
                logger.info(
                    "EXECUTION WORKER QUEUE ITEM ACQUIRED | "
                    f"symbol={job.get('symbol')} "
                    f"side={job.get('side')} "
                    f"grade={job.get('grade')} "
                    f"queue_size_after_get={self.execution_queue.qsize()}"
                )

                with self.execution_lock:
                    has_conflict, conflicting_trade, concurrency_policy = self.has_conflicting_active_trade_for_job(job)
                    if has_conflict:
                        logger.warning(
                            "CONCURRENCY BLOCKED EXECUTION | "
                            f"stage={concurrency_policy['stage']} "
                            f"mode={concurrency_policy['mode']} "
                            f"conflicting_trade_id={conflicting_trade['trade_id']} "
                            f"conflicting_symbol={conflicting_trade['symbol']} "
                            f"conflicting_state={conflicting_trade['state']} "
                            f"incoming_symbol={job['symbol']}"
                        )
                        logger.warning(
                            "EXECUTION WORKER EXECUTION SKIPPED | "
                            f"symbol={job.get('symbol')} "
                            "reason=concurrency_block"
                        )
                        continue

                    risk_regime = self.evaluate_live_execution_risk_regime(job)
                    if not risk_regime["execution_allowed"]:
                        logger.warning(
                            "EXECUTION RISK REGIME BLOCKED | "
                            f"stage={risk_regime['stage']} "
                            f"risk_branch={risk_regime['risk_branch']} "
                            f"incoming_symbol={job['symbol']} "
                            f"day_key={risk_regime.get('day_key')} "
                            f"trigger_trade_id={risk_regime.get('trigger_trade_id')} "
                            f"reason={risk_regime['reason']}"
                        )
                        logger.warning(
                            "EXECUTION WORKER EXECUTION SKIPPED | "
                            f"symbol={job.get('symbol')} "
                            "reason=risk_regime_block"
                        )
                        continue

                    logger.info("PRE-EXECUTION HEALTH CHECK")
                    if not self.execution_preflight_check(job):
                        logger.error("EXECUTION PREFLIGHT FAILED | SKIPPING JOB")
                        logger.error(
                            "EXECUTION WORKER EXECUTION SKIPPED | "
                            f"symbol={job.get('symbol')} "
                            "reason=preflight_failed"
                        )
                        continue
                    job["preflight_completed_time"] = datetime.now(timezone.utc)

                    logger.info(f"IB CONNECTED: {self.ib.isConnected()}")
                    logger.info("STARTING ORDER EXECUTION")

                    self.place_bracket_order(job)

                    logger.info(
                        "EXECUTION WORKER EXECUTION COMPLETED | "
                        f"symbol={job.get('symbol')} "
                        f"side={job.get('side')}"
                    )

            except queue.Empty:
                logger.info("EXECUTION WORKER QUEUE POLL | acquired_job=false")
                continue
            except Exception:
                logger.exception(
                    "EXECUTION WORKER EXECUTION FAILED | "
                    f"symbol={job.get('symbol') if job else None} "
                    f"side={job.get('side') if job else None}"
                )
            finally:
                if job is not None:
                    self.execution_queue.task_done()
                    logger.info(
                        "EXECUTION WORKER TASK DONE | "
                        f"symbol={job.get('symbol')} "
                        f"side={job.get('side')} "
                        "current_iteration_job_only=true"
                    )

    # ==========================================================
    # EXECUTION QUALITY LOGGING
    # ==========================================================

    def place_bracket_order(self, job):
        symbol = job["symbol"]
        side = job["side"]
        size_constraints = self.get_size_constraints(symbol)
        configured_max_size = size_constraints["max_size"]
        configured_min_stop_distance = self.get_min_stop_distance(symbol)

        entry_plan = self.derive_executable_entry_plan_from_job(job)
        if entry_plan is None:
            logger.error(
                "EXECUTION ENTRY DERIVATION FAILED | "
                f"symbol={symbol} side={side} reference_price={job.get('reference_price')}"
            )
            return

        spread_adjusted_entry = entry_plan["spread_adjusted_entry"]
        entry = entry_plan["final_entry"]

        contract = self.get_contract(symbol)

        # P050: Derive stop from FINAL executable entry (after spread/rounding)
        # Optionally uses structure anchors from normalized_signal for enriched stop derivation
        execution_grade = job.get("grade", "A")
        normalized_signal = job.get("normalized_signal")
        stop_plan = self.derive_det_stop_plan(symbol, entry, side, execution_grade, normalized_signal)
        stop = stop_plan["stop_price"]
        stop = self.round_to_tick(symbol, stop)
        stop_validation = self.validate_final_stop_distance(symbol, entry, stop)
        stop_floor_affected_execution_eligibility = not stop_validation["ok"]

        if symbol == "FDXM":
            containment_reason = None
            if configured_min_stop_distance is None or configured_min_stop_distance <= 0:
                containment_reason = "missing_or_invalid_configured_min_stop_distance"
            elif configured_max_size is None or configured_max_size <= 0:
                containment_reason = "missing_or_invalid_configured_max_size"
            elif stop_validation["final_stop_distance"] is None:
                containment_reason = "missing_final_stop_distance"

            logger.info(
                "FDXM_CONTAINMENT | "
                f"symbol={symbol} "
                f"reference_price={entry_plan['reference_price']} "
                f"final_entry={entry} "
                f"final_stop={stop} "
                f"final_stop_distance={stop_validation['final_stop_distance']} "
                f"configured_min_stop_distance={configured_min_stop_distance} "
                "raw_size=None "
                "normalized_size=None "
                f"configured_max_size={configured_max_size} "
                "size_capping_happened=None "
                f"stop_floor_affected_execution_eligibility={stop_floor_affected_execution_eligibility} "
                f"containment_reason={containment_reason or 'stop_containment_ready'} "
                f"validation_reason={stop_validation['reason']}"
            )

            if containment_reason is not None:
                logger.error(
                    "FDXM_CONTAINMENT_FAIL_CLOSED | "
                    f"symbol={symbol} "
                    f"reference_price={entry_plan['reference_price']} "
                    f"final_entry={entry} "
                    f"final_stop={stop} "
                    f"final_stop_distance={stop_validation['final_stop_distance']} "
                    f"configured_min_stop_distance={configured_min_stop_distance} "
                    "raw_size=None "
                    "normalized_size=None "
                    f"configured_max_size={configured_max_size} "
                    "size_capping_happened=None "
                    f"stop_floor_affected_execution_eligibility={stop_floor_affected_execution_eligibility} "
                    f"reason={containment_reason}"
                )
                return

        logger.info(
            "STOP PLAN | "
            f"symbol={symbol} "
            f"side={side} "
            f"reference_price={entry_plan['reference_price']} "
            f"spread_adjusted_entry={spread_adjusted_entry} "
            f"final_entry={entry} "
            f"final_stop={stop} "
            f"final_stop_distance={stop_validation['final_stop_distance']} "
            f"min_stop_distance={stop_validation['min_stop_distance']} "
            f"stop_source={stop_plan['stop_source']} "
            f"selected_stop_source={stop_plan['selected_stop_source']} "
            f"anchor_based={stop_plan['anchor_based']} "
            f"validation_reason={stop_validation['reason']}"
        )
        if not stop_validation["ok"]:
            logger.error(
                "STOP DISTANCE BLOCKED EXECUTION | "
                f"symbol={symbol} "
                f"side={side} "
                f"reference_price={entry_plan['reference_price']} "
                f"spread_adjusted_entry={spread_adjusted_entry} "
                f"final_entry={entry} "
                f"final_stop={stop} "
                f"final_stop_distance={stop_validation['final_stop_distance']} "
                f"min_stop_distance={stop_validation['min_stop_distance']} "
                f"stop_source={stop_plan['stop_source']} "
                f"selected_stop_source={stop_plan['selected_stop_source']} "
                f"anchor_based={stop_plan['anchor_based']} "
                f"reason={stop_validation['reason']}"
            )
            return

        # P049: Derive 2R target from final entry and stop
        target = self.derive_target_from_r(entry, stop, side)
        target = self.round_to_tick(symbol, target)

        # P049: Compute final 1R distance from final executable entry and stop
        risk_distance_r = abs(entry - stop)

        sizing_result = self.calculate_execution_position_size(
            symbol,
            job.get("grade"),
            entry,
            stop
        )
        raw_size = sizing_result["raw_position_size"]
        normalized_size = sizing_result["normalized_position_size"]
        size_capping_happened = bool(
            configured_max_size is not None and
            raw_size is not None and
            raw_size > configured_max_size
        )

        if symbol == "FDXM":
            containment_reason = None
            if raw_size is None:
                containment_reason = "missing_raw_size"
            elif normalized_size is None:
                containment_reason = "missing_normalized_size"

            logger.info(
                "FDXM_CONTAINMENT | "
                f"symbol={symbol} "
                f"reference_price={entry_plan['reference_price']} "
                f"final_entry={entry} "
                f"final_stop={stop} "
                f"final_stop_distance={stop_validation['final_stop_distance']} "
                f"configured_min_stop_distance={configured_min_stop_distance} "
                f"raw_size={raw_size} "
                f"normalized_size={normalized_size} "
                f"configured_max_size={configured_max_size} "
                f"size_capping_happened={size_capping_happened} "
                f"stop_floor_affected_execution_eligibility={stop_floor_affected_execution_eligibility} "
                f"containment_reason={containment_reason or 'sizing_containment_ready'} "
                f"sizing_ok={sizing_result['ok']} "
                f"sizing_reason={sizing_result['reason']}"
            )

            if containment_reason is not None:
                logger.error(
                    "FDXM_CONTAINMENT_FAIL_CLOSED | "
                    f"symbol={symbol} "
                    f"reference_price={entry_plan['reference_price']} "
                    f"final_entry={entry} "
                    f"final_stop={stop} "
                    f"final_stop_distance={stop_validation['final_stop_distance']} "
                    f"configured_min_stop_distance={configured_min_stop_distance} "
                    f"raw_size={raw_size} "
                    f"normalized_size={normalized_size} "
                    f"configured_max_size={configured_max_size} "
                    f"size_capping_happened={size_capping_happened} "
                    f"stop_floor_affected_execution_eligibility={stop_floor_affected_execution_eligibility} "
                    f"reason={containment_reason}"
                )
                return

        logger.info(
            "SIZING PLAN | "
            f"symbol={symbol} "
            f"reference_price={entry_plan['reference_price']} "
            f"spread_adjusted_entry={spread_adjusted_entry} "
            f"final_entry={entry} "
            f"final_stop={stop} "
            f"final_stop_distance={stop_validation['final_stop_distance']} "
            f"min_stop_distance={stop_validation['min_stop_distance']} "
            f"stop_source={stop_plan['stop_source']} "
            f"selected_stop_source={stop_plan['selected_stop_source']} "
            f"capital_base={sizing_result['capital_base']} "
            f"execution_grade={job.get('grade', '')} "
            f"intended_risk_percent={sizing_result['intended_risk_percent']} "
            f"allowed_money_risk={sizing_result['allowed_money_risk']} "
            f"stop_distance_points={sizing_result['stop_distance_points']} "
            f"point_value={sizing_result['point_value']} "
            f"risk_per_contract={sizing_result['risk_per_contract']} "
            f"raw_size={sizing_result['raw_position_size']} "
            f"normalized_size={sizing_result['normalized_position_size']} "
            f"validation_result={sizing_result['validation_reason']}"
        )
        if not sizing_result["ok"]:
            logger.error(
                "SIZING BLOCKED EXECUTION | "
                f"symbol={symbol} "
                f"side={side} "
                f"reference_price={entry_plan['reference_price']} "
                f"spread_adjusted_entry={spread_adjusted_entry} "
                f"final_entry={entry} "
                f"final_stop={stop} "
                f"final_stop_distance={stop_validation['final_stop_distance']} "
                f"min_stop_distance={stop_validation['min_stop_distance']} "
                f"stop_source={stop_plan['stop_source']} "
                f"selected_stop_source={stop_plan['selected_stop_source']} "
                f"capital_base={sizing_result['capital_base']} "
                f"execution_grade={job.get('grade', '')} "
                f"intended_risk_percent={sizing_result['intended_risk_percent']} "
                f"allowed_money_risk={sizing_result['allowed_money_risk']} "
                f"stop_distance_points={sizing_result['stop_distance_points']} "
                f"point_value={sizing_result['point_value']} "
                f"raw_size={sizing_result['raw_position_size']} "
                f"normalized_size={sizing_result['normalized_position_size']} "
                f"reason={sizing_result['reason']}"
            )
            return

        job["allowed_money_risk"] = sizing_result["allowed_money_risk"]
        job["stop_distance_points"] = sizing_result["stop_distance_points"]
        job["raw_position_size"] = sizing_result["raw_position_size"]
        job["normalized_position_size"] = normalized_size

        if (
            configured_max_size is not None and
            sizing_result["raw_position_size"] is not None and
            sizing_result["raw_position_size"] > configured_max_size
        ):
            logger.warning(
                "POSITION SIZE CAPPED | "
                f"symbol={symbol} "
                f"side={side} "
                f"raw_size={sizing_result['raw_position_size']} "
                f"normalized_size={normalized_size} "
                f"max_size={configured_max_size} "
                "reason=max_size_applied"
            )

        if side == "long":
            parent_action = "BUY"
            child_action = "SELL"
        else:
            parent_action = "SELL"
            child_action = "BUY"

        logger.info(
            f"OFFICIAL BRACKET | {symbol} {side} "
            f"grade={job.get('grade', '')} "
            f"truth_classification={job.get('truth_classification', '')} "
            f"det_classification={job.get('det_classification', '')} "
            f"execution_lane={job.get('execution_lane', 'standard')} "
            f"promoted_from_shadow={job.get('promoted_from_shadow', False)} "
            f"shadow_override_reason={job.get('shadow_override_reason', '')} "
            f"reference_price={job.get('reference_price', 'N/A')} "
            f"spread_adjusted_entry={spread_adjusted_entry} "
            f"trigger_bar_high={job.get('normalized_signal', {}).get('trigger_bar_high', 'N/A')} "
            f"trigger_bar_low={job.get('normalized_signal', {}).get('trigger_bar_low', 'N/A')} "
            f"structure_anchor_low={job.get('normalized_signal', {}).get('structure_anchor_low', 'N/A')} "
            f"structure_anchor_high={job.get('normalized_signal', {}).get('structure_anchor_high', 'N/A')} "
            f"final_entry={entry} "
            f"final_stop={stop} "
            f"final_stop_distance={stop_validation['final_stop_distance']} "
            f"min_stop_distance={stop_validation['min_stop_distance']} "
            f"stop_source={stop_plan['stop_source']} "
            f"selected_stop_source={stop_plan['selected_stop_source']} "
            f"final_target={target} "
            f"planned_position_size={normalized_size} "
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

        parent = LimitOrder(parent_action, normalized_size, entry)
        parent.orderId = parent_id
        parent.transmit = False
        parent.tif = "GTC"

        tp = LimitOrder(child_action, normalized_size, target)
        tp.orderId = tp_id
        tp.parentId = parent_id
        tp.transmit = False
        tp.tif = "GTC"

        sl = StopOrder(child_action, normalized_size, stop)
        sl.orderId = sl_id
        sl.parentId = parent_id
        sl.transmit = True
        sl.tif = "GTC"

        logger.info(
            f"ORDER DEF PARENT → orderId={parent.orderId} parentId={parent.parentId} "
            f"action={parent.action} orderType={parent.orderType} "
            f"totalQuantity={getattr(parent, 'totalQuantity', None)} "
            f"lmtPrice={getattr(parent, 'lmtPrice', None)} auxPrice={getattr(parent, 'auxPrice', None)} "
            f"transmit={parent.transmit}"
        )
        logger.info(
            f"ORDER DEF TP → orderId={tp.orderId} parentId={tp.parentId} "
            f"action={tp.action} orderType={tp.orderType} "
            f"totalQuantity={getattr(tp, 'totalQuantity', None)} "
            f"lmtPrice={getattr(tp, 'lmtPrice', None)} auxPrice={getattr(tp, 'auxPrice', None)} "
            f"transmit={tp.transmit}"
        )
        logger.info(
            f"ORDER DEF SL → orderId={sl.orderId} parentId={sl.parentId} "
            f"action={sl.action} orderType={sl.orderType} "
            f"totalQuantity={getattr(sl, 'totalQuantity', None)} "
            f"lmtPrice={getattr(sl, 'lmtPrice', None)} auxPrice={getattr(sl, 'auxPrice', None)} "
            f"transmit={sl.transmit}"
        )

        self.set_trade_timestamp(trade_id, "bracket_submit_start_time")
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
        pending_broker_ack = False
        broker_confirmation = None
        parent_status = None
        tp_status = None
        sl_status = None
        parent_perm = 0
        tp_perm = 0
        sl_perm = 0

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

            broker_confirmation = self.assess_broker_bracket_confirmation(parent_id, tp_id, sl_id)
            if broker_confirmation.get("all_broker_live"):
                confirmed = True
                pending_broker_ack = False
                break
            if broker_confirmation["confirmed"]:
                confirmed = True
                pending_broker_ack = False
                break
            if broker_confirmation.get("pending_broker_ack"):
                pending_broker_ack = True

            self.ib.sleep(0.10)

        self.log_trade_snapshot("POST CONFIRM PARENT", parent_trade)
        self.log_trade_snapshot("POST CONFIRM TP", tp_trade)
        self.log_trade_snapshot("POST CONFIRM SL", sl_trade)

        if broker_confirmation is None:
            broker_confirmation = self.assess_broker_bracket_confirmation(parent_id, tp_id, sl_id)
        broker_state_category = broker_confirmation.get("broker_state_category", "BROKEN_OR_TERMINAL")
        confirmed = bool(broker_confirmation.get("confirmed"))
        pending_broker_ack = bool(broker_confirmation.get("pending_broker_ack")) and not confirmed

        submitted_count_eligible = broker_state_category in {
            "ACKNOWLEDGED",
            "WAITABLE_ACK",
            "AMBIGUOUS_ACK",
        }
        if submitted_count_eligible:
            self.increment_trade_counter_once(
                trade_id,
                "submitted_counted",
                "submitted_count",
            )

        logger.info(
            f"BRACKET CONFIRMATION ASSESSMENT | outcome={broker_confirmation.get('outcome')} | confirmed={confirmed} | "
            f"pending_broker_ack={pending_broker_ack} | "
            f"broker_state_category={broker_state_category} | "
            f"parent: orderId={parent_id} status={parent_status} permId={parent_perm} | "
            f"tp: orderId={tp_id} status={tp_status} permId={tp_perm} | "
            f"sl: orderId={sl_id} status={sl_status} permId={sl_perm} | "
            f"broker_visible_order_ids={broker_confirmation['visible_order_ids']} | "
            f"broker_visible_statuses={broker_confirmation['visible_statuses']} | "
            f"parent_visible={broker_confirmation['parent_visible']} "
            f"tp_visible={broker_confirmation['tp_visible']} "
            f"sl_visible={broker_confirmation['sl_visible']} | "
            f"parent_link_ok={broker_confirmation['parent_link_ok']} "
            f"tp_link_ok={broker_confirmation['tp_link_ok']} "
            f"sl_link_ok={broker_confirmation['sl_link_ok']} | "
            f"all_perm_ids_assigned={broker_confirmation['all_perm_ids_assigned']} | "
            f"all_broker_acknowledged={broker_confirmation.get('all_broker_acknowledged')} | "
            f"all_broker_live={broker_confirmation.get('all_broker_live')} | "
            f"all_statuses_pending_ack={broker_confirmation['all_statuses_pending_ack']} | "
            f"all_live_ack_statuses={broker_confirmation['all_live_ack_statuses']} | "
            f"submitted_unacknowledged={broker_confirmation.get('submitted_unacknowledged')} | "
            f"ambiguous_broker_state={broker_confirmation.get('ambiguous_broker_state')} | "
            f"broken_or_terminal_state={broker_confirmation.get('broken_or_terminal_state')} | "
            f"submitted_count_eligible={submitted_count_eligible} | "
            f"reason={broker_confirmation['reason']}"
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

        if broker_state_category in {"WAITABLE_ACK", "AMBIGUOUS_ACK"}:
            self.set_execution_validation_status(
                trade_id,
                "submitted_to_ib",
                f"{broker_confirmation.get('outcome')}:{broker_confirmation['reason']}"
            )
            self.append_trade_event(
                trade_id,
                f"{broker_confirmation.get('outcome')} broker_state_category={broker_state_category} "
                f"reason={broker_confirmation['reason']} "
                f"visible_order_ids={broker_confirmation['visible_order_ids']}"
            )
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    previous_state = record["state"]
                    record["state"] = "BROKER_ACK_PENDING"
                    record["broker_ack_pending_since"] = datetime.now(timezone.utc)
                    record["broker_ack_pending_last_check"] = record["broker_ack_pending_since"]
                    record["broker_ack_pending_reason"] = broker_confirmation["reason"]
                    record["broker_ack_pending_category"] = broker_state_category
                    record["broker_ack_pending_visible_order_ids"] = broker_confirmation["visible_order_ids"]
                    self.append_trade_event(
                        trade_id,
                        f"BROKER ACK PENDING ENTER previous_state={previous_state} "
                        f"broker_state_category={broker_state_category} "
                        f"visible_order_ids={broker_confirmation['visible_order_ids']} "
                        f"reason={broker_confirmation['reason']}"
                    )
            logger.warning(
                "BROKER_ACK_PENDING_ENTER | "
                f"{broker_confirmation.get('outcome')} | "
                f"stage={BOT_STAGE} "
                f"broker_state_category={broker_state_category} "
                f"trade_id={trade_id} "
                f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id} "
                f"broker_reason={broker_confirmation['reason']} "
                f"broker_visible_order_ids={broker_confirmation['visible_order_ids']} "
                f"broker_visible_statuses={broker_confirmation['visible_statuses']} "
                f"all_perm_ids_assigned={broker_confirmation['all_perm_ids_assigned']}"
            )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return

        if not confirmed:
            if broker_state_category == "BROKEN_OR_TERMINAL":
                self.append_anomaly(trade_id, "BROKER_BRACKET_CONFIRMATION_FAILED")
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    f"{broker_confirmation.get('outcome')}:{broker_confirmation['reason']}"
                )
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        record["state"] = "INCOMPLETE"
                self.finalize_trade_if_complete(trade_id)
                logger.error(
                    "BRACKET_CONFIRMATION_NON_WAITABLE_FAIL_CLOSED | "
                    f"outcome={broker_confirmation.get('outcome')} "
                    f"trade_id={trade_id} "
                    f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id} "
                    f"broker_state_category={broker_state_category} "
                    "decision=fail_closed_not_broker_ack_pending "
                    "validation_status=validation_incomplete "
                    "state=INCOMPLETE "
                    f"broker_reason={broker_confirmation['reason']} "
                    f"broker_visible_order_ids={broker_confirmation['visible_order_ids']} "
                    f"broker_visible_statuses={broker_confirmation['visible_statuses']}"
                )
                self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
                return

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                broker_reality_record = dict(record) if record is not None else None

            broker_reality = None
            if broker_reality_record is not None:
                broker_reality = self.get_trade_broker_reality(broker_reality_record)

            if broker_reality is not None and broker_reality["broker_real"]:
                now_dt = datetime.now(timezone.utc)
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        previous_state = record["state"]
                        record["state"] = "BROKER_ACK_PENDING"
                        record["broker_ack_pending_since"] = record.get("broker_ack_pending_since") or now_dt
                        record["broker_ack_pending_last_check"] = now_dt
                        record["broker_ack_pending_reason"] = broker_confirmation["reason"]
                        record["broker_ack_pending_category"] = broker_state_category
                        record["broker_ack_pending_visible_order_ids"] = broker_confirmation["visible_order_ids"]
                        self.append_trade_event(
                            trade_id,
                            f"BRACKET BROKER REALITY PRESERVED previous_state={previous_state} "
                            f"broker_state_category={broker_state_category} "
                            f"reason={broker_confirmation['reason']} "
                            f"visible_order_ids={broker_confirmation['visible_order_ids']}"
                        )
                logger.warning(
                    "BRACKET_CONFIRMATION_DEFERRED_BROKER_REALITY | "
                    f"outcome={broker_confirmation.get('outcome')} "
                    f"stage={BOT_STAGE} "
                    f"broker_state_category={broker_state_category} "
                    f"trade_id={trade_id} "
                    f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id} "
                    f"broker_reason={broker_confirmation['reason']} "
                    f"broker_visible_order_ids={broker_confirmation['visible_order_ids']} "
                    f"broker_visible_statuses={broker_confirmation['visible_statuses']} "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    "decision=keep_tracking_broker_ack_pending"
                )
                self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
                return

            self.append_anomaly(trade_id, "BROKER_BRACKET_CONFIRMATION_FAILED")
            self.set_execution_validation_status(
                trade_id,
                "validation_incomplete",
                f"{broker_confirmation.get('outcome')}:{broker_confirmation['reason']}"
            )
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["state"] = "INCOMPLETE"
            self.finalize_trade_if_complete(trade_id)
            logger.error(
                f"BRACKET_CONFIRMATION_FAILED_TRUE | outcome={broker_confirmation.get('outcome')} | trade_id={trade_id} "
                f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id} "
                f"broker_reason={broker_confirmation['reason']} "
                f"broker_visible_order_ids={broker_confirmation['visible_order_ids']} "
                f"broker_visible_statuses={broker_confirmation['visible_statuses']}"
            )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return

        if broker_confirmation.get("all_broker_live"):
            self.set_execution_validation_status(
                trade_id,
                "broker_live",
                f"{broker_confirmation.get('outcome')}:{broker_confirmation['reason']}"
            )
        else:
            self.set_execution_validation_status(
                trade_id,
                "broker_acknowledged",
                f"{broker_confirmation.get('outcome')}:{broker_confirmation['reason']}"
            )
        self.append_trade_event(trade_id, "BRACKET SUBMITTED")
        logger.info(
            f"{broker_confirmation.get('outcome')} | "
            f"trade_id={trade_id} "
            f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id} "
            f"broker_reason={broker_confirmation['reason']} "
            f"broker_visible_order_ids={broker_confirmation['visible_order_ids']} "
            f"all_perm_ids_assigned={broker_confirmation['all_perm_ids_assigned']} "
            f"all_broker_acknowledged={broker_confirmation.get('all_broker_acknowledged')} "
            f"all_broker_live={broker_confirmation.get('all_broker_live')}"
        )
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is not None and record["state"] == "BROKER_ACK_PENDING":
                record["state"] = "ENTRY_WORKING"
                record["broker_ack_pending_last_check"] = datetime.now(timezone.utc)
                record["broker_ack_pending_reason"] = broker_confirmation["reason"]
                self.append_trade_event(
                    trade_id,
                    f"BROKER ACK PENDING EXIT previous_state=BROKER_ACK_PENDING "
                    f"new_state={record['state']} reason={broker_confirmation['reason']}"
                )
        logger.info("BRACKET SUBMITTED")
        self.set_trade_timestamp(trade_id, "bracket_submit_end_time")

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
