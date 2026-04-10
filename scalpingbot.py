# ==========================================================
# IKBR SCALPING BOT | VERSION v1.6.0 P076 | STAGE: TEST
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
BOT_PATCH = "P076"
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
        "fallback_stop_distance": 2.00,
        "entry_band_a": 2.00,
        "entry_band_a_plus": 1.00,
        "min_size": 1,
        "size_step": 1,
        "max_size": None,
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
            "gross_pnl": 0.0,
            "commission": 0.0,
            "net_pnl": 0.0,
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
            "intended_risk_percent": job.get("intended_risk_percent"),
            "allowed_money_risk": job.get("allowed_money_risk"),
            "stop_distance_points": job.get("stop_distance_points"),
            "raw_position_size": job.get("raw_position_size"),
            "normalized_position_size": job.get("normalized_position_size"),
            "intended_parent_quantity": float(job.get("normalized_position_size", 0.0) or 0.0),
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
                if record["entry_filled"]:
                    record["state"] = "ENTRY_FILLED"
                else:
                    record["state"] = "ENTRY_WORKING"
            elif status in ("Cancelled", "ApiCancelled", "Inactive"):
                if not record["entry_filled"]:
                    record["state"] = "CANCELLED"

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
                "has_open_order_match": False,
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
            order_id = getattr(order, "orderId", None)
            perm_id = getattr(status, "permId", None)

            if order_id in order_ids:
                open_trade_order_ids.append(order_id)
            if perm_id in perm_ids:
                open_trade_perm_ids.append(perm_id)

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

        has_open_trade_match = bool(open_trade_order_ids or open_trade_perm_ids)
        has_open_order_match = bool(open_order_ids)
        has_position_match = bool(position_sizes)
        broker_real = has_open_trade_match or has_open_order_match or has_position_match

        return {
            "broker_real": broker_real,
            "has_open_trade_match": has_open_trade_match,
            "has_open_order_match": has_open_order_match,
            "has_position_match": has_position_match,
            "matching_open_trade_order_ids": sorted(set(open_trade_order_ids)),
            "matching_open_trade_perm_ids": sorted(set(open_trade_perm_ids)),
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

        self.finalize_trade_if_complete(trade_id)
        return True

    def get_stage_concurrency_policy(self):
        stage = BOT_STAGE

        if stage == "TEST":
            policy = {
                "stage": stage,
                "mode": "allow_all",
                "description": "TEST allows parallel execution across all active trades",
            }
        elif stage == "PAPER":
            policy = {
                "stage": stage,
                "mode": "per_instrument",
                "description": "PAPER blocks only same-symbol active trade conflicts",
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
                    "parent_order_id": record["parent_order_id"],
                    "tp_order_id": record["tp_order_id"],
                    "sl_order_id": record["sl_order_id"],
                    "parent_perm_id": record["parent_perm_id"],
                    "tp_perm_id": record["tp_perm_id"],
                    "sl_perm_id": record["sl_perm_id"],
                }
                for record in self.trade_analysis.values()
                if not record["summary_logged"] and record["state"] in ACTIVE_TRADE_STATES
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
                f"open_order_match={broker_reality['has_open_order_match']} "
                f"position_match={broker_reality['has_position_match']} "
                f"open_trade_order_ids={broker_reality['matching_open_trade_order_ids']} "
                f"open_trade_perm_ids={broker_reality['matching_open_trade_perm_ids']} "
                f"open_order_ids={broker_reality['matching_open_order_ids']} "
                f"position_sizes={broker_reality['matching_position_sizes']} "
                f"check_failed={broker_reality['check_failed']} "
                f"reason={reason_label}"
            )

            if broker_reality["broker_real"]:
                logger.warning(
                    "ACTIVE LOCK PRESERVED | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    f"reason={reason_label} "
                    "broker-side activity still exists"
                )
                broker_real_candidates.append({
                    "trade_id": record_snapshot["trade_id"],
                    "symbol": record_snapshot["symbol"],
                    "state": record_snapshot["state"],
                    "entry_filled": record_snapshot["entry_filled"],
                    "broker_reality": broker_reality,
                })
                continue

            logger.warning(
                "STALE ACTIVE LOCK DETECTED | "
                f"trade_id={record_snapshot['trade_id']} "
                f"symbol={record_snapshot['symbol']} "
                f"state={record_snapshot['state']} "
                f"reason={reason_label} "
                "no matching broker-side open trade, open order, or live position"
            )
            self.cleanup_stale_active_trade_lock(
                record_snapshot["trade_id"],
                broker_reality,
                reason_label
            )

        logger.info(
            "CONCURRENCY CANDIDATES RESULT | "
            f"reason={reason_label} "
            f"broker_real_count={len(broker_real_candidates)}"
        )
        return broker_real_candidates

    def has_conflicting_active_trade_for_job(self, job):
        policy = self.get_stage_concurrency_policy()
        incoming_symbol = job["symbol"]
        active_candidates = self.get_active_trade_candidates(
            f"CONCURRENCY_CHECK_{policy['stage']}"
        )

        if policy["mode"] == "allow_all":
            logger.info(
                "CONCURRENCY CHECK | "
                f"stage={policy['stage']} "
                f"incoming_symbol={incoming_symbol} "
                "decision=allow "
                "reason=test_allows_parallel_execution"
            )
            return False, None, policy

        if policy["mode"] == "per_instrument":
            for candidate in active_candidates:
                if candidate["symbol"] == incoming_symbol:
                    logger.warning(
                        "CONCURRENCY CHECK | "
                        f"stage={policy['stage']} "
                        f"incoming_symbol={incoming_symbol} "
                        f"conflicting_trade_id={candidate['trade_id']} "
                        f"conflicting_symbol={candidate['symbol']} "
                        f"conflicting_state={candidate['state']} "
                        "decision=block "
                        "reason=same_symbol_active_trade_in_paper"
                    )
                    return True, candidate, policy

                logger.info(
                    "CONCURRENCY CHECK | "
                    f"stage={policy['stage']} "
                    f"incoming_symbol={incoming_symbol} "
                    f"conflicting_trade_id={candidate['trade_id']} "
                    f"conflicting_symbol={candidate['symbol']} "
                    f"conflicting_state={candidate['state']} "
                    "decision=allow "
                    "reason=different_symbol_in_paper"
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
            logger.warning(
                "CONCURRENCY CHECK | "
                f"stage={policy['stage']} "
                f"incoming_symbol={incoming_symbol} "
                f"conflicting_trade_id={candidate['trade_id']} "
                f"conflicting_symbol={candidate['symbol']} "
                f"conflicting_state={candidate['state']} "
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

            if (
                state in ("Cancelled", "ApiCancelled", "Inactive")
                and order_id == record["parent_order_id"]
                and not record["entry_filled"]
            ):
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

    def is_entry_fully_filled(self, record):
        intended_parent_quantity = float(record.get("intended_parent_quantity") or 0.0)
        cumulative_entry_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
        return intended_parent_quantity > 0 and cumulative_entry_quantity >= intended_parent_quantity

    def is_exit_fully_filled(self, record):
        if not record.get("entry_filled"):
            return False
        realized_entry_quantity = float(record.get("realized_entry_quantity") or 0.0)
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
                    if record.get("bot_stage") == "LIVE":
                        self.activate_live_daily_sl_stop(
                            record["trade_id"],
                            record["exit_fill_time"]
                        )
                elif record["exit_reason"] == "MIXED_EXIT":
                    self.aggregate_stats["mixed_exit_count"] += 1
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
                f"planned_position_size={record['planned_position_size']} "
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
                f"time_in_trade_sec={duration_in_trade}"
            )

            logger.info(
                "TEST STATS | "
                f"total_trades={self.aggregate_stats['total_trades']} "
                f"closed_trades={self.aggregate_stats['closed_trades']} "
                f"filled_trades={self.aggregate_stats['filled_trades']} "
                f"tp_count={self.aggregate_stats['tp_count']} "
                f"sl_count={self.aggregate_stats['sl_count']} "
                f"mixed_exit_count={self.aggregate_stats['mixed_exit_count']} "
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
            realized_quantity = self.normalize_fill_quantity(getattr(execution, "shares", None))
            should_process, execution_identity = self.should_process_execution(execution, fill_time)

            trade_id, record = self.get_trade_by_order_id(order_id)
            if record is None:
                return

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
                record["entry_fill_time"] = fill_time
                previously_entry_filled = record["entry_filled"]
                self.accumulate_quantity_and_notional(
                    record,
                    "cumulative_entry_quantity",
                    "cumulative_entry_notional",
                    "entry_fill_price",
                    realized_quantity,
                    price
                )
                if self.is_entry_fully_filled(record):
                    record["realized_entry_quantity"] = float(record.get("cumulative_entry_quantity") or 0.0)
                    record["entry_filled"] = True
                    record["state"] = "ENTRY_FILLED"
                    if not previously_entry_filled:
                        self.aggregate_stats["filled_trades"] += 1
                        self.append_trade_event(
                            trade_id,
                            f"ENTRY FULLY FILLED quantity={record['realized_entry_quantity']} "
                            f"avg_fill={record['entry_fill_price']}"
                        )
                        logger.info(
                            "ENTRY FULLY FILLED | "
                            f"trade_id={trade_id} quantity={record['realized_entry_quantity']} "
                            f"intended_parent_quantity={record['intended_parent_quantity']} "
                            f"entry_fill_price={record['entry_fill_price']}"
                        )
                else:
                    record["state"] = "ENTRY_WORKING"
                    self.append_trade_event(
                        trade_id,
                        f"ENTRY PARTIAL FILL cumulative_quantity={record['cumulative_entry_quantity']} "
                        f"intended_parent_quantity={record['intended_parent_quantity']} "
                        f"avg_fill={record['entry_fill_price']}"
                    )
                    logger.info(
                        "ENTRY PARTIAL FILL | "
                        f"trade_id={trade_id} cumulative_quantity={record['cumulative_entry_quantity']} "
                        f"intended_parent_quantity={record['intended_parent_quantity']} "
                        f"entry_fill_price={record['entry_fill_price']}"
                    )
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
            should_process, commission_identity = self.should_process_commission(order_id, report)
            if not should_process:
                logger.info(
                    "DUPLICATE COMMISSION IGNORED | "
                    f"order_id={order_id} commission_identity={commission_identity}"
                )
                return

            commission = float(getattr(report, "commission", 0.0) or 0.0)
            trade_id, record = self.get_trade_by_order_id(order_id)
            if record is None:
                return

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

        if trigger_state["trigger_quality"] == "weak":
            soft_blockers.append("weak_trigger")

        if hard_blockers:
            det_classification = "REJECT"
            primary_reason = hard_blockers[0]
        else:
            a_plus_criteria = (
                context_state["bias_state"] == "aligned" and
                context_state["chop_state"] == "not_choppy" and
                context_state["late_state"] == "not_late" and
                context_state["departure_state"] == "not_departed" and
                structure_state["structure_quality"] in ("strong", "moderate") and
                trigger_state["trigger_quality"] == "strong" and
                len(soft_blockers) == 0
            )

            a_criteria = (
                context_state["bias_state"] in ("aligned", "mixed") and
                structure_state["structure_quality"] != "poor" and
                trigger_state["trigger_quality"] in ("strong", "moderate") and
                len(soft_blockers) <= 1
            )

            if a_plus_criteria:
                det_classification = "EXECUTE_A_PLUS"
                primary_reason = "strong_bot_alignment"
            elif a_criteria:
                det_classification = "EXECUTE_A"
                primary_reason = "good_bot_quality"
            else:
                det_classification = "SHADOW"
                primary_reason = soft_blockers[0] if soft_blockers else "insufficient_bot_quality"

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
            context_5m_truth in {"clean", "strong"} and
            structure_1m_truth in {"usable", "clean", "strong"} and
            trigger_truth in {"valid", "strong"}
        ):
            setup_truth = "valid"
        else:
            setup_truth = "weak"

        has_stop_model = plan_models["derived_stop_model"] in {"anchor_based", "trigger_bar_based", "fallback_stop"}
        has_target_model = plan_models["derived_target_model"] in {"fixed_2r", "fallback_2r"}

        if (
            not derived_side or
            not has_entry_basis or
            not has_invalidation_basis or
            not has_stop_model or
            not has_target_model
        ):
            trade_plan_truth = "false"
        elif (
            setup_truth == "strong" and
            plan_models["invalidation_basis_quality"] == "strong" and
            plan_models["stop_basis_quality"] == "strong" and
            plan_models["target_basis_quality"] == "acceptable"
        ):
            trade_plan_truth = "strong"
        elif (
            setup_truth in {"valid", "strong"} and
            plan_models["invalidation_basis_quality"] in {"acceptable", "strong"} and
            plan_models["stop_basis_quality"] in {"acceptable", "strong"} and
            plan_models["target_basis_quality"] == "acceptable"
        ):
            trade_plan_truth = "valid"
        else:
            trade_plan_truth = "weak"

        if (
            trade_plan_truth == "false" or
            trigger_truth == "false" or
            structure_1m_truth == "weak" or
            plan_models["invalidation_basis_quality"] == "weak" or
            plan_models["target_basis_quality"] == "weak"
        ):
            r_truth = "poor"
        elif (
            trade_plan_truth in {"valid", "strong"} and
            setup_truth == "strong" and
            structure_1m_truth == "strong" and
            trigger_truth == "strong" and
            plan_models["stop_basis_quality"] == "strong" and
            plan_models["target_basis_quality"] == "acceptable" and
            timing_truth in {"early", "acceptable"} and
            vwap_truth == "strong"
        ):
            r_truth = "efficient"
        elif (
            trade_plan_truth in {"valid", "strong"} and
            plan_models["stop_basis_quality"] in {"acceptable", "strong"} and
            trigger_truth in {"valid", "strong"} and
            timing_truth in {"early", "acceptable"}
        ):
            r_truth = "acceptable"
        else:
            r_truth = "poor"

        return {
            "market_profile": market_profile,
            "setup_profile": setup_profile,
            "regime_truth": regime_truth,
            "vwap_truth": vwap_truth,
            "context_5m_truth": context_5m_truth,
            "structure_1m_truth": structure_1m_truth,
            "setup_truth": setup_truth,
            "trigger_truth": trigger_truth,
            "timing_truth": timing_truth,
            "trade_plan_truth": trade_plan_truth,
            "r_truth": r_truth,
            "derived_side": derived_side,
            "derived_entry_model": plan_models["derived_entry_model"],
            "derived_invalidation_model": plan_models["derived_invalidation_model"],
            "derived_stop_model": plan_models["derived_stop_model"],
            "derived_target_model": plan_models["derived_target_model"],
            "hard_blockers": list(dict.fromkeys(hard_blockers)),
            "soft_blockers": list(dict.fromkeys(soft_blockers)),
            "primary_reason": primary_reason,
            "secondary_reasons": list(dict.fromkeys(secondary_reasons)),
            "legacy_det_classification": det_classification,
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

        if hard_blockers:
            truth_classification = "REJECT"
        else:
            a_plus_criteria = (
                truth_assessment["regime_truth"] == "strong" and
                truth_assessment["vwap_truth"] == "strong" and
                truth_assessment["context_5m_truth"] == "strong" and
                truth_assessment["structure_1m_truth"] == "strong" and
                truth_assessment["setup_truth"] == "strong" and
                truth_assessment["trigger_truth"] == "strong" and
                truth_assessment["timing_truth"] in ("early", "acceptable") and
                truth_assessment["trade_plan_truth"] == "strong" and
                truth_assessment["r_truth"] == "efficient" and
                len(soft_blockers) == 0 and
                truth_assessment["derived_side"] in {"long", "short"}
            )

            a_criteria = (
                truth_assessment["regime_truth"] in {"usable", "strong"} and
                truth_assessment["vwap_truth"] in {"aligned", "strong"} and
                truth_assessment["context_5m_truth"] in {"clean", "strong"} and
                truth_assessment["structure_1m_truth"] in {"usable", "clean", "strong"} and
                truth_assessment["setup_truth"] in {"valid", "strong"} and
                truth_assessment["trigger_truth"] in {"valid", "strong"} and
                truth_assessment["timing_truth"] in {"early", "acceptable"} and
                truth_assessment["trade_plan_truth"] in {"valid", "strong"} and
                truth_assessment["r_truth"] in {"acceptable", "efficient"} and
                truth_assessment["derived_side"] in {"long", "short"} and
                len(soft_blockers) <= 1
            )

            if a_plus_criteria:
                truth_classification = "A+"
            elif a_criteria:
                truth_classification = "A"
            else:
                truth_classification = "SHADOW"

        logger.info(
            "BOT TRUTH CLASSIFICATION | "
            f"classification={truth_classification} "
            f"primary_reason={truth_assessment['primary_reason']} "
            f"hard_blockers={truth_assessment['hard_blockers']} "
            f"soft_blockers={truth_assessment['soft_blockers']}"
        )
        return truth_classification

    def build_classification_result(self, normalized, assessment):
        truth_classification = assessment.get("truth_classification")
        if truth_classification is None:
            truth_classification = self.classify_truth(assessment)

        if truth_classification == "A+":
            det_classification = "EXECUTE_A_PLUS"
        elif truth_classification == "A":
            det_classification = "EXECUTE_A"
        else:
            det_classification = truth_classification

        execution_permission = truth_classification in ("A", "A+")
        execution_grade = ""

        if truth_classification == "A+":
            execution_grade = "A+"
        elif truth_classification == "A":
            execution_grade = "A"

        return {
            "truth_classification": truth_classification,
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
            "side": assessment["derived_side"],
            "market_profile": assessment["market_profile"],
            "setup_profile": assessment["setup_profile"],
            "regime_truth": assessment["regime_truth"],
            "vwap_truth": assessment["vwap_truth"],
            "context_5m_truth": assessment["context_5m_truth"],
            "structure_1m_truth": assessment["structure_1m_truth"],
            "setup_truth": assessment["setup_truth"],
            "trigger_truth": assessment["trigger_truth"],
            "timing_truth": assessment["timing_truth"],
            "trade_plan_truth": assessment["trade_plan_truth"],
            "r_truth": assessment["r_truth"],
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
        gate_enabled = stage in {"TEST", "PAPER", "LIVE"}
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

    def assess_broker_bracket_confirmation(self, parent_id, tp_id, sl_id):
        expected_ids = {parent_id, tp_id, sl_id}
        broker_orders = {}

        try:
            for trade in self.ib.openTrades():
                order = getattr(trade, "order", None)
                status = getattr(trade, "orderStatus", None)
                order_id = getattr(order, "orderId", None)
                if order_id in expected_ids:
                    broker_orders[order_id] = {
                        "source": "openTrades",
                        "orderId": order_id,
                        "parentId": getattr(order, "parentId", None),
                        "permId": getattr(status, "permId", None),
                        "status": getattr(status, "status", None),
                    }

            for order in self.ib.openOrders():
                order_id = getattr(order, "orderId", None)
                if order_id in expected_ids and order_id not in broker_orders:
                    broker_orders[order_id] = {
                        "source": "openOrders",
                        "orderId": order_id,
                        "parentId": getattr(order, "parentId", None),
                        "permId": getattr(order, "permId", None),
                        "status": None,
                    }
        except Exception:
            logger.exception(
                "BROKER BRACKET CONFIRMATION CHECK FAILED | "
                f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id}"
            )
            return {
                "confirmed": False,
                "reason": "broker_state_check_failed",
                "visible_count": 0,
                "visible_order_ids": [],
                "parent_visible": False,
                "tp_visible": False,
                "sl_visible": False,
                "parent_link_ok": False,
                "tp_link_ok": False,
                "sl_link_ok": False,
                "all_perm_ids_assigned": False,
                "broker_orders": {},
            }

        parent_visible = parent_id in broker_orders
        tp_visible = tp_id in broker_orders
        sl_visible = sl_id in broker_orders

        parent_link_ok = parent_visible and broker_orders[parent_id]["parentId"] in (0, None)
        tp_link_ok = tp_visible and broker_orders[tp_id]["parentId"] == parent_id
        sl_link_ok = sl_visible and broker_orders[sl_id]["parentId"] == parent_id

        all_visible = parent_visible and tp_visible and sl_visible
        all_links_ok = parent_link_ok and tp_link_ok and sl_link_ok
        all_perm_ids_assigned = all(
            broker_orders[order_id].get("permId") not in (None, 0)
            for order_id in expected_ids
            if order_id in broker_orders
        ) and all_visible

        confirmed = all_visible and all_links_ok and all_perm_ids_assigned

        if not all_visible:
            reason = "broker_missing_one_or_more_bracket_legs"
        elif not all_links_ok:
            reason = "broker_bracket_linkage_invalid"
        elif not all_perm_ids_assigned:
            reason = "broker_perm_id_assignment_incomplete"
        else:
            reason = "broker_visible_three_leg_chain_confirmed"

        return {
            "confirmed": confirmed,
            "reason": reason,
            "visible_count": len(broker_orders),
            "visible_order_ids": sorted(broker_orders.keys()),
            "parent_visible": parent_visible,
            "tp_visible": tp_visible,
            "sl_visible": sl_visible,
            "parent_link_ok": parent_link_ok,
            "tp_link_ok": tp_link_ok,
            "sl_link_ok": sl_link_ok,
            "all_perm_ids_assigned": all_perm_ids_assigned,
            "broker_orders": broker_orders,
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

    def build_det_execution_job(self, normalized, classification_result):
        """Build execution job with DET planning details.
        P050: Includes new enriched fields for structure-aware stop derivation.
        P049: Moved stop/target derivation to place_bracket_order() after final executable entry.
        This job now carries planning info only; final R is computed at execution time.
        """
        execution_grade = classification_result["execution_grade"]
        reference_price = self.get_preferred_reference_price(normalized)
        symbol = normalized["symbol"]
        side = normalized["side"]
        planned_executable_entry = self.derive_entry_price_from_candidate(normalized)

        # Get intended risk percent for grade
        intended_risk_percent = self.get_grade_risk_percent(execution_grade)

        return {
            "symbol": symbol,
            "side": side,
            "bot_stage": BOT_STAGE,
            "entry": reference_price,
            "signal_time": normalized["timestamp_utc"] or normalized["time"],
            "enqueue_time": self.utc_now_iso(),
            "grade": execution_grade,
            "intended_risk_percent": intended_risk_percent,
            "det_classification": classification_result["det_classification"],
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
        logger.info(f"WEBHOOK RECEIVED: {data}")

        if not isinstance(data, dict):
            logger.error(f"INVALID PAYLOAD TYPE: {type(data)}")
            return "invalid_payload_type"

        normalized = self.normalize_signal(data)
        self.log_normalized_signal(normalized)

        if normalized["payload_format"] == "diagnostic":
            self.log_diagnostic_signal(data)
            observation_layer = self.build_observation_layer(normalized)
            assessment = self.assess_truth(
                observation_layer,
                observation_layer["market_profile"],
                observation_layer["setup_profile"]
            )
            assessment["truth_classification"] = self.classify_truth(assessment)
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
            observation_layer = self.build_observation_layer(normalized)
            assessment = self.assess_truth(
                observation_layer,
                observation_layer["market_profile"],
                observation_layer["setup_profile"]
            )
            assessment["truth_classification"] = self.classify_truth(assessment)
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

            if not policy["allow_queue"]:
                logger.info(
                    "QUEUE DECISION | "
                    f"path={policy['policy_branch']} "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"det_classification={classification_result['det_classification']} "
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
                    f"det_classification={classification_result['det_classification']} "
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
                    f"det_classification={classification_result['det_classification']} "
                    f"queued=false "
                    "blocked_by=missing_price_reference "
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

            job = self.build_det_execution_job(normalized, classification_result)

            logger.info(
                "QUEUE PUT | "
                f"symbol={job['symbol']} "
                f"side={job['side']} "
                f"reference_price={job['reference_price']} "
                f"planned_executable_entry={job.get('planned_executable_entry')} "
                f"grade={job['grade']} "
                f"intended_risk_pct={job['intended_risk_percent']*100:.1f}% "
                f"det_classification={job['det_classification']} "
                f"payload_format={job['payload_format']} "
                f"signal_id={job['signal_id']}"
            )

            self.execution_queue.put(job)
            logger.info(
                "QUEUE DECISION | "
                f"path=det_gated_execution "
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
        stop = self.derive_det_stop_price(symbol, entry, side, execution_grade, normalized_signal)
        stop = self.round_to_tick(symbol, stop)

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
        logger.info(
            "SIZING PLAN | "
            f"symbol={symbol} "
            f"reference_price={entry_plan['reference_price']} "
            f"spread_adjusted_entry={spread_adjusted_entry} "
            f"final_entry={entry} "
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

        normalized_size = sizing_result["normalized_position_size"]
        job["allowed_money_risk"] = sizing_result["allowed_money_risk"]
        job["stop_distance_points"] = sizing_result["stop_distance_points"]
        job["raw_position_size"] = sizing_result["raw_position_size"]
        job["normalized_position_size"] = normalized_size

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
            f"spread_adjusted_entry={spread_adjusted_entry} "
            f"trigger_bar_high={job.get('normalized_signal', {}).get('trigger_bar_high', 'N/A')} "
            f"trigger_bar_low={job.get('normalized_signal', {}).get('trigger_bar_low', 'N/A')} "
            f"structure_anchor_low={job.get('normalized_signal', {}).get('structure_anchor_low', 'N/A')} "
            f"structure_anchor_high={job.get('normalized_signal', {}).get('structure_anchor_high', 'N/A')} "
            f"final_entry={entry} "
            f"final_stop={stop} "
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
        broker_confirmation = None

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
            if broker_confirmation["confirmed"]:
                confirmed = True
                break

            self.ib.sleep(0.10)

        self.log_trade_snapshot("POST CONFIRM PARENT", parent_trade)
        self.log_trade_snapshot("POST CONFIRM TP", tp_trade)
        self.log_trade_snapshot("POST CONFIRM SL", sl_trade)

        if broker_confirmation is None:
            broker_confirmation = self.assess_broker_bracket_confirmation(parent_id, tp_id, sl_id)

        logger.info(
            f"BRACKET CONFIRMATION ASSESSMENT | confirmed={confirmed} | "
            f"parent: orderId={parent_id} status={parent_status} permId={parent_perm} | "
            f"tp: orderId={tp_id} status={tp_status} permId={tp_perm} | "
            f"sl: orderId={sl_id} status={sl_status} permId={sl_perm} | "
            f"broker_visible_order_ids={broker_confirmation['visible_order_ids']} | "
            f"parent_visible={broker_confirmation['parent_visible']} "
            f"tp_visible={broker_confirmation['tp_visible']} "
            f"sl_visible={broker_confirmation['sl_visible']} | "
            f"parent_link_ok={broker_confirmation['parent_link_ok']} "
            f"tp_link_ok={broker_confirmation['tp_link_ok']} "
            f"sl_link_ok={broker_confirmation['sl_link_ok']} | "
            f"all_perm_ids_assigned={broker_confirmation['all_perm_ids_assigned']} | "
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

        if not confirmed:
            self.append_anomaly(trade_id, "BROKER_BRACKET_CONFIRMATION_FAILED")
            self.trade_analysis[trade_id]["state"] = "INCOMPLETE"
            self.finalize_trade_if_complete(trade_id)
            logger.error(
                f"BRACKET NOT CONFIRMED | trade_id={trade_id} "
                f"parent_order_id={parent_id} tp_order_id={tp_id} sl_order_id={sl_id} "
                f"broker_reason={broker_confirmation['reason']} "
                f"broker_visible_order_ids={broker_confirmation['visible_order_ids']}"
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
