# ==========================================================
# IKBR SCALPING BOT | VERSION v1.6.0 P214 | STAGE: ACC
# ==========================================================

import logging
import asyncio
import threading
import queue
import time
import decimal
import json
import os
import socket
import hashlib
import hmac
import uuid
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, HTTPException
from ib_insync import IB, Future, Forex, LimitOrder, StopOrder, MarketOrder, ExecutionFilter

# ==========================================================
# VERSIONING
# ==========================================================

BOT_NAME = "IKBR_SCALPING_BOT"
BOT_VERSION = "v1.6.0"
BOT_PATCH = "P214"
BOT_STAGE = "ACC"  # TST | ACC | PRD

def generate_bot_run_id():
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%d-%H%M%S')}-{now.microsecond:06d}-{uuid.uuid4().hex[:8]}"

logger = logging.getLogger("ikbr_scalpingbot")
logging.basicConfig(level=logging.INFO)

AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")
DEFAULT_IB_CLIENT_ID = 1
WEBHOOK_SECRET_ENV_VAR = "IKBR_WEBHOOK_SECRET"
PRD_DRY_RUN_ENV_VAR = "IKBR_PRD_DRY_RUN"
EXECUTION_TRANSMISSION_MODE_ENV_VAR = "EXECUTION_TRANSMISSION_MODE"
LIVE_ORDER_APPROVAL_ENV_VAR = "IKBR_ALLOW_LIVE_ORDERS"
PRD_LIVE_APPROVAL_ENV_VAR = "IKBR_PRD_LIVE_APPROVED"
BOT_ACCOUNT_CONTEXT = os.getenv("IKBR_ACCOUNT_CONTEXT", "default")
SENSITIVE_PAYLOAD_KEYS = {
    "secret",
    "token",
    "password",
    "auth",
    "authorization",
    "api_key",
    "apikey",
    "webhook_secret",
    "webhooksecret",
}
REDACTED_VALUE = "[REDACTED]"
RUNTIME_LOCK_SOCKET = None
RUNTIME_LOCK_KEY = None
bot = None
bot_init_lock = threading.Lock()

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
        "containment_max_size": 10,
        "notional_model": "future_contract_value",
        "notional_currency": "USD",
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
        "containment_max_size": 10,
        "notional_model": "future_contract_value",
        "notional_currency": "USD",
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
        "max_size": None,
        "containment_max_size": 10,
        "notional_model": "future_contract_value",
        "notional_currency": "EUR",
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
        "containment_max_size": 10,
        "notional_model": "fx_future_contract_value",
        "notional_currency": "USD",
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
        "containment_max_size": 100000,
        "notional_model": "spot_fx_base_units",
        "notional_currency": "USD",
        "session_profile": "FX",
    },
}

QUALIFIED_FUTURE_SYMBOLS = ("MNQ", "MES", "M6E", "FDXM")

# Runtime kill-switch only. Empty means all configured futures are enabled.
RUNTIME_DISABLED_SYMBOLS = set()

EXECUTION_CAPITAL_BASE = 170000.0
EURUSD_EXECUTION_DISABLED_REASON = "eurusd_execution_disabled_spot_fx_sizing_not_implemented"

CONTAINMENT_STAGE_PROFILES = {
    "TST": {
        "profile": "TST_WIDE",
        "exceeds_cap_action": "cap",
        "max_sizes": {
            "MNQ": 10,
            "MES": 10,
            "FDXM": 10,
            "M6E": 10,
            "EURUSD": 100000,
        },
        "max_notionals": {
            "MNQ": 800000,
            "MES": 300000,
            "FDXM": 1500000,
            "M6E": 200000,
            "EURUSD": 100000,
        },
    },
    "ACC": {
        "profile": "ACC_CONSERVATIVE",
        "exceeds_cap_action": "cap",
        "max_sizes": {
            "MNQ": 10,
            "MES": 10,
            "FDXM": 10,
            "M6E": 10,
            "EURUSD": 100000,
        },
        "max_notionals": {
            "MNQ": 800000,
            "MES": 300000,
            "FDXM": 1500000,
            "M6E": 200000,
            "EURUSD": 100000,
        },
    },
    "PRD": {
        "profile": "PRD_DET_CONTROLLED",
        "exceeds_cap_action": "cap",
        "max_sizes": {
            "MNQ": 10,
            "MES": 10,
            "FDXM": 10,
            "M6E": 10,
            "EURUSD": 100000,
        },
        "max_notionals": {
            "MNQ": 800000,
            "MES": 300000,
            "FDXM": 1500000,
            "M6E": 200000,
            "EURUSD": 100000,
        },
    },
}

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
BROKER_READ_SLOW_MS = 250.0
BROKER_WRITE_SLOW_MS = 250.0
WEBHOOK_SLOW_MS = 500.0
RECONNECT_FILL_RECONSTRUCTION_LOOKBACK_SECONDS = 900.0
PROTECTIVE_SL_MISSING_POLICY = "EMERGENCY_FLATTEN"
PROTECTIVE_SL_MISSING_MAX_ATTEMPTS = 3
PROTECTIVE_SL_MISSING_RECHECK_SECONDS = 1.0
PROTECTIVE_SL_MISSING_CANCEL_CONFLICTING_ORDERS = True
PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS = 10.0
TIME_EXIT_ENABLED = True
TIME_EXIT_AFTER_SECONDS = 300.0
TIME_EXIT_MAX_ATTEMPTS = 3
TIME_EXIT_REASON = "TIME_EXIT_5_MIN_MAX_DURATION"

PROTECTIVE_EMERGENCY_INFLIGHT_STATUSES = {
    None,
    "",
    "PendingSubmit",
    "ApiPending",
    "PreSubmitted",
    "Submitted",
    "PendingCancel",
    "unknown_recent",
    "submit_in_progress",
    "flatten_order_submitted",
    "in_flight",
    "pending_position_confirmation",
    "submit_failed_submission_uncertain",
    "broker_submission_uncertain",
}

PROTECTIVE_EMERGENCY_TERMINAL_STATUSES = {
    "Cancelled",
    "ApiCancelled",
    "Inactive",
    "Rejected",
}

PROTECTIVE_EMERGENCY_EXIT_REASON = "EMERGENCY_FLATTEN_SL_MISSING"

SESSION_CLOSE_ENABLED = True
NO_NEW_ENTRIES_BEFORE_CLOSE_MIN = 15
EOD_FLATTEN_ENABLED = True
EOD_FLATTEN_BEFORE_CLOSE_MIN = 5
EOD_FLATTEN_ORDER_TYPE = "MKT"
EOD_FLATTEN_MAX_ATTEMPTS = 3
EOD_FLATTEN_RECHECK_SECONDS = 2
EOD_FLATTEN_SCOPE = "CONFIGURED_BOT_SYMBOLS"
ACCOUNT_WIDE_FLATTEN = False
SESSION_CLOSE_FORCE_BLOCK_NEW_ENTRIES = True
SESSION_CLOSE_CANCEL_WORKING_ENTRIES = True
SESSION_CLOSE_MONITOR_POLL_SECONDS = 5.0
SESSION_CLOSE_SYSTEM_JOB_DEDUPE_SECONDS = 20.0

SESSION_FLAT_BY_TIMES = {
    "MES": {"timezone": "America/Chicago", "flat_by": "16:00"},
    "MNQ": {"timezone": "America/Chicago", "flat_by": "16:00"},
    "M6E": {"timezone": "America/Chicago", "flat_by": "16:00"},
    "FDXM": {"timezone": "Europe/Berlin", "flat_by": "22:00"},
}

SESSION_CLOSE_SYSTEM_JOB_TYPES = {
    "SESSION_CLOSE_SWEEP",
    "SESSION_CLOSE_FLATTEN",
}

BROKER_SYSTEM_JOB_TYPES = SESSION_CLOSE_SYSTEM_JOB_TYPES | {
    "RECONNECT_RECOVERY",
    "BROKER_REALITY_RECONCILIATION",
}

SHADOW_TEST_PROHIBITED_REASONS = {
    "chop",
    "late",
    "poor_structure",
    "conflicting_bias",
    "session_invalid",
}

ACTIVE_TRADE_STATES = {
    "SUBMITTING",
    "BRACKET_SUBMIT_FAILED_UNCERTAIN",
    "BROKER_ACK_PENDING",
    "ENTRY_WORKING",
    "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL",
    "ENTRY_FILLED",
    "EXIT_WORKING",
    "TP_FILLED",
    "SL_FILLED",
    "TIME_EXIT_ARMED",
    "TIME_EXIT_DUE",
    "TIME_EXIT_SUBMITTED",
    "TIME_EXIT_PENDING_CONFIRMATION",
    "TIME_EXIT_FAILED_UNCERTAIN",
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

CONTAINMENT_ACTION_HIERARCHY = (
    "approve_as_is",
    "capped",
    "denied",
)

CONTAINMENT_DECISION_SOURCES = {
    "none",
    "size_containment",
    "exposure_containment",
    "stage_containment",
}

def get_configured_webhook_secret():
    return os.getenv(WEBHOOK_SECRET_ENV_VAR)

def webhook_secret_required():
    return BOT_STAGE in {"ACC", "PRD"}

def validate_webhook_secret_config():
    configured_secret = get_configured_webhook_secret()
    if webhook_secret_required() and not configured_secret:
        raise RuntimeError(f"{WEBHOOK_SECRET_ENV_VAR}_missing_for_{BOT_STAGE.lower()}")
    return configured_secret

def parse_env_bool(value):
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None

def get_execution_transmission_mode():
    value = os.getenv(EXECUTION_TRANSMISSION_MODE_ENV_VAR)
    if value is None or not str(value).strip():
        return None
    return str(value).strip().lower()

def normalize_sensitive_payload_key(key):
    return "".join(
        character
        for character in str(key).lower()
        if character.isalnum()
    )

def is_sensitive_payload_key(key):
    normalized_key = normalize_sensitive_payload_key(key)
    normalized_sensitive_keys = {
        normalize_sensitive_payload_key(sensitive_key)
        for sensitive_key in SENSITIVE_PAYLOAD_KEYS
    }
    return any(
        sensitive_key and sensitive_key in normalized_key
        for sensitive_key in normalized_sensitive_keys
    )

def redact_sensitive_payload(value):
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            if is_sensitive_payload_key(key):
                redacted[key] = REDACTED_VALUE
            else:
                redacted[key] = redact_sensitive_payload(item)
        return redacted
    if isinstance(value, list):
        return [redact_sensitive_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive_payload(item) for item in value)
    return value

def build_sanitized_payload_summary(payload):
    if not isinstance(payload, dict):
        return {"payload_type": type(payload).__name__}
    return {
        "payload_format_hint": payload.get("payload_format") or payload.get("mode") or payload.get("event"),
        "symbol": payload.get("symbol"),
        "side": payload.get("side") or payload.get("candidate_side") or payload.get("direction"),
        "grade": payload.get("grade") or payload.get("candidate_grade") or payload.get("tv_candidate_grade"),
        "signal_id": payload.get("signal_id"),
        "schema_version": payload.get("schema_version"),
        "sanitized_payload": redact_sensitive_payload(payload),
    }

def get_runtime_lock_key():
    return f"{BOT_NAME}:{BOT_STAGE}:client_id={DEFAULT_IB_CLIENT_ID}:account={BOT_ACCOUNT_CONTEXT}"

def get_runtime_lock_port(lock_key):
    digest = hashlib.sha256(lock_key.encode("utf-8")).hexdigest()
    return 20000 + (int(digest[:8], 16) % 20000)

def acquire_runtime_process_lock():
    global RUNTIME_LOCK_SOCKET, RUNTIME_LOCK_KEY
    if RUNTIME_LOCK_SOCKET is not None:
        return

    lock_key = get_runtime_lock_key()
    lock_port = get_runtime_lock_port(lock_key)
    lock_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
    try:
        lock_socket.bind(("127.0.0.1", lock_port))
        lock_socket.listen(1)
    except OSError as exc:
        lock_socket.close()
        logger.critical(
            "RUNTIME_SINGLETON_LOCK_FAILED | "
            f"lock_key={lock_key} "
            f"lock_port={lock_port} "
            f"reason={exc} "
            "decision=fail_startup_closed"
        )
        raise RuntimeError(f"runtime_singleton_lock_unavailable:{lock_key}:{lock_port}") from exc

    RUNTIME_LOCK_SOCKET = lock_socket
    RUNTIME_LOCK_KEY = lock_key
    logger.warning(
        "RUNTIME_SINGLETON_LOCK_ACQUIRED | "
        f"lock_key={lock_key} "
        f"lock_port={lock_port}"
    )

def release_runtime_process_lock():
    global RUNTIME_LOCK_SOCKET, RUNTIME_LOCK_KEY
    if RUNTIME_LOCK_SOCKET is None:
        return
    try:
        RUNTIME_LOCK_SOCKET.close()
        logger.warning(
            "RUNTIME_SINGLETON_LOCK_RELEASED | "
            f"lock_key={RUNTIME_LOCK_KEY}"
        )
    finally:
        RUNTIME_LOCK_SOCKET = None
        RUNTIME_LOCK_KEY = None

# ==========================================================
# BOT
# ==========================================================

class ScalpingBot:

    def __init__(self):

        self.ib = IB()

        self.IB_HOST = "127.0.0.1"
        self.IB_PORT = 7497
        self.IB_CLIENT_ID = DEFAULT_IB_CLIENT_ID
        self.run_id = generate_bot_run_id()
        logger.warning(
            "RUN_ID_CREATED | "
            f"run_id={self.run_id} "
            f"bot_name={BOT_NAME} "
            f"bot_version={BOT_VERSION} "
            f"bot_patch={BOT_PATCH} "
            f"bot_stage={BOT_STAGE}"
        )

        self.contract_cache = {}
        self.contract_min_ticks = {}
        self.execution_queue = queue.Queue()
        self.broker_io_owner_mode = "enforce_single_owner"
        self.broker_io_owner_expected = "execution_worker"
        self.broker_io_owner_thread_ident = None
        self.broker_io_owner_thread_name = None
        self.broker_system_job_last_queued = {}

        # Legacy observability flag only; not the authoritative execution-safety gate.
        # Active trade authority lives in trade_analysis + broker reality + concurrency policy.
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
        self.session_initialization_failed = False
        self.session_initialization_failure_reason = None
        self.session_reconnect_count = 0
        self.last_reconnect_time = None
        self.events_attached = False
        self.session_recovery_in_progress = False
        self.connectivity_uncertain = False
        self.connectivity_uncertain_since = None
        self.connectivity_reconstruction_anchor_since = None
        self.connectivity_last_error_code = None
        self.connectivity_last_error_message = None
        self.connectivity_reconciliation_required = False
        self.connectivity_reconciliation_completed_at = None
        self.reconnect_unmatched_fill_dedupe = {}

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
        self.paper_daily_stop_day_key = None
        self.paper_symbol_daily_sl_stops = {}
        self.session_close_config_logged = False
        self.session_close_last_system_job_queued = {}
        self.session_close_symbol_states = {}
        self.startup_reconciliation_required = True
        self.startup_reconciliation_completed = False
        self.startup_reconciliation_started_at = None
        self.startup_reconciliation_completed_at = None
        self.startup_reconciliation_status = "required"
        self.startup_reconciliation_block_reason = "startup_reconciliation_required"
        self.startup_reconciliation_ambiguous_symbols = set()
        self.external_entries_blocked_reason = "startup_reconciliation_required"

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
            "dry_run_planned_count": 0,
            "dry_run_not_transmitted_count": 0,
            "submitted_count": 0,
            "shadow_test_submitted_count": 0,
            "broker_acknowledged_count": 0,
            "broker_live_count": 0,
            "shadow_test_entry_filled_count": 0,
            "shadow_test_tp_count": 0,
            "shadow_test_sl_count": 0,
            "shadow_test_incomplete_count": 0,
            "emergency_flatten_count": 0,
            "time_exit_count": 0,
            "gross_pnl": 0.0,
            "commission": 0.0,
            "net_pnl": 0.0,
            "shadow_test_net_pnl": 0.0,
        }
        self.symbol_stats = {
            symbol: {
                "total_trades": 0,
                "dry_run_planned_count": 0,
                "dry_run_not_transmitted_count": 0,
                "filled_trades": 0,
                "tp_count": 0,
                "sl_count": 0,
                "emergency_flatten_count": 0,
                "time_exit_count": 0,
                "gross_pnl": 0.0,
                "commission": 0.0,
                "net_pnl": 0.0,
            }
            for symbol in INSTRUMENT_SPECS.keys()
        }

        threading.Thread(target=self.execution_worker, daemon=True).start()
        threading.Thread(target=self.ib_watchdog, daemon=True).start()
        threading.Thread(target=self.session_close_monitor, daemon=True).start()

    # ==========================================================
    # PRICE LOGIC
    # ==========================================================

    def get_instrument_spec(self, symbol):
        spec = INSTRUMENT_SPECS.get(symbol)
        if spec is None:
            raise ValueError(f"Unsupported symbol: {symbol}")
        return spec

    def get_order_tif(self, symbol, order_role=None):
        try:
            spec = self.get_instrument_spec(symbol)
        except Exception as exc:
            logger.error(
                "ORDER_TIF_RESOLUTION_FAILED | "
                f"symbol={symbol} "
                f"order_role={order_role} "
                f"reason=instrument_spec_unavailable:{exc}"
            )
            return None

        broker_type = spec.get("broker_type")
        if broker_type in {"future", "forex"}:
            return "DAY"

        logger.warning(
            "ORDER_TIF_RESOLUTION_FAILED | "
            f"symbol={symbol} "
            f"order_role={order_role} "
            f"broker_type={broker_type} "
            "reason=unsupported_broker_type"
        )
        return None

    def get_order_transmission_policy(self):
        mode = get_execution_transmission_mode()
        dry_run_flag_raw = os.getenv(PRD_DRY_RUN_ENV_VAR)
        live_approval_raw = os.getenv(LIVE_ORDER_APPROVAL_ENV_VAR)
        prd_live_approval_raw = os.getenv(PRD_LIVE_APPROVAL_ENV_VAR)
        dry_run_flag = parse_env_bool(dry_run_flag_raw)
        live_approval = parse_env_bool(live_approval_raw)
        prd_live_approval = parse_env_bool(prd_live_approval_raw)
        invalid_reasons = []

        if mode is not None and mode not in {"dry_run", "live"}:
            invalid_reasons.append(f"invalid_{EXECUTION_TRANSMISSION_MODE_ENV_VAR}")
        if dry_run_flag_raw is not None and dry_run_flag is None:
            invalid_reasons.append(f"invalid_{PRD_DRY_RUN_ENV_VAR}")
        if live_approval_raw is not None and live_approval is None:
            invalid_reasons.append(f"invalid_{LIVE_ORDER_APPROVAL_ENV_VAR}")
        if prd_live_approval_raw is not None and prd_live_approval is None:
            invalid_reasons.append(f"invalid_{PRD_LIVE_APPROVAL_ENV_VAR}")
        if mode == "dry_run" and dry_run_flag is False:
            invalid_reasons.append("dry_run_mode_conflicts_with_false_flag")
        if mode == "live" and dry_run_flag is True:
            invalid_reasons.append("live_mode_conflicts_with_dry_run_flag")

        dry_run_enabled = (
            BOT_STAGE == "PRD" and
            (mode == "dry_run" or dry_run_flag is True)
        )

        if BOT_STAGE != "PRD":
            live_allowed = not dry_run_enabled
            reason = "non_prd_stage_transmission_policy"
        elif dry_run_enabled:
            live_allowed = False
            reason = "prd_dry_run_no_order_transmission"
        elif invalid_reasons:
            live_allowed = False
            reason = "prd_transmission_config_invalid:" + ",".join(invalid_reasons)
        elif mode == "live" and live_approval is True and prd_live_approval is True:
            live_allowed = True
            reason = "prd_live_order_transmission_explicitly_approved"
        else:
            live_allowed = False
            reason = "prd_live_order_transmission_not_explicitly_approved"

        return {
            "stage": BOT_STAGE,
            "mode": mode,
            "dry_run_enabled": dry_run_enabled,
            "live_allowed": live_allowed,
            "live_approval": live_approval,
            "prd_live_approval": prd_live_approval,
            "invalid_reasons": invalid_reasons,
            "reason": reason,
        }

    def is_prd_dry_run_enabled(self):
        return self.get_order_transmission_policy()["dry_run_enabled"]

    def is_live_order_transmission_allowed(self):
        return self.get_order_transmission_policy()["live_allowed"]

    def get_enabled_futures_symbols(self):
        return [
            symbol for symbol in QUALIFIED_FUTURE_SYMBOLS
            if symbol in INSTRUMENT_SPECS and symbol not in RUNTIME_DISABLED_SYMBOLS
        ]

    def is_webhook_secret_production_ready(self):
        secret = get_configured_webhook_secret()
        if not secret:
            return False
        secret_text = str(secret).strip()
        unsafe_values = {
            "",
            "secret",
            "changeme",
            "default",
            "password",
            "test",
            "fdax_bot_secure_2026",
        }
        return len(secret_text) >= 16 and secret_text.lower() not in unsafe_values

    def evaluate_prd_live_release_gates(self, symbol=None, job=None, trade_id=None):
        passed_gates = []
        failed_gates = []
        unknown_gates = []

        def gate(name, condition, unknown=False):
            if unknown:
                unknown_gates.append(name)
            elif condition:
                passed_gates.append(name)
            else:
                failed_gates.append(name)

        policy = self.get_order_transmission_policy()
        enabled_symbols = self.get_enabled_futures_symbols()
        target_symbol = symbol or (job or {}).get("symbol")
        prd_profile = CONTAINMENT_STAGE_PROFILES.get("PRD")

        gate("stage_is_prd", BOT_STAGE == "PRD")
        gate("prd_live_release_approved", policy.get("prd_live_approval") is True)
        gate("prd_dry_run_disabled", not policy.get("dry_run_enabled"))
        gate("webhook_secret_production_ready", self.is_webhook_secret_production_ready())
        gate("runtime_singleton_lock_acquired", RUNTIME_LOCK_SOCKET is not None)
        gate("broker_io_owner_enforced", getattr(self, "broker_io_owner_mode", None) == "enforce_single_owner")
        gate("execution_worker_owner_registered", self.get_broker_io_owner_thread_ident() is not None)
        gate("startup_reconciliation_completed", getattr(self, "startup_reconciliation_completed", None) is True)
        gate("startup_reconciliation_clear", getattr(self, "startup_reconciliation_status", None) == "clear")
        gate(
            "startup_reconciliation_no_ambiguity",
            not bool(getattr(self, "startup_reconciliation_ambiguous_symbols", set())),
        )
        gate("connectivity_reconciliation_not_pending", not bool(getattr(self, "connectivity_reconciliation_required", True)))
        gate("connectivity_not_uncertain", not bool(getattr(self, "connectivity_uncertain", True)))
        gate(
            "ibkr_session_healthy",
            bool(getattr(self, "session_socket_connected", False)) and
            bool(getattr(self, "session_initialized", False)) and
            bool(getattr(self, "session_healthy", False)),
        )

        contracts_ready = True
        for enabled_symbol in enabled_symbols:
            contract = self.contract_cache.get(enabled_symbol) if hasattr(self, "contract_cache") else None
            spec = INSTRUMENT_SPECS.get(enabled_symbol, {})
            if contract is None:
                contracts_ready = False
                break
            if getattr(contract, "conId", None) in (None, 0):
                contracts_ready = False
                break
            if not getattr(contract, "localSymbol", None):
                contracts_ready = False
                break
            if spec.get("trading_class") and getattr(contract, "tradingClass", None) != spec.get("trading_class"):
                contracts_ready = False
                break
            if getattr(contract, "currency", None) != spec.get("currency"):
                contracts_ready = False
                break
        gate("qualified_contracts_ready", contracts_ready)

        gate(
            "symbol_supported_and_enabled",
            target_symbol is None or (
                target_symbol in INSTRUMENT_SPECS and
                target_symbol not in RUNTIME_DISABLED_SYMBOLS and
                target_symbol != "EURUSD"
            ),
        )
        gate("eurusd_execution_disabled", EURUSD_EXECUTION_DISABLED_REASON and target_symbol != "EURUSD")

        prd_profile_valid = bool(prd_profile and prd_profile.get("profile"))
        if prd_profile_valid:
            for enabled_symbol in enabled_symbols:
                max_size = prd_profile.get("max_sizes", {}).get(enabled_symbol)
                max_notional = prd_profile.get("max_notionals", {}).get(enabled_symbol)
                if max_size is None or max_size <= 0 or max_notional is None or max_notional <= 0:
                    prd_profile_valid = False
                    break
        gate("prd_containment_profile_valid", prd_profile_valid)

        session_close_valid = bool(SESSION_CLOSE_ENABLED)
        for enabled_symbol in enabled_symbols:
            config = SESSION_FLAT_BY_TIMES.get(enabled_symbol)
            if not config or not config.get("timezone") or not config.get("flat_by"):
                session_close_valid = False
                break
        gate("session_close_configured", session_close_valid)
        gate("prd_daily_sl_stop_clear", getattr(self, "live_daily_stop_active", None) is False)
        gate("no_active_execution_trade", getattr(self, "active_execution_trade_id", None) is None)
        gate(
            "no_ambiguous_broker_reality_cached",
            getattr(self, "startup_reconciliation_status", None) == "clear" and
            not bool(getattr(self, "startup_reconciliation_ambiguous_symbols", set())) and
            not bool(getattr(self, "external_entries_blocked_reason", None)),
        )
        gate(
            "raw_secret_logging_disabled",
            "secret" in SENSITIVE_PAYLOAD_KEYS and REDACTED_VALUE == "[REDACTED]",
        )
        gate(
            "live_transmission_approval_explicit",
            policy.get("mode") == "live" and
            policy.get("live_approval") is True and
            policy.get("prd_live_approval") is True,
        )

        allowed = not failed_gates and not unknown_gates
        reason = "prd_live_release_gates_passed" if allowed else "prd_live_release_gates_blocked"
        return {
            "allowed": allowed,
            "stage": BOT_STAGE,
            "failed_gates": failed_gates,
            "passed_gates": passed_gates,
            "unknown_gates": unknown_gates,
            "reason": reason,
            "policy": policy,
        }

    def assert_prd_live_release_allowed(self, symbol=None, job=None, trade_id=None):
        if BOT_STAGE != "PRD":
            return {
                "allowed": True,
                "stage": BOT_STAGE,
                "failed_gates": [],
                "passed_gates": ["non_prd_stage_not_applicable"],
                "unknown_gates": [],
                "reason": "non_prd_stage_not_applicable",
            }

        gate_result = self.evaluate_prd_live_release_gates(symbol=symbol, job=job, trade_id=trade_id)
        if gate_result["allowed"]:
            logger.warning(
                "PRD_LIVE_RELEASE_GATES_PASSED | "
                f"symbol={symbol} "
                f"trade_id={trade_id} "
                f"passed_gates={','.join(gate_result['passed_gates'])} "
                f"reason={gate_result['reason']}"
            )
            return gate_result

        logger.critical(
            "PRD_LIVE_RELEASE_GATE_BLOCKED | "
            f"symbol={symbol} "
            f"trade_id={trade_id} "
            f"failed_gates={','.join(gate_result['failed_gates'])} "
            f"unknown_gates={','.join(gate_result['unknown_gates'])} "
            f"reason={gate_result['reason']} "
            "operator_action_required=True"
        )
        if trade_id is not None:
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["state"] = "PRD_LIVE_RELEASE_BLOCKED"
                    record["transmitted"] = False
                    record["transmission_block_reason"] = gate_result["reason"]
                    record["execution_validation_status"] = "prd_live_release_blocked"
                    self.append_trade_event(
                        trade_id,
                        "PRD_LIVE_RELEASE_BLOCKED "
                        f"failed_gates={','.join(gate_result['failed_gates'])} "
                        f"unknown_gates={','.join(gate_result['unknown_gates'])}",
                    )
                    if self.active_execution_trade_id == trade_id:
                        self.active_execution_trade_id = None
        raise RuntimeError(gate_result["reason"])

    def assert_order_transmission_allowed(self, symbol=None, trade_id=None, reason_label=None):
        policy = self.get_order_transmission_policy()
        if policy["live_allowed"]:
            return policy

        log_label = (
            "ORDER_TRANSMISSION_BLOCKED_DRY_RUN"
            if policy["dry_run_enabled"]
            else "ORDER_TRANSMISSION_BLOCKED_FAIL_CLOSED"
        )
        logger.critical(
            f"{log_label} | "
            f"stage={policy['stage']} "
            f"mode={policy['mode']} "
            f"dry_run_enabled={policy['dry_run_enabled']} "
            f"live_approval={policy['live_approval']} "
            f"symbol={symbol} "
            f"trade_id={trade_id} "
            f"reason_label={reason_label} "
            "operator_action_required=True "
            f"reason={policy['reason']}"
        )
        raise RuntimeError(policy["reason"])

    def assert_cancel_mutation_allowed(self, symbol=None, trade_id=None, reason_label=None, real_broker_exposure=False):
        policy = self.get_order_transmission_policy()
        if policy["stage"] != "PRD":
            return policy
        if not policy["dry_run_enabled"]:
            if policy["live_allowed"]:
                return policy
            logger.critical(
                "CANCEL_MUTATION_BLOCKED_FAIL_CLOSED | "
                f"stage={policy['stage']} "
                f"mode={policy['mode']} "
                f"live_approval={policy['live_approval']} "
                f"symbol={symbol} "
                f"trade_id={trade_id} "
                f"reason_label={reason_label} "
                f"reason={policy['reason']}"
            )
            raise RuntimeError(policy["reason"])
        if real_broker_exposure:
            logger.warning(
                "PRD_DRY_RUN_CANCEL_ALLOWED_REAL_EXPOSURE | "
                f"symbol={symbol} "
                f"trade_id={trade_id} "
                f"reason_label={reason_label} "
                "reason=prd_dry_run_real_broker_exposure_cleanup"
            )
            return policy

        logger.critical(
            "CANCEL_MUTATION_BLOCKED_DRY_RUN | "
            f"symbol={symbol} "
            f"trade_id={trade_id} "
            f"reason_label={reason_label} "
            "operator_action_required=True "
            "reason=prd_dry_run_cancel_requires_real_broker_exposure"
        )
        raise RuntimeError("prd_dry_run_cancel_requires_real_broker_exposure")

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
        price_decimal = decimal.Decimal(str(price))
        price_decimals = self.get_instrument_spec(symbol)["price_decimals"]
        price_quantizer = decimal.Decimal("1").scaleb(-price_decimals)

        rounded_ticks = (price_decimal / tick).to_integral_value(
            rounding=decimal.ROUND_HALF_UP
        )
        rounded_decimal = (rounded_ticks * tick).quantize(
            price_quantizer,
            rounding=decimal.ROUND_HALF_UP,
        )
        quantized_input = price_decimal.quantize(
            price_quantizer,
            rounding=decimal.ROUND_HALF_UP,
        )

        if symbol == "M6E" or rounded_decimal != quantized_input:
            logger.info(
                "TICK ROUNDING | "
                f"symbol={symbol} "
                f"raw_price={price} "
                f"tick_size={tick} "
                f"price_decimals={price_decimals} "
                f"rounded_price={rounded_decimal} "
                "rounding_method=decimal_nearest_tick_round_half_up"
            )

        return float(rounded_decimal)

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

    def get_containment_profile(self, stage=None):
        return CONTAINMENT_STAGE_PROFILES.get(stage or BOT_STAGE)

    def get_containment_max_size(self, symbol, stage=None):
        profile = self.get_containment_profile(stage)
        if profile is None:
            return None
        return profile.get("max_sizes", {}).get(symbol)

    def get_containment_max_notional(self, symbol, stage=None):
        profile = self.get_containment_profile(stage)
        if profile is None:
            return None
        return profile.get("max_notionals", {}).get(symbol)

    def get_notional_metadata(self, symbol):
        spec = self.get_instrument_spec(symbol)
        return spec.get("notional_model"), spec.get("notional_currency")

    def estimate_notional_exposure(self, symbol, size, reference_price):
        try:
            notional_model, notional_currency = self.get_notional_metadata(symbol)
            if not notional_model or not notional_currency:
                return None
            if size is None or reference_price is None:
                return None
            size_value = abs(float(size))
            reference_value = abs(float(reference_price))
            if size_value <= 0 or reference_value <= 0:
                return None
            return round(size_value * reference_value * self.get_point_value(symbol), 2)
        except Exception:
            return None

    def derive_notional_capped_size(self, symbol, reference_price, max_notional):
        single_unit_notional = self.estimate_notional_exposure(symbol, 1, reference_price)
        if single_unit_notional is None or single_unit_notional <= 0:
            return None, "invalid_single_unit_notional"
        if max_notional is None or max_notional <= 0:
            return None, "invalid_max_notional"

        raw_capped_size = max_notional / single_unit_notional
        constraints = self.get_size_constraints(symbol)

        if constraints["broker_type"] == "future":
            capped_size = self.normalize_position_size(symbol, raw_capped_size)
            size_valid, validation_reason = self.validate_position_size(symbol, capped_size)
            if not size_valid:
                return capped_size, validation_reason
            return capped_size, "size_valid"

        capped_decimal = decimal.Decimal(str(raw_capped_size)).to_integral_value(
            rounding=decimal.ROUND_DOWN
        )
        if capped_decimal <= 0:
            return None, "size_non_positive"
        return int(capped_decimal), "size_valid"

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

    def resolve_execution_containment(self, symbol, theoretical_size_before_containment, reference_price=None):
        containment_stage = BOT_STAGE
        containment_profile = self.get_containment_profile(containment_stage)
        containment_profile_name = None
        containment_exceeds_cap_action = None
        containment_max_size = None
        containment_max_notional = None
        notional_model, notional_currency = self.get_notional_metadata(symbol)

        if containment_profile is not None:
            containment_profile_name = containment_profile.get("profile")
            containment_exceeds_cap_action = containment_profile.get("exceeds_cap_action")
            containment_max_size = containment_profile.get("max_sizes", {}).get(symbol)
            containment_max_notional = containment_profile.get("max_notionals", {}).get(symbol)

        def containment_result(
            action,
            reason,
            approved_size,
            capped_size,
            triggering_rule,
            decision_source,
            denial_source=None,
        ):
            if action not in CONTAINMENT_ACTION_HIERARCHY:
                action = "denied"
                reason = f"invalid_containment_action:{reason}"
                approved_size = None
                decision_source = "stage_containment"
                denial_source = "stage_containment"

            if decision_source not in CONTAINMENT_DECISION_SOURCES:
                decision_source = "stage_containment"
                denial_source = "stage_containment"

            if action == "approve_as_is" and decision_source != "none":
                action = "denied"
                reason = f"invalid_approve_as_is_decision_source:{decision_source}"
                approved_size = None
                decision_source = "stage_containment"
                denial_source = "stage_containment"

            if action in {"capped", "denied"} and decision_source == "none":
                action = "denied"
                reason = f"invalid_containment_decision_source:{reason}"
                approved_size = None
                decision_source = "stage_containment"
                denial_source = "stage_containment"

            if action == "denied" and denial_source not in CONTAINMENT_DECISION_SOURCES:
                denial_source = decision_source

            if action == "denied" and denial_source == "none":
                denial_source = decision_source if decision_source != "none" else "stage_containment"

            estimated_notional_after_size_containment = self.estimate_notional_exposure(
                symbol,
                capped_size,
                reference_price,
            )
            return {
                "containment_status": "active",
                "containment_stage": containment_stage,
                "containment_profile": containment_profile_name,
                "containment_action": action,
                "containment_reason": reason,
                "containment_hierarchy": ">".join(CONTAINMENT_ACTION_HIERARCHY),
                "containment_decision_source": decision_source,
                "containment_denial_source": denial_source if action == "denied" else "none",
                "triggering_containment_rule": triggering_rule,
                "notional_model": notional_model,
                "notional_currency": notional_currency,
                "theoretical_size_before_containment": theoretical_size_before_containment,
                "capped_size_after_size_containment": capped_size,
                "approved_size_after_containment": approved_size,
                "containment_max_size": containment_max_size,
                "containment_max_notional": containment_max_notional,
                "estimated_notional_before_containment": self.estimate_notional_exposure(
                    symbol,
                    theoretical_size_before_containment,
                    reference_price,
                ),
                "estimated_notional_after_size_containment": estimated_notional_after_size_containment,
                "estimated_notional_after_containment": self.estimate_notional_exposure(
                    symbol,
                    approved_size,
                    reference_price,
                ),
            }

        if containment_profile is None or not containment_profile_name:
            return containment_result(
                "denied",
                "missing_or_invalid_containment_profile",
                None,
                None,
                "stage_profile",
                "stage_containment",
                "stage_containment",
            )

        if containment_max_size is None or containment_max_size <= 0:
            return containment_result(
                "denied",
                "missing_or_invalid_stage_containment_max_size",
                None,
                None,
                "instrument_size_cap",
                "stage_containment",
                "stage_containment",
            )

        if containment_max_notional is None or containment_max_notional <= 0:
            return containment_result(
                "denied",
                "missing_or_invalid_stage_containment_max_notional",
                None,
                None,
                "instrument_notional_cap",
                "stage_containment",
                "stage_containment",
            )

        if not notional_model or not notional_currency:
            return containment_result(
                "denied",
                "missing_or_invalid_notional_metadata",
                None,
                None,
                "instrument_notional_model",
                "stage_containment",
                "stage_containment",
            )

        if theoretical_size_before_containment is None:
            return containment_result(
                "denied",
                "missing_theoretical_size_before_containment",
                None,
                None,
                "theoretical_size",
                "size_containment",
                "size_containment",
            )

        if theoretical_size_before_containment <= 0:
            return containment_result(
                "denied",
                "non_positive_theoretical_size_before_containment",
                None,
                None,
                "theoretical_size",
                "size_containment",
                "size_containment",
            )

        if theoretical_size_before_containment <= containment_max_size:
            approved_size_after_containment = theoretical_size_before_containment
            capped_size_after_size_containment = theoretical_size_before_containment
            containment_action = "approve_as_is"
            containment_reason = "within_instrument_cap"
            triggering_containment_rule = "none"
        else:
            if containment_exceeds_cap_action == "deny":
                return containment_result(
                    "denied",
                    "exceeds_prd_containment_cap",
                    None,
                    None,
                    "instrument_size_cap",
                    "stage_containment",
                    "stage_containment",
                )

            if containment_exceeds_cap_action != "cap":
                return containment_result(
                    "denied",
                    "invalid_containment_exceeds_cap_action",
                    None,
                    None,
                    "stage_profile",
                    "stage_containment",
                    "stage_containment",
                )

            approved_size_after_containment = containment_max_size
            capped_size_after_size_containment = containment_max_size
            containment_action = "capped"
            containment_reason = "exceeds_instrument_cap"
            triggering_containment_rule = "instrument_size_cap"

        if (
            approved_size_after_containment is None or
            approved_size_after_containment <= 0
        ):
            return containment_result(
                "denied",
                "capped_size_invalid:size_non_positive",
                approved_size_after_containment,
                capped_size_after_size_containment,
                "instrument_size_cap",
                "size_containment",
                "size_containment",
            )

        size_constraints = self.get_size_constraints(symbol)
        if size_constraints["broker_type"] == "future":
            size_valid, validation_reason = self.validate_position_size(
                symbol,
                approved_size_after_containment,
            )
            if not size_valid:
                return containment_result(
                    "denied",
                    f"capped_size_invalid:{validation_reason or 'size_invalid'}",
                    approved_size_after_containment,
                    capped_size_after_size_containment,
                    "instrument_size_cap",
                    "size_containment",
                    "size_containment",
                )

        estimated_notional_after_containment = self.estimate_notional_exposure(
            symbol,
            approved_size_after_containment,
            reference_price,
        )

        if estimated_notional_after_containment is None:
            return containment_result(
                "denied",
                "estimated_notional_unavailable",
                approved_size_after_containment,
                capped_size_after_size_containment,
                "instrument_notional_cap",
                "exposure_containment",
                "exposure_containment",
            )

        if estimated_notional_after_containment <= containment_max_notional:
            return containment_result(
                containment_action,
                containment_reason,
                approved_size_after_containment,
                capped_size_after_size_containment,
                triggering_containment_rule,
                "none" if containment_action == "approve_as_is" else "size_containment",
            )

        notional_capped_size, notional_cap_reason = self.derive_notional_capped_size(
            symbol,
            reference_price,
            containment_max_notional,
        )
        if (
            notional_capped_size is None or
            notional_capped_size <= 0 or
            notional_capped_size >= approved_size_after_containment
        ):
            return containment_result(
                "denied",
                f"notional_cap_unable_to_reduce:{notional_cap_reason}",
                None,
                capped_size_after_size_containment,
                "instrument_notional_cap",
                "exposure_containment",
                "exposure_containment",
            )

        if size_constraints["broker_type"] == "future":
            size_valid, validation_reason = self.validate_position_size(symbol, notional_capped_size)
            if not size_valid:
                return containment_result(
                    "denied",
                    f"notional_capped_size_invalid:{validation_reason or 'size_invalid'}",
                    notional_capped_size,
                    capped_size_after_size_containment,
                    "instrument_notional_cap",
                    "exposure_containment",
                    "exposure_containment",
                )

        estimated_notional_after_notional_cap = self.estimate_notional_exposure(
            symbol,
            notional_capped_size,
            reference_price,
        )
        if (
            estimated_notional_after_notional_cap is None or
            estimated_notional_after_notional_cap > containment_max_notional
        ):
            return containment_result(
                "denied",
                "notional_capped_size_still_exceeds_cap",
                notional_capped_size,
                capped_size_after_size_containment,
                "instrument_notional_cap",
                "exposure_containment",
                "exposure_containment",
            )

        return containment_result(
            "capped",
            "exceeds_instrument_notional_cap",
            notional_capped_size,
            capped_size_after_size_containment,
            "instrument_notional_cap",
            "exposure_containment",
        )

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

    def build_tick_based_stop_distance_comparison(self, symbol, entry_price, stop_price, min_stop_distance):
        try:
            tick = decimal.Decimal(str(self.get_tick_size(symbol)))
            entry_decimal = decimal.Decimal(str(entry_price))
            stop_decimal = decimal.Decimal(str(stop_price))
            min_stop_decimal = decimal.Decimal(str(min_stop_distance))
        except Exception as exc:
            return {
                "ok": False,
                "reason": f"invalid_decimal_stop_distance_inputs:{exc}",
                "comparison_basis": "tick_based_decimal",
                "final_stop_distance_ticks": None,
                "min_stop_distance_ticks": None,
            }

        if tick <= 0:
            return {
                "ok": False,
                "reason": "invalid_tick_size",
                "comparison_basis": "tick_based_decimal",
                "final_stop_distance_ticks": None,
                "min_stop_distance_ticks": None,
            }
        if min_stop_decimal <= 0:
            return {
                "ok": False,
                "reason": "invalid_min_stop_distance",
                "comparison_basis": "tick_based_decimal",
                "final_stop_distance_ticks": None,
                "min_stop_distance_ticks": None,
            }

        distance_decimal = abs(stop_decimal - entry_decimal)
        if distance_decimal <= 0:
            return {
                "ok": False,
                "reason": "invalid_final_stop_distance",
                "comparison_basis": "tick_based_decimal",
                "final_stop_distance_ticks": None,
                "min_stop_distance_ticks": None,
            }

        distance_tick_ratio = distance_decimal / tick
        distance_ticks = distance_tick_ratio.to_integral_value(rounding=decimal.ROUND_HALF_UP)
        if distance_tick_ratio != distance_ticks:
            return {
                "ok": False,
                "reason": "final_stop_distance_tick_misaligned",
                "comparison_basis": "tick_based_decimal",
                "final_stop_distance_ticks": float(distance_tick_ratio),
                "min_stop_distance_ticks": None,
            }

        min_stop_tick_ratio = min_stop_decimal / tick
        min_stop_ticks = min_stop_tick_ratio.to_integral_value(rounding=decimal.ROUND_CEILING)

        return {
            "ok": distance_ticks >= min_stop_ticks,
            "reason": (
                "final_stop_distance_valid"
                if distance_ticks >= min_stop_ticks
                else "final_stop_distance_below_min_stop_distance"
            ),
            "comparison_basis": "tick_based_decimal",
            "final_stop_distance_ticks": int(distance_ticks),
            "min_stop_distance_ticks": int(min_stop_ticks),
        }

    def validate_final_stop_distance(self, symbol, entry_price, stop_price):
        final_stop_distance = self.calculate_stop_distance_points(entry_price, stop_price)
        min_stop_distance = self.get_min_stop_distance(symbol)

        if min_stop_distance is None:
            return {
                "ok": True,
                "final_stop_distance": final_stop_distance,
                "min_stop_distance": min_stop_distance,
                "reason": "no_min_stop_distance_configured",
                "stop_distance_comparison_basis": "none",
                "final_stop_distance_ticks": None,
                "min_stop_distance_ticks": None,
            }

        if final_stop_distance is None:
            return {
                "ok": False,
                "final_stop_distance": final_stop_distance,
                "min_stop_distance": min_stop_distance,
                "reason": "invalid_final_stop_distance",
                "stop_distance_comparison_basis": "tick_based_decimal",
                "final_stop_distance_ticks": None,
                "min_stop_distance_ticks": None,
            }

        tick_comparison = self.build_tick_based_stop_distance_comparison(
            symbol,
            entry_price,
            stop_price,
            min_stop_distance,
        )

        return {
            "ok": tick_comparison["ok"],
            "final_stop_distance": final_stop_distance,
            "min_stop_distance": min_stop_distance,
            "reason": tick_comparison["reason"],
            "stop_distance_comparison_basis": tick_comparison["comparison_basis"],
            "final_stop_distance_ticks": tick_comparison["final_stop_distance_ticks"],
            "min_stop_distance_ticks": tick_comparison["min_stop_distance_ticks"],
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

    def get_reconnect_fill_reconstruction_since(self, reference_time=None):
        since_time = self.connectivity_reconstruction_anchor_since or reference_time
        if since_time is None:
            since_time = datetime.now(timezone.utc)
        elif isinstance(since_time, datetime):
            if since_time.tzinfo is None:
                since_time = since_time.replace(tzinfo=timezone.utc)
            else:
                since_time = since_time.astimezone(timezone.utc)
        else:
            since_time = datetime.now(timezone.utc)

        return since_time - timedelta(seconds=RECONNECT_FILL_RECONSTRUCTION_LOOKBACK_SECONDS)

    def get_reconnect_unmatched_fill_dedupe_key(self, fill, execution, diagnostic_reason):
        effective_fill_time, _, _ = self.get_effective_execution_time(fill, execution)
        return (
            f"{diagnostic_reason}:"
            f"{self.get_execution_identity(execution, effective_fill_time)}"
        )

    def should_log_reconnect_unmatched_fill_diagnostic(self, fill, execution, diagnostic_reason):
        dedupe_key = self.get_reconnect_unmatched_fill_dedupe_key(
            fill,
            execution,
            diagnostic_reason,
        )
        with self.trade_analysis_lock:
            if dedupe_key in self.reconnect_unmatched_fill_dedupe:
                return False

            self.reconnect_unmatched_fill_dedupe[dedupe_key] = time.time()
            while len(self.reconnect_unmatched_fill_dedupe) > 500:
                oldest_key = next(iter(self.reconnect_unmatched_fill_dedupe))
                self.reconnect_unmatched_fill_dedupe.pop(oldest_key, None)

        return True

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
                "PRD DAILY SL STOP RESET | "
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
        day_key = self.refresh_live_daily_stop_state("PRD_SL_TRIGGER", exit_time)

        if not self.live_daily_stop_active:
            self.live_daily_stop_active = True
            self.live_daily_stop_trigger_trade_id = trade_id
            logger.warning(
                "PRD DAILY SL STOP ACTIVATED | "
                f"day_key={day_key} "
                f"trigger_trade_id={trade_id}"
            )
        else:
            logger.warning(
                "PRD DAILY SL STOP ALREADY ACTIVE | "
                f"day_key={day_key} "
                f"existing_trigger_trade_id={self.live_daily_stop_trigger_trade_id} "
                f"incoming_trade_id={trade_id}"
            )

    def evaluate_live_execution_risk_regime(self, job):
        stage = BOT_STAGE

        if stage != "PRD":
            return {
                "stage": stage,
                "risk_branch": "non_prd_regime",
                "execution_allowed": True,
                "reason": "prd_daily_sl_stop_not_applicable",
            }

        day_key = self.refresh_live_daily_stop_state("PRD_EXECUTION_GATE")

        if self.live_daily_stop_active:
            reason = (
                "PRD execution denied because a realized SL already activated the daily stop "
                f"for Amsterdam day {day_key}"
            )
            logger.warning(
                "PRD DAILY SL STOP DENIAL | "
                f"day_key={day_key} "
                f"trigger_trade_id={self.live_daily_stop_trigger_trade_id} "
                f"incoming_symbol={job['symbol']} "
                f"reason={reason}"
            )
            return {
                "stage": stage,
                "risk_branch": "prd_daily_sl_stop_active",
                "execution_allowed": False,
                "day_key": day_key,
                "trigger_trade_id": self.live_daily_stop_trigger_trade_id,
                "reason": reason,
            }

        logger.info(
            "PRD RISK REGIME | "
            f"stage={stage} "
            f"day_key={day_key} "
            "execution_allowed=true "
            "reason=no_realized_sl_day_stop"
        )
        return {
            "stage": stage,
            "risk_branch": "prd_daily_sl_stop_clear",
            "execution_allowed": True,
            "day_key": day_key,
            "trigger_trade_id": self.live_daily_stop_trigger_trade_id,
            "reason": "no_realized_sl_day_stop",
        }

    def refresh_paper_daily_stop_state(self, reason_label, reference_time=None):
        day_key = self.get_amsterdam_day_key(reference_time)

        if self.paper_daily_stop_day_key is None:
            self.paper_daily_stop_day_key = day_key
            return day_key

        if self.paper_daily_stop_day_key != day_key:
            logger.info(
                "ACC SYMBOL DAILY SL STOP RESET | "
                f"old_day_key={self.paper_daily_stop_day_key} "
                f"new_day_key={day_key} "
                f"previous_symbol_stops={self.paper_symbol_daily_sl_stops} "
                f"reason={reason_label}"
            )
            self.paper_daily_stop_day_key = day_key
            self.paper_symbol_daily_sl_stops.clear()

        return day_key

    def activate_paper_symbol_daily_sl_stop(self, symbol, trade_id, exit_time=None):
        day_key = self.refresh_paper_daily_stop_state("ACC_SYMBOL_SL_TRIGGER", exit_time)
        stop = self.paper_symbol_daily_sl_stops.get(symbol)
        triggered_at = exit_time
        if not isinstance(triggered_at, datetime):
            triggered_at = datetime.now(timezone.utc)
        elif triggered_at.tzinfo is None:
            triggered_at = triggered_at.replace(tzinfo=timezone.utc)
        else:
            triggered_at = triggered_at.astimezone(timezone.utc)

        if not stop or not stop.get("active"):
            self.paper_symbol_daily_sl_stops[symbol] = {
                "active": True,
                "trigger_trade_id": trade_id,
                "day_key": day_key,
                "triggered_at": triggered_at,
            }
            logger.warning(
                "ACC SYMBOL DAILY SL STOP ACTIVATED | "
                f"stage={BOT_STAGE} "
                f"day_key={day_key} "
                f"symbol={symbol} "
                f"trigger_trade_id={trade_id} "
                f"triggered_at={self.to_iso(triggered_at)}"
            )
        else:
            logger.warning(
                "ACC SYMBOL DAILY SL STOP ALREADY ACTIVE | "
                f"stage={BOT_STAGE} "
                f"day_key={day_key} "
                f"symbol={symbol} "
                f"trigger_trade_id={trade_id} "
                f"existing_trigger_trade_id={stop.get('trigger_trade_id')} "
                f"triggered_at={self.to_iso(stop.get('triggered_at'))}"
            )

    def evaluate_paper_execution_risk_regime(self, job):
        stage = BOT_STAGE

        if stage != "ACC":
            return {
                "stage": stage,
                "risk_branch": "non_acc_regime",
                "execution_allowed": True,
                "reason": "acc_symbol_daily_sl_stop_not_applicable",
            }

        day_key = self.refresh_paper_daily_stop_state("ACC_EXECUTION_GATE")
        incoming_symbol = job["symbol"]
        stop = self.paper_symbol_daily_sl_stops.get(incoming_symbol)

        if stop and stop.get("active") and stop.get("day_key") == day_key:
            reason = "acc_symbol_daily_sl_stop_active"
            logger.warning(
                "ACC SYMBOL DAILY SL STOP DENIAL | "
                f"stage={stage} "
                f"day_key={day_key} "
                f"incoming_symbol={incoming_symbol} "
                f"trigger_trade_id={stop.get('trigger_trade_id')} "
                f"reason={reason}"
            )
            return {
                "stage": stage,
                "risk_branch": "acc_symbol_daily_sl_stop_active",
                "execution_allowed": False,
                "day_key": day_key,
                "trigger_trade_id": stop.get("trigger_trade_id"),
                "reason": reason,
            }

        logger.info(
            "ACC SYMBOL DAILY SL STOP CLEAR | "
            f"stage={stage} "
            f"day_key={day_key} "
            f"incoming_symbol={incoming_symbol} "
            "trigger_trade_id=None "
            "reason=acc_symbol_daily_sl_stop_clear"
        )
        return {
            "stage": stage,
            "risk_branch": "acc_symbol_daily_sl_stop_clear",
            "execution_allowed": True,
            "day_key": day_key,
            "trigger_trade_id": None,
            "reason": "acc_symbol_daily_sl_stop_clear",
        }

    def to_iso(self, value):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.isoformat()
        return value

    # ==========================================================
    # SESSION CLOSE / EOD FLATTEN POLICY
    # ==========================================================

    def log_session_close_config_loaded_once(self):
        if self.session_close_config_logged:
            return
        self.session_close_config_logged = True
        logger.warning(
            "SESSION_CLOSE_CONFIG_LOADED | "
            f"enabled={SESSION_CLOSE_ENABLED} "
            f"no_new_entries_before_close_min={NO_NEW_ENTRIES_BEFORE_CLOSE_MIN} "
            f"eod_flatten_enabled={EOD_FLATTEN_ENABLED} "
            f"eod_flatten_before_close_min={EOD_FLATTEN_BEFORE_CLOSE_MIN} "
            f"eod_flatten_order_type={EOD_FLATTEN_ORDER_TYPE} "
            f"eod_flatten_max_attempts={EOD_FLATTEN_MAX_ATTEMPTS} "
            f"eod_flatten_recheck_seconds={EOD_FLATTEN_RECHECK_SECONDS} "
            f"eod_flatten_scope={EOD_FLATTEN_SCOPE} "
            f"account_wide_flatten={ACCOUNT_WIDE_FLATTEN} "
            f"force_block_new_entries={SESSION_CLOSE_FORCE_BLOCK_NEW_ENTRIES} "
            f"cancel_working_entries={SESSION_CLOSE_CANCEL_WORKING_ENTRIES} "
            f"session_flat_by_times={json.dumps(SESSION_FLAT_BY_TIMES, sort_keys=True)}"
        )

    def get_session_close_scope_symbols(self):
        if ACCOUNT_WIDE_FLATTEN or EOD_FLATTEN_SCOPE == "ACCOUNT_WIDE":
            return [
                symbol for symbol in INSTRUMENT_SPECS.keys()
                if symbol not in RUNTIME_DISABLED_SYMBOLS
            ]
        return [
            symbol for symbol in SESSION_FLAT_BY_TIMES.keys()
            if symbol in INSTRUMENT_SPECS and symbol not in RUNTIME_DISABLED_SYMBOLS
        ]

    def get_configured_flat_by_datetime(self, symbol, reference_time=None):
        config = SESSION_FLAT_BY_TIMES.get(symbol)
        if not config:
            return None, None
        try:
            configured_tz = ZoneInfo(config["timezone"])
            if reference_time is None:
                local_now = datetime.now(configured_tz)
            elif isinstance(reference_time, datetime):
                local_now = (
                    reference_time.astimezone(configured_tz)
                    if reference_time.tzinfo
                    else reference_time.replace(tzinfo=timezone.utc).astimezone(configured_tz)
                )
            else:
                local_now = datetime.now(configured_tz)

            hour_text, minute_text = str(config["flat_by"]).split(":", 1)
            flat_by = local_now.replace(
                hour=int(hour_text),
                minute=int(minute_text),
                second=0,
                microsecond=0,
            )
            return flat_by, config["timezone"]
        except Exception as exc:
            logger.exception(
                "SESSION_CLOSE_CONFIG_INVALID | "
                f"symbol={symbol} config={config} reason={exc}"
            )
            return None, config.get("timezone") if isinstance(config, dict) else None

    def get_session_close_state(self, symbol, reference_time=None):
        self.log_session_close_config_loaded_once()
        flat_by, timezone_name = self.get_configured_flat_by_datetime(symbol, reference_time)
        if not SESSION_CLOSE_ENABLED or flat_by is None:
            return {
                "enabled": False,
                "symbol": symbol,
                "timezone": timezone_name,
                "configured_flat_by_time": self.to_iso(flat_by),
                "minutes_to_close": None,
                "no_entry_active": False,
                "flatten_window_active": False,
                "hard_close_active": False,
                "reason": "session_close_disabled_or_unconfigured",
            }

        now_value = reference_time if isinstance(reference_time, datetime) else datetime.now(timezone.utc)
        if now_value.tzinfo is None:
            now_value = now_value.replace(tzinfo=timezone.utc)
        now_local = now_value.astimezone(flat_by.tzinfo)
        minutes_to_close = round((flat_by - now_local).total_seconds() / 60.0, 3)
        hard_close_active = minutes_to_close <= 0
        no_entry_active = (
            SESSION_CLOSE_FORCE_BLOCK_NEW_ENTRIES
            and minutes_to_close <= NO_NEW_ENTRIES_BEFORE_CLOSE_MIN
        )
        flatten_window_active = (
            EOD_FLATTEN_ENABLED
            and minutes_to_close <= EOD_FLATTEN_BEFORE_CLOSE_MIN
        )
        reason = "session_close_clear"
        if hard_close_active:
            reason = "session_flat_by_time_reached"
        elif flatten_window_active:
            reason = "session_flatten_window_active"
        elif no_entry_active:
            reason = "session_close_entry_cutoff"

        return {
            "enabled": True,
            "symbol": symbol,
            "timezone": timezone_name,
            "configured_flat_by_time": self.to_iso(flat_by),
            "minutes_to_close": minutes_to_close,
            "no_entry_active": no_entry_active,
            "flatten_window_active": flatten_window_active,
            "hard_close_active": hard_close_active,
            "reason": reason,
        }

    def should_block_new_entry_for_session_close(self, symbol, reference_time=None):
        if not symbol or self.is_runtime_symbol_disabled(symbol):
            return False, self.get_session_close_state(symbol, reference_time)
        state = self.get_session_close_state(symbol, reference_time)
        return bool(state.get("enabled") and state.get("no_entry_active")), state

    def log_session_entry_blocked(self, symbol, side=None, job=None, normalized=None, location=None, state=None):
        if state is None:
            state = self.get_session_close_state(symbol)
        logger.warning(
            "SESSION_ENTRY_BLOCKED | "
            f"symbol={symbol} "
            f"side={side or (job or {}).get('side') or (normalized or {}).get('side')} "
            "reason=session_close_entry_cutoff "
            f"minutes_to_close={state.get('minutes_to_close')} "
            f"cutoff_minutes={NO_NEW_ENTRIES_BEFORE_CLOSE_MIN} "
            f"configured_flat_by_time={state.get('configured_flat_by_time')} "
            f"timezone={state.get('timezone')} "
            f"stage={BOT_STAGE} "
            f"job_id={(job or {}).get('job_id')} "
            f"job_type={(job or {}).get('job_type')} "
            f"signal_id={(job or {}).get('signal_id') or (normalized or {}).get('signal_id')} "
            f"location={location} "
            "blocked_by=session_close_policy"
        )

    def is_session_close_system_job(self, job):
        return isinstance(job, dict) and job.get("job_type") in SESSION_CLOSE_SYSTEM_JOB_TYPES

    def is_broker_system_job(self, job):
        return isinstance(job, dict) and job.get("job_type") in BROKER_SYSTEM_JOB_TYPES

    def enqueue_broker_system_job(self, job_type, reason_label, symbol=None, force=False, **fields):
        if job_type not in BROKER_SYSTEM_JOB_TYPES:
            logger.error(
                "BROKER_SYSTEM_JOB_REJECTED | "
                f"job_type={job_type} "
                f"symbol={symbol} "
                f"reason={reason_label} "
                "decision=unknown_system_job_type"
            )
            return False

        if (
            job_type == "RECONNECT_RECOVERY"
            and self.is_unresolved_contract_qualification_failure()
        ):
            logger.critical(
                "BROKER_SYSTEM_JOB_SUPPRESSED | "
                f"job_type={job_type} "
                f"symbol={symbol} "
                f"reason={reason_label} "
                f"session_initialization_failure_reason={self.session_initialization_failure_reason} "
                "decision=suppress_reconnect_recovery_until_initialization_failure_cleared"
            )
            return False

        dedupe_key = f"{job_type}:{symbol or 'ALL'}:{reason_label}"
        now_value = time.time()
        last_queued = self.broker_system_job_last_queued.get(dedupe_key)
        if not force and last_queued is not None and now_value - last_queued < SESSION_CLOSE_SYSTEM_JOB_DEDUPE_SECONDS:
            return False

        self.broker_system_job_last_queued[dedupe_key] = now_value
        job = {
            "job_type": job_type,
            "symbol": symbol,
            "reason_label": reason_label,
            "queue_put_time": datetime.now(timezone.utc),
            "enqueue_time": datetime.now(timezone.utc),
            "internal_system_job": True,
        }
        job.update(fields)
        self.execution_queue.put(job)
        logger.warning(
            "BROKER_SYSTEM_JOB_QUEUED | "
            f"job_type={job_type} "
            f"symbol={symbol} "
            f"reason={reason_label} "
            f"queue_size_after_put={self.execution_queue.qsize()}"
        )
        return True

    def enqueue_session_close_system_job(self, reason_label, symbol=None, force=False):
        if not SESSION_CLOSE_ENABLED:
            return False
        dedupe_key = f"{symbol or 'ALL'}:{reason_label}"
        now_value = time.time()
        last_queued = self.session_close_last_system_job_queued.get(dedupe_key)
        if not force and last_queued is not None and now_value - last_queued < SESSION_CLOSE_SYSTEM_JOB_DEDUPE_SECONDS:
            return False

        self.session_close_last_system_job_queued[dedupe_key] = now_value
        self.execution_queue.put({
            "job_type": "SESSION_CLOSE_SWEEP",
            "symbol": symbol,
            "reason_label": reason_label,
            "queue_put_time": datetime.now(timezone.utc),
            "enqueue_time": datetime.now(timezone.utc),
        })
        logger.warning(
            "SESSION_CLOSE_SYSTEM_JOB_QUEUED | "
            f"job_type=SESSION_CLOSE_SWEEP "
            f"symbol={symbol} "
            f"reason={reason_label} "
            f"queue_size_after_put={self.execution_queue.qsize()}"
        )
        return True

    def process_broker_system_job(self, job):
        job_type = job.get("job_type")
        reason_label = job.get("reason_label") or job_type or "broker_system_job"

        if job_type in SESSION_CLOSE_SYSTEM_JOB_TYPES:
            return self.run_session_close_sweep(
                reason_label,
                target_symbol=job.get("symbol"),
            )

        if job_type == "RECONNECT_RECOVERY":
            return self.run_reconnect_recovery_job(job)

        if job_type == "BROKER_REALITY_RECONCILIATION":
            return self.run_broker_reality_reconciliation_job(job)

        logger.error(
            "BROKER_SYSTEM_JOB_UNKNOWN | "
            f"job_type={job_type} "
            f"reason={reason_label}"
        )
        return 0

    def run_broker_reality_reconciliation_job(self, job):
        reason_label = job.get("reason_label") or "broker_reality_reconciliation"
        logger.warning(
            "BROKER_REALITY_RECONCILIATION_STARTED | "
            f"reason={reason_label} "
            f"symbol={job.get('symbol')}"
        )
        reconciled = self.reconcile_active_trade_lifecycle_from_broker_fills(
            reason_label
        )
        if job.get("check_flat_positions"):
            try:
                positions = self.broker_read_positions(
                    caller="broker_reality_reconciliation",
                    reason_label=f"{reason_label}:flat_position_check",
                    symbol=job.get("symbol"),
                    trade_analysis_lock_context="not_locked",
                    failure_log_level="info",
                )
                if not any(position.position != 0 for position in positions):
                    self.trade_state = "IDLE"
                    logger.info("→ IDLE")
            except Exception as exc:
                logger.warning(
                    "BROKER_REALITY_RECONCILIATION_FLAT_CHECK_FAILED | "
                    f"reason={reason_label} "
                    f"failure={exc}"
                )
        if job.get("include_active_candidate_check"):
            self.get_active_trade_candidates(f"{reason_label}:active_candidate_check")
        if job.get("connectivity_restore_complete"):
            self.connectivity_uncertain = False
            self.connectivity_reconciliation_required = False
            self.connectivity_reconciliation_completed_at = datetime.now(timezone.utc)
            self.connectivity_reconstruction_anchor_since = None
        logger.warning(
            "BROKER_REALITY_RECONCILIATION_COMPLETE | "
            f"reason={reason_label} "
            f"reconciled={reconciled} "
            f"connectivity_restore_complete={job.get('connectivity_restore_complete')}"
        )
        return reconciled

    def run_reconnect_recovery_job(self, job):
        reason_label = job.get("reason_label") or "reconnect_recovery"
        logger.warning(
            "RECONNECT_RECOVERY_JOB_STARTED | "
            f"reason={reason_label} "
            f"reconnect_count={self.session_reconnect_count}"
        )
        self.update_session_health()

        try:
            if not self.session_socket_connected:
                self.connect_ib()
                self.post_reconnect_fill_reconstruction_sweep(
                    f"{reason_label}:post_connect_reconstruction",
                    since_time=job.get("since_time") or self.get_reconnect_fill_reconstruction_since(),
                )
            elif not self.session_healthy:
                self.force_session_recovery(reason_label)
            elif job.get("force_reconciliation"):
                self.post_reconnect_fill_reconstruction_sweep(
                    f"{reason_label}:forced_reconciliation",
                    since_time=job.get("since_time") or self.get_reconnect_fill_reconstruction_since(),
                )
        except Exception:
            logger.exception(
                "RECONNECT_RECOVERY_JOB_FAILED | "
                f"reason={reason_label}"
            )
            self.update_session_health()
            return 0

        self.update_session_health()
        self.log_session_health(f"RECONNECT_RECOVERY_JOB_COMPLETE_{reason_label}")
        logger.warning(
            "RECONNECT_RECOVERY_JOB_COMPLETE | "
            f"reason={reason_label} "
            f"socket_connected={self.session_socket_connected} "
            f"session_initialized={self.session_initialized} "
            f"session_healthy={self.session_healthy}"
        )
        return 1

    def session_close_monitor(self):
        self.log_session_close_config_loaded_once()
        while True:
            try:
                for symbol in self.get_session_close_scope_symbols():
                    block_active, state = self.should_block_new_entry_for_session_close(symbol)
                    if block_active:
                        logger.warning(
                            "SESSION_ENTRY_CUTOFF_ACTIVE | "
                            f"symbol={symbol} "
                            f"minutes_to_close={state.get('minutes_to_close')} "
                            f"cutoff_minutes={NO_NEW_ENTRIES_BEFORE_CLOSE_MIN} "
                            f"configured_flat_by_time={state.get('configured_flat_by_time')} "
                            f"timezone={state.get('timezone')} "
                            f"reason={state.get('reason')} "
                            f"stage={BOT_STAGE}"
                        )
                        self.enqueue_session_close_system_job(
                            state.get("reason") or "session_close_monitor",
                            symbol=symbol,
                        )
                    if state.get("hard_close_active"):
                        logger.critical(
                            "SESSION_CLOSE_HARD_FLATTEN_REQUIRED | "
                            f"symbol={symbol} "
                            f"minutes_to_close={state.get('minutes_to_close')} "
                            f"configured_flat_by_time={state.get('configured_flat_by_time')} "
                            f"timezone={state.get('timezone')} "
                            "operator_action_required=False "
                            "reason=session_flat_by_time_reached"
                        )
                        self.enqueue_session_close_system_job(
                            "session_close_hard_flatten_required",
                            symbol=symbol,
                        )
                time.sleep(SESSION_CLOSE_MONITOR_POLL_SECONDS)
            except Exception:
                logger.exception("SESSION CLOSE MONITOR FAILED")
                time.sleep(SESSION_CLOSE_MONITOR_POLL_SECONDS)

    # P180/P181/P182: observability-only timing helpers for broker reads and webhook processing.
    def current_monotonic_ms(self):
        return time.perf_counter() * 1000.0

    def elapsed_ms(self, start_ms):
        try:
            return round(self.current_monotonic_ms() - start_ms, 3)
        except Exception:
            return None

    def classify_broker_read_context(self, caller=None):
        try:
            thread_name = str(threading.current_thread().name or "").lower()
            caller_text = str(caller or "").lower()

            if "execution_worker" in caller_text or "execution_worker" in thread_name:
                return "execution_worker"
            if "ib_watchdog" in caller_text or "ib_watchdog" in thread_name:
                return "ib_watchdog"
            if caller_text in {"on_exec", "on_open_order", "on_order_status", "on_commission", "on_error"}:
                return "ib_event_callback"
            if "threadpool" in thread_name or "anyio" in thread_name or "uvicorn" in thread_name:
                return "webhook_or_api_thread"
        except Exception:
            return "unknown"
        return "unknown"

    def classify_broker_read_owner(self, caller=None):
        caller_text = str(caller or "").lower()

        if caller_text in {
            "place_bracket_order",
            "protective_emergency_flatten",
            "execution_worker",
            "session_close_sweep",
            "session_close_flatten",
            "session_close_cancel_working_entries",
            "session_close_cancel_stale_orders",
            "process_broker_system_job",
            "broker_reality_reconciliation",
            "reconnect_recovery",
            "startup_reconciliation",
        }:
            return "execution_worker_owned_candidate"
        if caller_text in {"on_exec", "on_open_order", "on_order_status", "on_commission", "on_error"}:
            return "callback_owned_current"
        if caller_text in {
            "ib_watchdog",
            "connect_ib",
            "force_session_recovery",
            "update_session_health",
            "qualify_contracts",
            "ensure_symbol_contract_ready",
            "update_session_health",
        }:
            return "watchdog_or_session_recovery"
        if caller_text in {"handle_webhook_signal", "webhook_handler"}:
            return "webhook_or_api_adjacent"
        if caller_text in {
            "get_trade_broker_reality",
            "build_in_session_broker_fill_reconciliation_candidate",
            "post_reconnect_fill_reconstruction_sweep",
            "cancel_unacknowledged_bracket_legs",
            "assess_broker_bracket_confirmation",
            "cancel_partial_entry_remainder",
            "log_exit_cleanup_visibility",
            "get_timeout_retained_child_snapshot",
            "cancel_timeout_retained_child_orders",
            "place_timeout_retained_replacement_protection",
            "reconcile_timeout_retained_child_protection",
            "get_partial_timeout_parent_finality_snapshot",
            "post_reconnect_fill_reconstruction_sweep",
        }:
            return "lifecycle_recovery_current"

        return "unknown"

    def classify_broker_write_owner(self, caller=None):
        return self.classify_broker_read_owner(caller)

    def get_broker_read_owner_note(self, owner_class):
        notes = {
            "execution_worker_owned_candidate": "candidate_for_future_single_owner_broker_io",
            "callback_owned_current": "callback_path_currently_reads_broker_state",
            "watchdog_or_session_recovery": "session_health_or_recovery_broker_context",
            "webhook_or_api_adjacent": "api_adjacent_broker_read_should_remain_suspect",
            "lifecycle_recovery_current": "lifecycle_recovery_currently_reads_broker_state",
            "unknown": "broker_read_owner_unknown",
        }
        return notes.get(owner_class, "broker_read_owner_unknown")

    def classify_broker_read_lock_risk(self, trade_analysis_lock_context):
        if trade_analysis_lock_context == "maybe_locked":
            return "medium"
        if trade_analysis_lock_context == "not_locked":
            return "low"
        if trade_analysis_lock_context == "not_tracked":
            return "unknown"
        return "unknown"

    # P184/P197: broker I/O owner audit and single-owner enforcement.
    def register_broker_io_owner_candidate(self, owner_label):
        try:
            self.broker_io_owner_expected = owner_label or self.broker_io_owner_expected
            self.broker_io_owner_thread_ident = threading.get_ident()
            self.broker_io_owner_thread_name = threading.current_thread().name
            logger.info(
                "BROKER IO OWNER CANDIDATE REGISTERED | "
                f"broker_io_owner_mode={self.broker_io_owner_mode} "
                f"broker_io_owner_expected={self.broker_io_owner_expected} "
                f"broker_io_owner_thread_name={self.broker_io_owner_thread_name} "
                f"broker_io_owner_thread_ident={self.broker_io_owner_thread_ident} "
                f"broker_io_owner_action={self.get_broker_io_owner_action()}"
            )
        except Exception:
            try:
                logger.exception("BROKER IO OWNER CANDIDATE REGISTRATION LOG FAILED")
            except Exception:
                pass

    def get_broker_io_owner_thread_ident(self):
        return getattr(self, "broker_io_owner_thread_ident", None)

    def is_current_broker_io_owner_context(self, caller=None):
        expected_ident = self.get_broker_io_owner_thread_ident()
        if expected_ident is None:
            return False
        return threading.get_ident() == expected_ident

    def is_broker_io_enforcement_active(self):
        return getattr(self, "broker_io_owner_mode", None) == "enforce_single_owner"

    def get_broker_io_owner_action(self):
        return "enforced" if self.is_broker_io_enforcement_active() else "audit_only"

    def classify_broker_io_owner_violation(self, caller=None):
        owner_class = self.classify_broker_read_owner(caller)
        expected_ident = self.get_broker_io_owner_thread_ident()

        if expected_ident is None:
            return "owner_candidate_not_registered"
        if self.is_current_broker_io_owner_context(caller):
            return "none"
        if owner_class == "execution_worker_owned_candidate":
            return "execution_worker_owned_candidate_thread_mismatch"
        if owner_class == "callback_owned_current":
            return "callback_non_owner_current"
        if owner_class == "watchdog_or_session_recovery":
            return "watchdog_or_session_recovery_non_owner_current"
        if owner_class == "webhook_or_api_adjacent":
            return "webhook_or_api_non_owner_current"
        if owner_class == "lifecycle_recovery_current":
            return "lifecycle_recovery_non_owner_current"
        return "unknown_non_owner_context"

    def build_broker_io_owner_audit_fields(self, caller=None):
        owner_ident = self.get_broker_io_owner_thread_ident()
        owner_name = getattr(self, "broker_io_owner_thread_name", None)
        current_ident = threading.get_ident()
        current_name = threading.current_thread().name
        owner_match = self.is_current_broker_io_owner_context(caller)

        return {
            "broker_io_owner_mode": getattr(self, "broker_io_owner_mode", "audit_only_no_enforcement"),
            "broker_io_owner_expected": (
                f"{getattr(self, 'broker_io_owner_expected', 'execution_worker')}:"
                f"{owner_name}:{owner_ident}"
            ),
            "broker_io_owner_actual": f"{current_name}:{current_ident}",
            "broker_io_owner_match": str(bool(owner_match)).lower(),
            "broker_io_owner_violation_class": self.classify_broker_io_owner_violation(caller),
            "broker_io_owner_action": self.get_broker_io_owner_action(),
        }

    def format_broker_io_owner_audit_fields(self, caller=None):
        fields = self.build_broker_io_owner_audit_fields(caller)
        return " ".join(f"{key}={value}" for key, value in fields.items())

    def log_broker_io_owner_audit(
        self,
        broker_call,
        caller,
        reason_label=None,
        trade_id=None,
        symbol=None,
        trade_analysis_lock_context="not_locked",
    ):
        try:
            logger.info(
                "BROKER IO OWNER AUDIT | "
                f"broker_call={broker_call} "
                f"caller={caller} "
                f"reason_label={reason_label} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"thread_name={threading.current_thread().name} "
                f"thread_ident={threading.get_ident()} "
                f"trade_analysis_lock_context={trade_analysis_lock_context} "
                f"{self.format_broker_io_owner_audit_fields(caller)}"
            )
        except Exception:
            try:
                logger.exception("BROKER IO OWNER AUDIT LOG FAILED")
            except Exception:
                pass

    def assert_broker_io_owner(
        self,
        caller,
        broker_call,
        operation_type,
        trade_id=None,
        symbol=None,
    ):
        if not self.is_broker_io_enforcement_active():
            return
        if self.is_current_broker_io_owner_context(caller):
            return

        message = (
            "BROKER_IO_OWNER_VIOLATION | "
            f"broker_call={broker_call} "
            f"caller={caller} "
            f"operation_type={operation_type} "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"thread_name={threading.current_thread().name} "
            f"thread_ident={threading.get_ident()} "
            f"{self.format_broker_io_owner_audit_fields(caller)}"
        )
        logger.critical(message)
        raise RuntimeError(message)

    def log_broker_read_lock_risk(
        self,
        broker_call,
        caller,
        reason_label=None,
        trade_id=None,
        symbol=None,
        trade_analysis_lock_context="not_locked",
    ):
        if trade_analysis_lock_context != "maybe_locked":
            return

        try:
            logger.warning(
                "BROKER READ LOCK RISK | "
                f"broker_call={broker_call} "
                f"caller={caller} "
                f"reason_label={reason_label} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"thread_name={threading.current_thread().name} "
                f"thread_ident={threading.get_ident()} "
                f"trade_analysis_lock_context={trade_analysis_lock_context} "
                f"broker_read_lock_action={self.get_broker_io_owner_action()} "
                f"{self.format_broker_io_owner_audit_fields(caller)}"
            )
        except Exception:
            try:
                logger.exception("BROKER READ LOCK RISK LOG FAILED")
            except Exception:
                pass

    def log_broker_state_read_context(self, broker_call, caller, reason_label=None, trade_id=None, symbol=None):
        try:
            broker_read_context_class = self.classify_broker_read_context(caller)
            broker_read_owner_class = self.classify_broker_read_owner(caller)
            broker_read_owner_note = self.get_broker_read_owner_note(broker_read_owner_class)
            logger.info(
                "BROKER STATE READ CONTEXT | "
                f"broker_call={broker_call} "
                f"caller={caller} "
                f"reason_label={reason_label} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"broker_read_context_class={broker_read_context_class} "
                f"broker_read_owner_class={broker_read_owner_class} "
                f"broker_read_owner_note={broker_read_owner_note} "
                f"thread_name={threading.current_thread().name} "
                f"thread_ident={threading.get_ident()} "
                f"bot_stage={BOT_STAGE} "
                f"active_execution_trade_id={getattr(self, 'active_execution_trade_id', None)} "
                f"connectivity_uncertain={getattr(self, 'connectivity_uncertain', None)} "
                f"connectivity_reconciliation_required={getattr(self, 'connectivity_reconciliation_required', None)} "
                f"session_socket_connected={getattr(self, 'session_socket_connected', None)} "
                f"session_initialized={getattr(self, 'session_initialized', None)} "
                f"session_healthy={getattr(self, 'session_healthy', None)} "
                f"{self.format_broker_io_owner_audit_fields(caller)}"
            )
        except Exception:
            try:
                logger.exception("BROKER STATE READ CONTEXT LOG FAILED")
            except Exception:
                pass

    def log_broker_state_read_timing(
        self,
        broker_call,
        caller,
        reason_label,
        duration_ms,
        success,
        trade_id=None,
        symbol=None,
        failure=None,
        trade_analysis_lock_context="not_locked",
        failure_log_level="warning",
    ):
        try:
            broker_read_context_class = self.classify_broker_read_context(caller)
            broker_read_owner_class = self.classify_broker_read_owner(caller)
            broker_read_owner_note = self.get_broker_read_owner_note(broker_read_owner_class)
            broker_read_lock_risk = self.classify_broker_read_lock_risk(trade_analysis_lock_context)
            message = (
                "BROKER STATE READ TIMING | "
                f"broker_call={broker_call} "
                f"caller={caller} "
                f"reason_label={reason_label} "
                f"duration_ms={duration_ms} "
                f"broker_read_context_class={broker_read_context_class} "
                f"broker_read_owner_class={broker_read_owner_class} "
                f"broker_read_owner_note={broker_read_owner_note} "
                f"thread_name={threading.current_thread().name} "
                f"thread_ident={threading.get_ident()} "
                f"bot_stage={BOT_STAGE} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"success={str(bool(success)).lower()} "
                f"failure={failure} "
                f"trade_analysis_lock_context={trade_analysis_lock_context} "
                f"broker_read_lock_risk={broker_read_lock_risk} "
                f"broker_read_lock_action={self.get_broker_io_owner_action()} "
                f"{self.format_broker_io_owner_audit_fields(caller)}"
            )
            if success:
                logger.info(message)
                if duration_ms is not None and duration_ms >= BROKER_READ_SLOW_MS:
                    logger.warning(message.replace("BROKER STATE READ TIMING |", "BROKER STATE READ SLOW |", 1))
            elif failure_log_level == "info":
                logger.info(message)
            else:
                logger.warning(message)
        except Exception:
            try:
                logger.exception("BROKER STATE READ TIMING LOG FAILED")
            except Exception:
                pass

    # P183/P197: broker I/O wrappers with audit, timing, and single-owner enforcement.
    def broker_read(
        self,
        broker_call,
        read_fn,
        caller,
        reason_label,
        trade_id=None,
        symbol=None,
        trade_analysis_lock_context="not_locked",
        failure_log_level="warning",
    ):
        self.log_broker_io_owner_audit(
            broker_call=broker_call,
            caller=caller,
            reason_label=reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
        )
        self.log_broker_read_lock_risk(
            broker_call=broker_call,
            caller=caller,
            reason_label=reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
        )
        self.log_broker_state_read_context(
            broker_call=broker_call,
            caller=caller,
            reason_label=reason_label,
            trade_id=trade_id,
            symbol=symbol,
        )
        self.assert_broker_io_owner(
            caller=caller,
            broker_call=broker_call,
            operation_type="read",
            trade_id=trade_id,
            symbol=symbol,
        )
        broker_read_start_ms = self.current_monotonic_ms()
        try:
            result = read_fn()
            self.log_broker_state_read_timing(
                broker_call=broker_call,
                caller=caller,
                reason_label=reason_label,
                duration_ms=self.elapsed_ms(broker_read_start_ms),
                success=True,
                trade_id=trade_id,
                symbol=symbol,
                trade_analysis_lock_context=trade_analysis_lock_context,
            )
            return result
        except Exception as exc:
            self.log_broker_state_read_timing(
                broker_call=broker_call,
                caller=caller,
                reason_label=reason_label,
                duration_ms=self.elapsed_ms(broker_read_start_ms),
                success=False,
                trade_id=trade_id,
                symbol=symbol,
                failure=str(exc),
                trade_analysis_lock_context=trade_analysis_lock_context,
                failure_log_level=failure_log_level,
            )
            raise

    def broker_read_open_trades(self, caller, reason_label, trade_id=None, symbol=None, trade_analysis_lock_context="not_locked"):
        return self.broker_read(
            "openTrades",
            self.ib.openTrades,
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
        )

    def broker_read_open_orders(self, caller, reason_label, trade_id=None, symbol=None, trade_analysis_lock_context="not_locked"):
        return self.broker_read(
            "openOrders",
            self.ib.openOrders,
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
        )

    def broker_read_positions(self, caller, reason_label, trade_id=None, symbol=None, trade_analysis_lock_context="not_locked", failure_log_level="warning"):
        return self.broker_read(
            "positions",
            self.ib.positions,
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
            failure_log_level=failure_log_level,
        )

    def broker_read_fills(self, caller, reason_label, trade_id=None, symbol=None, trade_analysis_lock_context="not_locked"):
        return self.broker_read(
            "fills",
            self.ib.fills,
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
        )

    def broker_read_executions(self, exec_filter, caller, reason_label, trade_id=None, symbol=None, trade_analysis_lock_context="not_locked"):
        return self.broker_read(
            "reqExecutions",
            lambda: self.ib.reqExecutions(exec_filter),
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
        )

    def broker_read_is_connected(self, caller, reason_label, trade_id=None, symbol=None, failure_log_level="warning"):
        return self.broker_read(
            "isConnected",
            self.ib.isConnected,
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context="not_locked",
            failure_log_level=failure_log_level,
        )

    def broker_read_open_trades_open_orders(self, caller, reason_label, trade_id=None, symbol=None, trade_analysis_lock_context="not_locked"):
        return self.broker_read(
            "openTrades+openOrders",
            lambda: (self.ib.openTrades(), self.ib.openOrders()),
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
        )

    def broker_read_open_trades_positions_open_orders(self, caller, reason_label, trade_id=None, symbol=None, trade_analysis_lock_context="not_locked"):
        return self.broker_read(
            "openTrades+positions+openOrders",
            lambda: (self.ib.openTrades(), self.ib.positions(), self.ib.openOrders()),
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context=trade_analysis_lock_context,
        )

    # P186/P197: broker write owner audit used by enforced broker writes.
    def log_broker_write_owner_audit(
        self,
        broker_call,
        caller,
        reason_label=None,
        trade_id=None,
        symbol=None,
    ):
        try:
            broker_write_owner_class = self.classify_broker_write_owner(caller)
            logger.info(
                "BROKER WRITE OWNER AUDIT | "
                f"broker_call={broker_call} "
                f"caller={caller} "
                f"reason_label={reason_label} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"broker_write_owner_class={broker_write_owner_class} "
                f"thread_name={threading.current_thread().name} "
                f"thread_ident={threading.get_ident()} "
                f"{self.format_broker_io_owner_audit_fields(caller)}"
            )
        except Exception:
            try:
                logger.exception("BROKER WRITE OWNER AUDIT LOG FAILED")
            except Exception:
                pass

    def log_broker_write_timing(
        self,
        broker_call,
        caller,
        reason_label,
        duration_ms,
        success,
        trade_id=None,
        symbol=None,
        failure=None,
        failure_log_level="warning",
    ):
        try:
            broker_write_owner_class = self.classify_broker_write_owner(caller)
            message = (
                "BROKER WRITE TIMING | "
                f"broker_call={broker_call} "
                f"caller={caller} "
                f"reason_label={reason_label} "
                f"duration_ms={duration_ms} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"broker_write_owner_class={broker_write_owner_class} "
                f"thread_name={threading.current_thread().name} "
                f"thread_ident={threading.get_ident()} "
                f"success={str(bool(success)).lower()} "
                f"failure={failure} "
                f"{self.format_broker_io_owner_audit_fields(caller)}"
            )
            if success:
                logger.info(message)
                if duration_ms is not None and duration_ms >= BROKER_WRITE_SLOW_MS:
                    logger.warning(message.replace("BROKER WRITE TIMING |", "BROKER WRITE SLOW |", 1))
            elif failure_log_level == "info":
                logger.info(message)
            else:
                logger.warning(message)
        except Exception:
            try:
                logger.exception("BROKER WRITE TIMING LOG FAILED")
            except Exception:
                pass

    def broker_write(
        self,
        broker_call,
        write_fn,
        caller,
        reason_label,
        trade_id=None,
        symbol=None,
        failure_log_level="warning",
    ):
        self.log_broker_write_owner_audit(
            broker_call=broker_call,
            caller=caller,
            reason_label=reason_label,
            trade_id=trade_id,
            symbol=symbol,
        )
        self.assert_broker_io_owner(
            caller=caller,
            broker_call=broker_call,
            operation_type="write",
            trade_id=trade_id,
            symbol=symbol,
        )
        broker_write_start_ms = self.current_monotonic_ms()
        try:
            result = write_fn()
            self.log_broker_write_timing(
                broker_call=broker_call,
                caller=caller,
                reason_label=reason_label,
                duration_ms=self.elapsed_ms(broker_write_start_ms),
                success=True,
                trade_id=trade_id,
                symbol=symbol,
            )
            return result
        except Exception as exc:
            self.log_broker_write_timing(
                broker_call=broker_call,
                caller=caller,
                reason_label=reason_label,
                duration_ms=self.elapsed_ms(broker_write_start_ms),
                success=False,
                trade_id=trade_id,
                symbol=symbol,
                failure=str(exc),
                failure_log_level=failure_log_level,
            )
            raise

    def broker_write_place_order(self, contract, order, caller, reason_label, trade_id=None, symbol=None):
        self.assert_order_transmission_allowed(
            symbol=symbol,
            trade_id=trade_id,
            reason_label=reason_label,
        )
        self.assert_prd_live_release_allowed(
            symbol=symbol,
            trade_id=trade_id,
        )
        return self.broker_write(
            "placeOrder",
            lambda: self.ib.placeOrder(contract, order),
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
        )

    def broker_write_cancel_order(self, order, caller, reason_label, trade_id=None, symbol=None, real_broker_exposure=False):
        self.assert_cancel_mutation_allowed(
            symbol=symbol,
            trade_id=trade_id,
            reason_label=reason_label,
            real_broker_exposure=real_broker_exposure,
        )
        return self.broker_write(
            "cancelOrder",
            lambda: self.ib.cancelOrder(order),
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
        )

    def broker_write_connect(self, host, port, client_id, caller, reason_label):
        return self.broker_write(
            "connect",
            lambda: self.ib.connect(host, port, clientId=client_id),
            caller,
            reason_label,
        )

    def broker_write_req_contract_details(self, contract, caller, reason_label, symbol=None):
        return self.broker_write(
            "reqContractDetails",
            lambda: self.ib.reqContractDetails(contract),
            caller,
            reason_label,
            symbol=symbol,
        )

    def broker_write_sleep(self, seconds, caller, reason_label, trade_id=None, symbol=None):
        return self.broker_write(
            "sleep",
            lambda: self.ib.sleep(seconds),
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
        )

    def broker_get_req_id(self, caller, reason_label, trade_id=None, symbol=None):
        return self.broker_read(
            "client.getReqId",
            self.ib.client.getReqId,
            caller,
            reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context="not_locked",
        )

    def normalize_broker_position_quantity(self, value):
        try:
            quantity = float(value or 0.0)
            if abs(quantity) <= 1e-9:
                return 0.0
            return quantity
        except Exception:
            return 0.0

    def get_position_quantity_from_positions(self, positions, symbol):
        total = 0.0
        for position in positions or []:
            contract = getattr(position, "contract", None)
            if self.contract_matches_symbol(contract, symbol):
                total += self.normalize_broker_position_quantity(getattr(position, "position", 0.0))
        return self.normalize_broker_position_quantity(total)

    def read_session_close_broker_snapshot(self, reason_label, symbol=None):
        open_trades, positions, open_orders = self.broker_read_open_trades_positions_open_orders(
            caller="session_close_sweep",
            reason_label=reason_label,
            symbol=symbol,
            trade_analysis_lock_context="not_locked",
        )
        return {
            "open_trades": open_trades,
            "positions": positions,
            "open_orders": open_orders,
        }

    def get_startup_reconciliation_symbols(self):
        return [
            symbol for symbol in QUALIFIED_FUTURE_SYMBOLS
            if symbol in INSTRUMENT_SPECS and symbol not in RUNTIME_DISABLED_SYMBOLS
        ]

    def should_block_external_entry_for_startup_reconciliation(self):
        if not self.startup_reconciliation_required:
            return False
        return not self.startup_reconciliation_completed

    def should_block_external_entry(self):
        if self.should_block_external_entry_for_startup_reconciliation():
            return True, "startup_reconciliation", (
                self.external_entries_blocked_reason
                or self.startup_reconciliation_block_reason
                or "startup_reconciliation_required"
            )
        if self.external_entries_blocked_reason:
            return True, "broker_quarantine", self.external_entries_blocked_reason
        return False, None, None

    def log_startup_entry_blocked(self, job=None, normalized=None, location=None):
        logger.warning(
            "STARTUP_RECONCILIATION_ENTRY_BLOCKED | "
            f"status={self.startup_reconciliation_status} "
            f"reason={self.startup_reconciliation_block_reason} "
            f"external_entries_blocked_reason={self.external_entries_blocked_reason} "
            f"ambiguous_symbols={sorted(self.startup_reconciliation_ambiguous_symbols)} "
            f"symbol={(job or {}).get('symbol') or (normalized or {}).get('symbol')} "
            f"signal_id={(job or {}).get('signal_id') or (normalized or {}).get('signal_id')} "
            f"location={location}"
        )

    def log_external_entry_blocked(self, job=None, normalized=None, location=None, block_source=None, reason=None):
        if block_source == "startup_reconciliation":
            self.log_startup_entry_blocked(job=job, normalized=normalized, location=location)
            return

        logger.critical(
            "EXTERNAL_ENTRY_BLOCKED | "
            f"block_source={block_source} "
            f"reason={reason} "
            f"external_entries_blocked_reason={self.external_entries_blocked_reason} "
            f"symbol={(job or {}).get('symbol') or (normalized or {}).get('symbol')} "
            f"signal_id={(job or {}).get('signal_id') or (normalized or {}).get('signal_id')} "
            f"location={location}"
        )

    def read_startup_broker_snapshot(self):
        open_trades, positions, open_orders = self.broker_read_open_trades_positions_open_orders(
            caller="startup_reconciliation",
            reason_label="startup_reconciliation_snapshot",
            trade_analysis_lock_context="not_locked",
        )
        return {
            "open_trades": open_trades,
            "positions": positions,
            "open_orders": open_orders,
        }

    def is_startup_bot_owned_order_for_symbol(self, order, symbol):
        order_ref = getattr(order, "orderRef", None)
        parsed = self.parse_order_ref(order_ref)
        return bool(parsed.get("symbol") == str(symbol).upper())

    def get_startup_bot_order_role(self, order, symbol):
        if not self.is_startup_bot_owned_order_for_symbol(order, symbol):
            return None
        parsed = self.parse_order_ref(getattr(order, "orderRef", None))
        return parsed.get("leg_role")

    def cleanup_startup_stale_orders_if_safe(self, snapshot, symbols):
        cancel_count = 0
        for symbol in symbols:
            position_qty = self.get_position_quantity_from_positions(
                snapshot.get("positions"),
                symbol,
            )
            if position_qty != 0.0:
                continue

            orders_by_id = {}
            ambiguous_order_ids = set()
            for trade in snapshot.get("open_trades") or []:
                order = getattr(trade, "order", None)
                status = getattr(trade, "orderStatus", None)
                contract = getattr(trade, "contract", None)
                if not self.order_is_open_for_session_close(order, status):
                    continue
                if not self.contract_matches_symbol(contract, symbol):
                    continue
                order_id = getattr(order, "orderId", None)
                if self.is_startup_bot_owned_order_for_symbol(order, symbol):
                    orders_by_id[order_id] = order
                elif order_id is not None:
                    ambiguous_order_ids.add(order_id)

            for order in snapshot.get("open_orders") or []:
                order_id = getattr(order, "orderId", None)
                if self.is_startup_bot_owned_order_for_symbol(order, symbol):
                    orders_by_id[order_id] = order

            if ambiguous_order_ids:
                logger.critical(
                    "STARTUP_RECONCILIATION_AMBIGUOUS | "
                    f"symbol={symbol} "
                    f"ambiguous_order_ids={sorted(ambiguous_order_ids)} "
                    "decision=skip_startup_cleanup_and_block"
                )
                continue

            for order_id, order in list(orders_by_id.items()):
                if order is None:
                    continue
                logger.warning(
                    "STARTUP_RECONCILIATION_STALE_ORDER_CANCEL_REQUESTED | "
                    f"symbol={symbol} "
                    f"order_id={order_id} "
                    f"order_ref={getattr(order, 'orderRef', None)} "
                    "reason=no_position_bot_owned_open_order"
                )
                self.broker_write_cancel_order(
                    order,
                    caller="startup_reconciliation",
                    reason_label="startup_stale_order_cleanup",
                    symbol=symbol,
                    real_broker_exposure=True,
                )
                cancel_count += 1

        if cancel_count:
            self.broker_write_sleep(
                EOD_FLATTEN_RECHECK_SECONDS,
                caller="startup_reconciliation",
                reason_label="startup_stale_order_cleanup_wait",
            )
        return cancel_count

    def get_startup_position_for_symbol(self, snapshot, symbol):
        for position in snapshot.get("positions") or []:
            contract = getattr(position, "contract", None)
            position_qty = self.normalize_broker_position_quantity(
                getattr(position, "position", 0.0)
            )
            if position_qty and self.contract_matches_symbol(contract, symbol):
                return position, position_qty
        return None, 0.0

    def get_startup_protective_orders_for_symbol(self, snapshot, symbol):
        orders_by_role = {"TP": [], "SL": []}
        ambiguous_order_ids = []
        for trade in snapshot.get("open_trades") or []:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            contract = getattr(trade, "contract", None)
            if not self.order_is_open_for_session_close(order, status):
                continue
            if not self.contract_matches_symbol(contract, symbol):
                continue
            role = self.get_startup_bot_order_role(order, symbol)
            order_id = getattr(order, "orderId", None)
            if role in orders_by_role:
                orders_by_role[role].append(order)
            elif order_id is not None:
                ambiguous_order_ids.append(order_id)
        return orders_by_role, sorted(set(ambiguous_order_ids))

    def build_startup_reconstruction_job(self, symbol, side, position_qty, entry_price):
        now_dt = datetime.now(timezone.utc)
        return {
            "symbol": symbol,
            "side": side,
            "bot_stage": BOT_STAGE,
            "entry": entry_price,
            "signal_time": now_dt,
            "enqueue_time": now_dt,
            "grade": "STARTUP_RECONSTRUCTED",
            "truth_classification": "STARTUP_RECONSTRUCTED",
            "det_classification": "STARTUP_RECONSTRUCTED",
            "primary_reason": "startup_reconstructed_protected_position",
            "execution_lane": "startup_reconstruction",
            "promoted_from_shadow": False,
            "shadow_override_reason": "",
            "webhook_received_time": None,
            "classification_completed_time": None,
            "queue_put_time": None,
            "worker_pickup_time": now_dt,
            "preflight_completed_time": None,
            "intended_risk_percent": None,
            "allowed_money_risk": None,
            "stop_distance_points": None,
            "raw_position_size": None,
            "normalized_position_size": abs(float(position_qty)),
            "risk_intent_profile": "STARTUP_RECONSTRUCTED",
            "risk_intent_percent": None,
            "risk_intent_status": "startup_reconstructed",
            "risk_intent_reason": "startup_reconstructed_existing_position",
            "theoretical_size_before_containment": abs(float(position_qty)),
            "approved_size_after_containment": abs(float(position_qty)),
            "containment_status": "startup_reconstructed",
            "containment_stage": BOT_STAGE,
            "containment_profile": None,
            "containment_max_size": self.get_containment_max_size(symbol),
            "containment_max_notional": self.get_containment_max_notional(symbol),
            "containment_hierarchy": ">".join(CONTAINMENT_ACTION_HIERARCHY),
            "containment_decision_source": "none",
            "containment_denial_source": "none",
            "triggering_containment_rule": "none",
            "notional_model": self.get_notional_metadata(symbol)[0],
            "notional_currency": self.get_notional_metadata(symbol)[1],
            "capped_size_after_size_containment": abs(float(position_qty)),
            "estimated_notional_before_containment": self.estimate_notional_exposure(
                symbol,
                abs(float(position_qty)),
                entry_price,
            ),
            "estimated_notional_after_size_containment": self.estimate_notional_exposure(
                symbol,
                abs(float(position_qty)),
                entry_price,
            ),
            "estimated_notional_after_containment": self.estimate_notional_exposure(
                symbol,
                abs(float(position_qty)),
                entry_price,
            ),
            "containment_action": "approve_as_is",
            "containment_reason": "startup_reconstructed_existing_position",
        }

    def validate_startup_protective_order_pair(self, symbol, side, position_qty, tp_order, sl_order):
        expected_exit_action = "SELL" if side == "long" else "BUY"
        tp_action = str(getattr(tp_order, "action", "") or "").upper()
        sl_action = str(getattr(sl_order, "action", "") or "").upper()
        tp_qty = self.normalize_fill_quantity(getattr(tp_order, "totalQuantity", None))
        sl_qty = self.normalize_fill_quantity(getattr(sl_order, "totalQuantity", None))
        expected_qty = abs(float(position_qty))
        tp_price = getattr(tp_order, "lmtPrice", None)
        sl_price = getattr(sl_order, "auxPrice", None)

        reasons = []
        if self.get_startup_bot_order_role(tp_order, symbol) != "TP":
            reasons.append("tp_role_invalid")
        if self.get_startup_bot_order_role(sl_order, symbol) != "SL":
            reasons.append("sl_role_invalid")
        if tp_action != expected_exit_action:
            reasons.append("tp_action_mismatch")
        if sl_action != expected_exit_action:
            reasons.append("sl_action_mismatch")
        if abs(tp_qty - expected_qty) > 1e-9:
            reasons.append("tp_quantity_mismatch")
        if abs(sl_qty - expected_qty) > 1e-9:
            reasons.append("sl_quantity_mismatch")
        if tp_price is None or float(tp_price or 0.0) <= 0:
            reasons.append("tp_price_invalid")
        if sl_price is None or float(sl_price or 0.0) <= 0:
            reasons.append("sl_price_invalid")

        if reasons:
            return False, ",".join(reasons)
        return True, "startup_protective_pair_valid"

    def reconstruct_startup_trade_record(self, symbol, position, position_qty, tp_order, sl_order):
        side = "long" if position_qty > 0 else "short"
        entry_price = getattr(position, "avgCost", None) or 0.0
        stop_price = getattr(sl_order, "auxPrice", None) or 0.0
        target_price = getattr(tp_order, "lmtPrice", None) or 0.0
        now_dt = datetime.now(timezone.utc)
        synthetic_job = self.build_startup_reconstruction_job(
            symbol,
            side,
            position_qty,
            entry_price,
        )
        record = self.build_trade_record(
            synthetic_job,
            now_dt,
            now_dt,
            now_dt,
            entry_price,
            entry_price,
            stop_price,
            target_price,
            None,
            getattr(tp_order, "orderId", None),
            getattr(sl_order, "orderId", None),
        )
        tp_order_quantity = self.normalize_broker_quantity_or_none(
            getattr(tp_order, "totalQuantity", None)
        )
        sl_order_quantity = self.normalize_broker_quantity_or_none(
            getattr(sl_order, "totalQuantity", None)
        )
        planned_tp_quantity = tp_order_quantity if tp_order_quantity is not None else abs(float(position_qty))
        planned_sl_quantity = sl_order_quantity if sl_order_quantity is not None else abs(float(position_qty))
        tp_order_ref = self.get_order_ref(order=tp_order)
        sl_order_ref = self.get_order_ref(order=sl_order)
        parsed_tp_order_ref = self.parse_order_ref(tp_order_ref)
        parsed_sl_order_ref = self.parse_order_ref(sl_order_ref)
        parsed_startup_refs = [parsed_tp_order_ref, parsed_sl_order_ref]
        startup_has_foreign_run_ref = any(
            parsed.get("is_valid")
            and not parsed.get("is_legacy")
            and parsed.get("run_id") != self.run_id
            for parsed in parsed_startup_refs
        )
        startup_has_legacy_ref = any(
            parsed.get("is_legacy") or not parsed.get("is_valid")
            for parsed in parsed_startup_refs
        )
        broker_identity_scope = (
            "startup_reconstructed_foreign"
            if startup_has_foreign_run_ref
            else "startup_reconstructed_legacy"
            if startup_has_legacy_ref
            else "startup_reconstructed_current_run"
        )
        with self.trade_analysis_lock:
            trade_id = record["trade_id"]
            record.update({
                "state": "EXIT_WORKING",
                "planned_parent_quantity": None,
                "planned_tp_quantity": planned_tp_quantity,
                "planned_sl_quantity": planned_sl_quantity,
                "order_ref_entry": None,
                "order_ref_tp": tp_order_ref,
                "order_ref_sl": sl_order_ref,
                "broker_identity_scope": broker_identity_scope,
                "startup_reconstructed_order_refs": {
                    "tp": tp_order_ref,
                    "sl": sl_order_ref,
                },
                "tp_perm_id": getattr(tp_order, "permId", None),
                "sl_perm_id": getattr(sl_order, "permId", None),
                "entry_fill_price": entry_price,
                "entry_fill_time": now_dt,
                "entry_filled": True,
                "position_size": abs(float(position_qty)),
                "planned_position_size": abs(float(position_qty)),
                "realized_entry_quantity": abs(float(position_qty)),
                "cumulative_entry_quantity": abs(float(position_qty)),
                "execution_validation_status": "startup_reconstructed",
                "broker_acknowledged_at": now_dt,
                "broker_live_at": now_dt,
                "reconstructed_from_startup": True,
                "startup_reconstructed_at": now_dt,
                "submitted_counted": True,
                "broker_acknowledged_counted": True,
                "broker_live_counted": True,
            })
            record["events"].append(f"{self.utc_now_iso()} | STARTUP_RECONSTRUCTED protected_position")
            self.trade_analysis[trade_id] = record
            if record["tp_order_id"] is not None:
                self.order_to_trade[record["tp_order_id"]] = trade_id
            if record["sl_order_id"] is not None:
                self.order_to_trade[record["sl_order_id"]] = trade_id
            self.active_execution_trade_id = trade_id
            self.arm_time_exit_on_entry_exposure(trade_id, record, now_dt)
            record["events"].append(
                f"{self.utc_now_iso()} | TIME_EXIT_RESTART_SAFE_ARMED startup_reconstructed_position"
            )

        logger.warning(
            "STARTUP_RECONCILIATION_RECONSTRUCTED | "
            f"trade_id={trade_id} "
            f"run_id={self.run_id} "
            f"broker_identity_scope={broker_identity_scope} "
            f"symbol={symbol} "
            f"side={side} "
            f"position_qty={position_qty} "
            f"tp_order_id={record['tp_order_id']} "
            f"sl_order_id={record['sl_order_id']} "
            f"tp_order_ref={tp_order_ref} "
            f"sl_order_ref={sl_order_ref} "
            f"time_exit_status={record.get('time_exit_status')} "
            f"time_exit_deadline_at={record.get('time_exit_deadline_at')} "
            "decision=track_existing_protected_position_with_restart_safe_time_exit"
        )
        if broker_identity_scope in {"startup_reconstructed_legacy", "startup_reconstructed_foreign"}:
            logger.warning(
                "LEGACY_ORDER_REF_DETECTED | "
                f"trade_id={trade_id} "
                f"run_id={self.run_id} "
                f"symbol={symbol} "
                f"broker_identity_scope={broker_identity_scope} "
                f"tp_order_ref={tp_order_ref} "
                f"tp_parsed_run_id={parsed_tp_order_ref.get('run_id')} "
                f"tp_parse_error={parsed_tp_order_ref.get('parse_error')} "
                f"sl_order_ref={sl_order_ref} "
                f"sl_parsed_run_id={parsed_sl_order_ref.get('run_id')} "
                f"sl_parse_error={parsed_sl_order_ref.get('parse_error')} "
                "decision=allow_only_startup_reconstructed_exact_order_or_perm_matches"
            )
        return trade_id

    def reconstruct_startup_positions_if_safe(self, snapshot, symbols):
        reconstructed_symbols = set()
        for symbol in symbols:
            position, position_qty = self.get_startup_position_for_symbol(snapshot, symbol)
            if position is None or position_qty == 0.0:
                continue
            orders_by_role, ambiguous_order_ids = self.get_startup_protective_orders_for_symbol(
                snapshot,
                symbol,
            )
            if ambiguous_order_ids:
                logger.critical(
                    "STARTUP_RECONCILIATION_AMBIGUOUS | "
                    f"symbol={symbol} "
                    f"ambiguous_order_ids={ambiguous_order_ids} "
                    "decision=skip_reconstruction_and_block"
                )
                continue
            if len(orders_by_role["TP"]) == 1 and len(orders_by_role["SL"]) == 1:
                side = "long" if position_qty > 0 else "short"
                pair_valid, validation_reason = self.validate_startup_protective_order_pair(
                    symbol,
                    side,
                    position_qty,
                    orders_by_role["TP"][0],
                    orders_by_role["SL"][0],
                )
                if not pair_valid:
                    logger.critical(
                        "STARTUP_RECONCILIATION_AMBIGUOUS | "
                        f"symbol={symbol} "
                        f"position_qty={position_qty} "
                        f"reason={validation_reason} "
                        "decision=skip_reconstruction_and_block"
                    )
                    continue
                self.reconstruct_startup_trade_record(
                    symbol,
                    position,
                    position_qty,
                    orders_by_role["TP"][0],
                    orders_by_role["SL"][0],
                )
                reconstructed_symbols.add(symbol)
                continue
            if not orders_by_role["SL"]:
                logger.critical(
                    "STARTUP_RECONCILIATION_BLOCKED | "
                    f"symbol={symbol} "
                    f"position_qty={position_qty} "
                    "reason=position_without_protective_sl "
                    "decision=external_entries_remain_blocked_operator_action_required"
                )
        return reconstructed_symbols

    def classify_startup_symbol_reality(self, symbol, snapshot, reconstructed_symbols=None):
        reconstructed_symbols = reconstructed_symbols or set()
        position_qty = self.get_position_quantity_from_positions(
            snapshot.get("positions"),
            symbol,
        )
        matching_open_trade_order_ids = []
        matching_open_order_ids = []
        ambiguous_open_trade_order_ids = []

        for trade in snapshot.get("open_trades") or []:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            contract = getattr(trade, "contract", None)
            if (
                self.order_is_open_for_session_close(order, status)
                and self.contract_matches_symbol(contract, symbol)
            ):
                order_id = getattr(order, "orderId", None)
                if self.is_startup_bot_owned_order_for_symbol(order, symbol) and order_id is not None:
                    matching_open_trade_order_ids.append(order_id)
                elif order_id is not None:
                    ambiguous_open_trade_order_ids.append(order_id)

        for order in snapshot.get("open_orders") or []:
            order_id = getattr(order, "orderId", None)
            order_symbol_match = False
            order_ref = getattr(order, "orderRef", None)
            if order_ref and f"|{symbol}|" in str(order_ref):
                order_symbol_match = True
            if order_symbol_match and order_id is not None:
                matching_open_order_ids.append(order_id)

        has_position = position_qty != 0.0
        has_open_orders = bool(matching_open_trade_order_ids or matching_open_order_ids)
        has_ambiguous_orders = bool(ambiguous_open_trade_order_ids)
        status = "clear"
        reason = "no_position_no_open_orders"
        if symbol in reconstructed_symbols:
            status = "reconstructed"
            reason = "startup_reconstructed_protected_position"
        elif has_ambiguous_orders:
            status = "blocked"
            reason = "ambiguous_open_orders"
        elif has_position and has_open_orders:
            status = "blocked"
            reason = "position_with_open_orders_requires_reconstruction"
        elif has_position:
            status = "blocked"
            reason = "position_without_confirmed_protection"
        elif has_open_orders:
            status = "blocked"
            reason = "open_orders_require_startup_cleanup_or_operator_review"

        logger.warning(
            "STARTUP_RECONCILIATION_SYMBOL_SNAPSHOT | "
            f"symbol={symbol} "
            f"status={status} "
            f"reason={reason} "
            f"position_qty={position_qty} "
            f"matching_open_trade_order_ids={sorted(set(matching_open_trade_order_ids))} "
            f"matching_open_order_ids={sorted(set(matching_open_order_ids))} "
            f"ambiguous_open_trade_order_ids={sorted(set(ambiguous_open_trade_order_ids))}"
        )
        return {
            "symbol": symbol,
            "status": status,
            "reason": reason,
            "position_qty": position_qty,
            "matching_open_trade_order_ids": sorted(set(matching_open_trade_order_ids)),
            "matching_open_order_ids": sorted(set(matching_open_order_ids)),
            "ambiguous_open_trade_order_ids": sorted(set(ambiguous_open_trade_order_ids)),
        }

    def run_startup_reconciliation(self):
        self.startup_reconciliation_started_at = datetime.now(timezone.utc)
        self.startup_reconciliation_status = "running"
        self.startup_reconciliation_block_reason = "startup_reconciliation_running"
        self.external_entries_blocked_reason = "startup_reconciliation_running"
        self.startup_reconciliation_ambiguous_symbols.clear()
        symbols = self.get_startup_reconciliation_symbols()

        logger.warning(
            "STARTUP_RECONCILIATION_STARTED | "
            f"symbols={symbols} "
            f"stage={BOT_STAGE}"
        )

        try:
            snapshot = self.read_startup_broker_snapshot()
            cleanup_count = self.cleanup_startup_stale_orders_if_safe(
                snapshot,
                symbols,
            )
            if cleanup_count:
                logger.warning(
                    "STARTUP_RECONCILIATION_STALE_ORDER_CLEANUP_COMPLETE | "
                    f"cancel_count={cleanup_count} "
                    "decision=resnapshot_before_classification"
                )
                snapshot = self.read_startup_broker_snapshot()
            reconstructed_symbols = self.reconstruct_startup_positions_if_safe(
                snapshot,
                symbols,
            )
            symbol_results = [
                self.classify_startup_symbol_reality(
                    symbol,
                    snapshot,
                    reconstructed_symbols=reconstructed_symbols,
                )
                for symbol in symbols
            ]
        except Exception as exc:
            self.startup_reconciliation_status = "blocked"
            self.startup_reconciliation_block_reason = f"startup_reconciliation_snapshot_failed:{exc}"
            self.external_entries_blocked_reason = self.startup_reconciliation_block_reason
            logger.exception(
                "STARTUP_RECONCILIATION_BLOCKED | "
                f"reason={self.startup_reconciliation_block_reason}"
            )
            return False

        blocked = [
            result for result in symbol_results
            if result["status"] not in {"clear", "reconstructed"}
        ]
        if blocked:
            self.startup_reconciliation_status = "blocked"
            self.startup_reconciliation_block_reason = "startup_reconciliation_broker_reality_not_clear"
            self.external_entries_blocked_reason = self.startup_reconciliation_block_reason
            self.startup_reconciliation_ambiguous_symbols = {
                result["symbol"] for result in blocked
            }
            logger.critical(
                "STARTUP_RECONCILIATION_BLOCKED | "
                f"blocked_symbols={sorted(self.startup_reconciliation_ambiguous_symbols)} "
                f"results={json.dumps(symbol_results, sort_keys=True)} "
                "decision=external_entries_remain_blocked"
            )
            return False

        self.startup_reconciliation_completed = True
        self.startup_reconciliation_completed_at = datetime.now(timezone.utc)
        self.startup_reconciliation_status = "clear"
        self.startup_reconciliation_block_reason = None
        self.external_entries_blocked_reason = None
        logger.warning(
            "STARTUP_RECONCILIATION_CLEAR | "
            f"symbols={symbols}"
        )
        logger.warning(
            "STARTUP_RECONCILIATION_COMPLETED | "
            f"started_at={self.to_iso(self.startup_reconciliation_started_at)} "
            f"completed_at={self.to_iso(self.startup_reconciliation_completed_at)} "
            "external_entries_allowed=true"
        )
        return True

    def get_session_close_symbol_record_snapshots(self, symbol):
        with self.trade_analysis_lock:
            return [
                dict(record)
                for record in self.trade_analysis.values()
                if (
                    record.get("symbol") == symbol
                    and not record.get("summary_logged")
                    and record.get("state") in ACTIVE_TRADE_STATES
                )
            ]

    def order_is_open_for_session_close(self, order=None, status=None):
        order_status = getattr(status, "status", None)
        if order_status not in OPEN_BROKER_ORDER_STATUSES:
            return False
        remaining = getattr(status, "remaining", None)
        try:
            if remaining is not None and float(remaining) <= 0:
                return False
        except Exception:
            pass
        return order is not None

    def collect_session_close_orders_for_symbol(self, symbol, snapshot):
        records = self.get_session_close_symbol_record_snapshots(symbol)
        parent_order_ids = {record.get("parent_order_id") for record in records}
        child_order_ids = {
            order_id
            for record in records
            for order_id in (record.get("tp_order_id"), record.get("sl_order_id"))
        }
        parent_order_ids.discard(None)
        child_order_ids.discard(None)

        working_entries = {}
        symbol_orders = {}
        child_orders = {}

        for trade in snapshot.get("open_trades") or []:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            contract = getattr(trade, "contract", None)
            if not self.order_is_open_for_session_close(order, status):
                continue
            order_id = getattr(order, "orderId", None)
            if order_id in parent_order_ids:
                working_entries[order_id] = order
            if order_id in child_order_ids:
                child_orders[order_id] = order
            if self.contract_matches_symbol(contract, symbol):
                symbol_orders[order_id] = order

        for order in snapshot.get("open_orders") or []:
            order_id = getattr(order, "orderId", None)
            if order_id in parent_order_ids:
                working_entries[order_id] = order
            if order_id in child_order_ids:
                child_orders[order_id] = order

        return working_entries, child_orders, symbol_orders

    def cancel_session_close_orders(self, orders_by_id, symbol, reason_label, log_event):
        cancel_count = 0
        for order_id, order in list((orders_by_id or {}).items()):
            if order is None:
                continue
            try:
                logger.warning(
                    f"{log_event} | "
                    f"symbol={symbol} "
                    f"order_id={order_id} "
                    f"perm_id={getattr(order, 'permId', None)} "
                    f"action={getattr(order, 'action', None)} "
                    f"order_type={getattr(order, 'orderType', None)} "
                    f"reason={reason_label} "
                    "lifecycle_state=SESSION_CLOSING"
                )
                self.broker_write_cancel_order(
                    order,
                    caller=(
                        "session_close_cancel_working_entries"
                        if log_event == "SESSION_WORKING_ENTRY_CANCEL_REQUESTED"
                        else "session_close_cancel_stale_orders"
                    ),
                    reason_label=reason_label,
                    symbol=symbol,
                    real_broker_exposure=True,
                )
                cancel_count += 1
            except Exception as exc:
                logger.exception(
                    "SESSION_CLOSE_ORDER_CANCEL_FAILED | "
                    f"symbol={symbol} order_id={order_id} reason={reason_label} failure={exc}"
                )
        return cancel_count

    def cancel_working_entries_for_session_close(self, symbol, state, snapshot, reason_label):
        if not SESSION_CLOSE_CANCEL_WORKING_ENTRIES:
            return 0

        working_entries, child_orders, _ = self.collect_session_close_orders_for_symbol(symbol, snapshot)
        if not working_entries:
            return 0

        cancel_count = self.cancel_session_close_orders(
            working_entries,
            symbol,
            reason_label,
            "SESSION_WORKING_ENTRY_CANCEL_REQUESTED",
        )
        self.broker_write_sleep(
            EOD_FLATTEN_RECHECK_SECONDS,
            caller="session_close_cancel_working_entries",
            reason_label=reason_label,
            symbol=symbol,
        )
        refreshed = self.read_session_close_broker_snapshot(reason_label, symbol=symbol)
        remaining_entries, _, _ = self.collect_session_close_orders_for_symbol(symbol, refreshed)
        position_qty = self.get_position_quantity_from_positions(refreshed.get("positions"), symbol)

        for order_id in working_entries:
            if order_id not in remaining_entries:
                logger.warning(
                    "SESSION_WORKING_ENTRY_CANCELLED | "
                    f"symbol={symbol} "
                    f"order_id={order_id} "
                    f"position_qty={position_qty} "
                    f"minutes_to_close={state.get('minutes_to_close')} "
                    f"configured_flat_by_time={state.get('configured_flat_by_time')} "
                    f"timezone={state.get('timezone')} "
                    f"reason={reason_label} "
                    "lifecycle_state=SESSION_ENTRY_CANCELLED"
                )

        if position_qty == 0.0 and child_orders:
            self.cancel_session_close_orders(
                child_orders,
                symbol,
                reason_label,
                "SESSION_CLOSE_STALE_ORDER_CANCEL_REQUESTED",
            )

        return cancel_count

    def get_current_broker_position_quantity_for_symbol(self, symbol, reason_label):
        positions = self.broker_read_positions(
            caller="session_close_flatten",
            reason_label=reason_label,
            symbol=symbol,
            trade_analysis_lock_context="not_locked",
        )
        return self.get_position_quantity_from_positions(positions, symbol)

    def cancel_symbol_orders_for_session_close(self, symbol, reason_label):
        snapshot = self.read_session_close_broker_snapshot(reason_label, symbol=symbol)
        _, child_orders, symbol_orders = self.collect_session_close_orders_for_symbol(symbol, snapshot)
        orders_to_cancel = dict(symbol_orders)
        orders_to_cancel.update(child_orders)
        return self.cancel_session_close_orders(
            orders_to_cancel,
            symbol,
            reason_label,
            "SESSION_CLOSE_STALE_ORDER_CANCEL_REQUESTED",
        )

    def submit_session_close_flatten_order(self, symbol, quantity, action, reason_label, attempt):
        if EOD_FLATTEN_ORDER_TYPE != "MKT":
            raise RuntimeError(f"Unsupported EOD flatten order type: {EOD_FLATTEN_ORDER_TYPE}")
        if self.is_prd_dry_run_enabled():
            logger.critical(
                "SESSION_CLOSE_FLATTEN_BLOCKED_DRY_RUN | "
                f"symbol={symbol} "
                f"action={action} "
                f"quantity={quantity} "
                f"attempt={attempt} "
                f"reason_label={reason_label} "
                "operator_action_required=True "
                "reason=prd_dry_run_no_order_transmission"
            )
            raise RuntimeError("prd_dry_run_no_order_transmission")
        if not self.ensure_symbol_contract_ready(symbol):
            raise RuntimeError(f"contract_not_ready:{symbol}")
        contract = self.get_contract(symbol)
        order = MarketOrder(action, quantity)
        with self.order_id_lock:
            if self.next_order_id is None:
                self.next_order_id = self.broker_get_req_id(
                    caller="session_close_flatten",
                    reason_label=reason_label,
                    symbol=symbol,
                )
            order.orderId = self.next_order_id
            self.next_order_id += 1
        order.tif = self.get_order_tif(symbol, "session_close_flatten") or "DAY"
        order.orderRef = self.build_order_ref("SESSION_CLOSE", symbol, f"EOD_FLATTEN_ATTEMPT_{attempt}")
        trade = self.broker_write_place_order(
            contract,
            order,
            caller="session_close_flatten",
            reason_label=reason_label,
            symbol=symbol,
        )
        logger.critical(
            "SESSION_CLOSE_FLATTEN_ORDER_SUBMITTED | "
            f"symbol={symbol} "
            f"action={action} "
            f"quantity={quantity} "
            f"order_id={getattr(order, 'orderId', None)} "
            f"perm_id={getattr(order, 'permId', None)} "
            f"order_ref={getattr(order, 'orderRef', None)} "
            f"attempt={attempt} "
            f"order_type={EOD_FLATTEN_ORDER_TYPE} "
            f"reason={reason_label} "
            "lifecycle_state=EOD_FLATTENING"
        )
        return trade

    def confirm_session_close_flat_for_symbol(self, symbol, reason_label):
        position_qty = self.get_current_broker_position_quantity_for_symbol(symbol, reason_label)
        if position_qty == 0.0:
            self.cancel_symbol_orders_for_session_close(symbol, f"{reason_label}:flat_confirmed_cleanup")
            logger.warning(
                "SESSION_CLOSE_POSITION_FLAT_CONFIRMED | "
                f"symbol={symbol} "
                f"position_qty={position_qty} "
                f"reason={reason_label} "
                "lifecycle_state=EOD_FLAT_CONFIRMED"
            )
            return True
        return False

    def flatten_symbol_for_session_close(self, symbol, state, reason_label):
        if not EOD_FLATTEN_ENABLED:
            return False
        last_position_qty = None
        logger.critical(
            "SESSION_CLOSE_FLATTEN_STARTED | "
            f"symbol={symbol} "
            f"minutes_to_close={state.get('minutes_to_close')} "
            f"configured_flat_by_time={state.get('configured_flat_by_time')} "
            f"timezone={state.get('timezone')} "
            f"reason={reason_label} "
            "lifecycle_state=EOD_FLATTENING"
        )

        for attempt in range(1, EOD_FLATTEN_MAX_ATTEMPTS + 1):
            self.cancel_symbol_orders_for_session_close(symbol, f"{reason_label}:attempt_{attempt}:pre_flatten_cancel")
            self.broker_write_sleep(
                EOD_FLATTEN_RECHECK_SECONDS,
                caller="session_close_flatten",
                reason_label=reason_label,
                symbol=symbol,
            )
            position_qty = self.get_current_broker_position_quantity_for_symbol(symbol, reason_label)
            last_position_qty = position_qty
            logger.warning(
                "SESSION_CLOSE_FLATTEN_RECHECK | "
                f"symbol={symbol} "
                f"position_qty={position_qty} "
                f"attempt={attempt} "
                f"reason=pre_order_position_recheck "
                f"configured_flat_by_time={state.get('configured_flat_by_time')} "
                f"timezone={state.get('timezone')}"
            )
            if position_qty == 0.0:
                return self.confirm_session_close_flat_for_symbol(symbol, reason_label)

            action = "SELL" if position_qty > 0 else "BUY"
            quantity = abs(position_qty)
            self.submit_session_close_flatten_order(
                symbol,
                quantity,
                action,
                reason_label,
                attempt,
            )
            self.broker_write_sleep(
                EOD_FLATTEN_RECHECK_SECONDS,
                caller="session_close_flatten",
                reason_label=reason_label,
                symbol=symbol,
            )
            post_qty = self.get_current_broker_position_quantity_for_symbol(symbol, reason_label)
            last_position_qty = post_qty
            logger.warning(
                "SESSION_CLOSE_FLATTEN_RECHECK | "
                f"symbol={symbol} "
                f"position_qty={post_qty} "
                f"attempt={attempt} "
                f"reason=post_order_position_recheck "
                f"configured_flat_by_time={state.get('configured_flat_by_time')} "
                f"timezone={state.get('timezone')}"
            )
            if post_qty == 0.0:
                return self.confirm_session_close_flat_for_symbol(symbol, reason_label)

        logger.critical(
            "SESSION_CLOSE_FLATTEN_FAILED | "
            "severity=critical "
            f"symbol={symbol} "
            f"position={last_position_qty} "
            f"reason=max_flatten_attempts_exhausted:{reason_label} "
            f"attempts={EOD_FLATTEN_MAX_ATTEMPTS} "
            f"configured_flat_by_time={state.get('configured_flat_by_time')} "
            f"timezone={state.get('timezone')} "
            "operator_action_required=True "
            "lifecycle_state=EOD_FLATTEN_FAILED"
        )
        return False

    def run_session_close_sweep(self, reason_label, target_symbol=None):
        if not SESSION_CLOSE_ENABLED:
            return 0
        symbols = [target_symbol] if target_symbol else self.get_session_close_scope_symbols()
        symbols = [symbol for symbol in symbols if symbol and not self.is_runtime_symbol_disabled(symbol)]
        if not symbols:
            return 0

        logger.warning(
            "SESSION_CLOSE_SWEEP_STARTED | "
            f"job_type=SESSION_CLOSE_SWEEP "
            f"target_symbol={target_symbol} "
            f"symbols={symbols} "
            f"reason={reason_label} "
            f"stage={BOT_STAGE}"
        )

        self.update_session_health()
        if not self.session_socket_connected or not self.session_initialized or not self.session_healthy:
            logger.critical(
                "SESSION_CLOSE_BROKER_DISCONNECTED | "
                f"reason={reason_label} "
                f"session_socket_connected={self.session_socket_connected} "
                f"session_initialized={self.session_initialized} "
                f"session_healthy={self.session_healthy} "
                "operator_action_required=False"
            )
            try:
                logger.warning(
                    "SESSION_CLOSE_RECONNECT_ATTEMPT | "
                    f"reason={reason_label}"
                )
                self.connect_ib()
                logger.warning(
                    "SESSION_CLOSE_RECONNECT_SUCCESS | "
                    f"reason={reason_label}"
                )
                logger.warning(
                    "SESSION_CLOSE_POST_RECONNECT_SWEEP | "
                    f"reason={reason_label}"
                )
            except Exception as exc:
                logger.critical(
                    "SESSION_CLOSE_FLATTEN_FAILED | "
                    "severity=critical "
                    f"symbols={symbols} "
                    f"reason=broker_disconnected_reconnect_failed:{exc} "
                    "operator_action_required=True "
                    "lifecycle_state=EOD_FLATTEN_FAILED"
                )
                return 0

        actions_taken = 0
        for symbol in symbols:
            state = self.get_session_close_state(symbol)
            if not state.get("enabled") or not (
                state.get("no_entry_active")
                or state.get("flatten_window_active")
                or state.get("hard_close_active")
            ):
                continue

            self.session_close_symbol_states[symbol] = "SESSION_CLOSING"
            snapshot = self.read_session_close_broker_snapshot(reason_label, symbol=symbol)
            if state.get("no_entry_active"):
                actions_taken += self.cancel_working_entries_for_session_close(
                    symbol,
                    state,
                    snapshot,
                    reason_label,
                )

            current_position_qty = self.get_position_quantity_from_positions(snapshot.get("positions"), symbol)
            if current_position_qty != 0.0:
                logger.critical(
                    "SESSION_CLOSE_POSITION_DETECTED | "
                    f"symbol={symbol} "
                    f"position_qty={current_position_qty} "
                    f"minutes_to_close={state.get('minutes_to_close')} "
                    f"configured_flat_by_time={state.get('configured_flat_by_time')} "
                    f"timezone={state.get('timezone')} "
                    f"reason={reason_label} "
                    "lifecycle_state=SESSION_CLOSING"
                )

            if state.get("hard_close_active") and current_position_qty != 0.0:
                logger.critical(
                    "SESSION_CLOSE_HARD_FLATTEN_REQUIRED | "
                    f"symbol={symbol} "
                    f"position={current_position_qty} "
                    f"minutes_to_close={state.get('minutes_to_close')} "
                    f"configured_flat_by_time={state.get('configured_flat_by_time')} "
                    f"timezone={state.get('timezone')} "
                    "operator_action_required=False "
                    "reason=session_flat_by_time_reached"
                )

            if (state.get("flatten_window_active") or state.get("hard_close_active")) and current_position_qty != 0.0:
                if self.flatten_symbol_for_session_close(symbol, state, reason_label):
                    actions_taken += 1

        logger.warning(
            "SESSION_CLOSE_SWEEP_COMPLETE | "
            f"job_type=SESSION_CLOSE_SWEEP "
            f"target_symbol={target_symbol} "
            f"symbols={symbols} "
            f"actions_taken={actions_taken} "
            f"reason={reason_label} "
            f"stage={BOT_STAGE}"
        )
        return actions_taken

    def log_webhook_processing_timing(self, status, start_ms, normalized=None, queued=None):
        try:
            payload_format = None
            signal_id = None
            symbol = None
            if isinstance(normalized, dict):
                payload_format = normalized.get("payload_format")
                signal_id = normalized.get("signal_id")
                symbol = normalized.get("symbol")

            duration_ms = self.elapsed_ms(start_ms)
            message = (
                "WEBHOOK PROCESSING TIMING | "
                f"payload_format={payload_format} "
                f"signal_id={signal_id} "
                f"symbol={symbol} "
                f"status={status} "
                f"total_duration_ms={duration_ms} "
                f"queued={queued} "
                f"bot_stage={BOT_STAGE} "
                f"thread_name={threading.current_thread().name} "
                f"thread_ident={threading.get_ident()}"
            )
            logger.info(message)
            if duration_ms is not None and duration_ms >= WEBHOOK_SLOW_MS:
                logger.warning(message.replace("WEBHOOK PROCESSING TIMING |", "WEBHOOK SLOW PATH |", 1))
        except Exception:
            try:
                logger.exception("WEBHOOK PROCESSING TIMING LOG FAILED")
            except Exception:
                pass

        return status

    def log_lifecycle_mutation_context(
        self,
        mutation_point,
        reason_label=None,
        trade_id=None,
        symbol=None,
        previous_state=None,
        next_state=None,
    ):
        try:
            logger.info(
                "LIFECYCLE MUTATION CONTEXT | "
                f"mutation_point={mutation_point} "
                f"reason_label={reason_label} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"previous_state={previous_state} "
                f"next_state={next_state} "
                f"thread_name={threading.current_thread().name} "
                f"thread_ident={threading.get_ident()} "
                f"bot_stage={BOT_STAGE} "
                f"active_execution_trade_id={getattr(self, 'active_execution_trade_id', None)} "
                f"connectivity_uncertain={getattr(self, 'connectivity_uncertain', None)} "
                f"connectivity_reconciliation_required={getattr(self, 'connectivity_reconciliation_required', None)}"
            )
        except Exception:
            try:
                logger.exception("LIFECYCLE MUTATION CONTEXT LOG FAILED")
            except Exception:
                pass

    def get_effective_execution_time(self, fill=None, execution=None):
        execution_time = getattr(execution, "time", None) if execution is not None else None
        callback_fill_time = getattr(fill, "time", None) if fill is not None else None
        return execution_time or callback_fill_time, execution_time, callback_fill_time

    def is_legacy_trade_sequence(self, value):
        text = str(value or "").strip().upper()
        return len(text) == 7 and text.startswith("T") and text[1:].isdigit()

    def format_trade_sequence(self, sequence):
        if self.is_legacy_trade_sequence(sequence):
            return str(sequence).strip().upper()
        return f"T{int(sequence):06d}"

    def build_trade_id(self, trade_sequence):
        return f"{self.run_id}-{self.format_trade_sequence(trade_sequence)}"

    def parse_run_unique_trade_id(self, trade_id):
        text = str(trade_id or "").strip()
        run_id, separator, sequence_number = text.rpartition("-T")
        if not separator or not run_id:
            return None, None
        trade_seq = f"T{sequence_number}"
        if not self.is_legacy_trade_sequence(trade_seq):
            return None, None
        return run_id, trade_seq

    def parse_order_ref(self, order_ref):
        result = {
            "raw_order_ref": order_ref,
            "is_valid": False,
            "is_legacy": False,
            "run_id": None,
            "trade_seq": None,
            "trade_id": None,
            "symbol": None,
            "leg_role": None,
            "parse_error": None,
        }
        if order_ref is None:
            result["parse_error"] = "missing_order_ref"
            return result

        order_ref_text = str(order_ref).strip()
        result["raw_order_ref"] = order_ref_text
        if not order_ref_text:
            result["parse_error"] = "empty_order_ref"
            return result

        parts = [part.strip() for part in order_ref_text.split("|")]
        if len(parts) < 3:
            result["parse_error"] = "order_ref_missing_parts"
            return result

        trade_identity = parts[0]
        symbol = parts[1].upper() if parts[1] else None
        leg_role = parts[2].upper() if parts[2] else None
        result["symbol"] = symbol
        result["leg_role"] = leg_role

        run_id, trade_seq = self.parse_run_unique_trade_id(trade_identity)
        if run_id and trade_seq:
            result.update({
                "is_valid": True,
                "is_legacy": False,
                "run_id": run_id,
                "trade_seq": trade_seq,
                "trade_id": trade_identity,
            })
            return result

        if self.is_legacy_trade_sequence(trade_identity):
            result.update({
                "is_valid": True,
                "is_legacy": True,
                "trade_seq": trade_identity.upper(),
                "trade_id": trade_identity.upper(),
                "parse_error": "legacy_order_ref",
            })
            return result

        result.update({
            "is_legacy": True,
            "trade_id": trade_identity or None,
            "parse_error": "unknown_trade_identity_format",
        })
        return result

    # P189/P212: compact broker-side correlation for missed/late fills, now run-unique.
    def build_order_ref(self, trade_id, symbol, role):
        normalized_trade_id = str(trade_id or "").strip()
        if self.is_legacy_trade_sequence(normalized_trade_id):
            normalized_trade_id = self.build_trade_id(normalized_trade_id)
        return f"{normalized_trade_id}|{str(symbol).upper()}|{str(role).upper()}"

    def get_record_order_refs(self, record):
        if not isinstance(record, dict):
            return set()
        return {
            str(order_ref)
            for order_ref in (
                record.get("order_ref_entry"),
                record.get("order_ref_tp"),
                record.get("order_ref_sl"),
                record.get("emergency_flatten_order_ref"),
            )
            if order_ref
        }

    def record_allows_reconstructed_broker_identity(self, record):
        if not isinstance(record, dict):
            return False
        return bool(
            record.get("reconstructed_from_startup")
            or record.get("broker_identity_scope") in {
                "startup_reconstructed_legacy",
                "startup_reconstructed_foreign",
            }
        )

    def get_expected_order_ref_roles_for_execution(self, record, execution):
        order_id = getattr(execution, "orderId", None)
        perm_id = getattr(execution, "permId", None)
        roles = set()
        if order_id == record.get("parent_order_id") or (
            perm_id not in (None, 0) and perm_id == record.get("parent_perm_id")
        ):
            roles.add("ENTRY")
        if order_id == record.get("tp_order_id") or (
            perm_id not in (None, 0) and perm_id == record.get("tp_perm_id")
        ):
            roles.add("TP")
        if order_id == record.get("sl_order_id") or (
            perm_id not in (None, 0) and perm_id == record.get("sl_perm_id")
        ):
            roles.add("SL")
        if order_id == record.get("emergency_flatten_order_id") or (
            perm_id not in (None, 0) and perm_id == record.get("emergency_flatten_perm_id")
        ):
            roles.add("PROTECTIVE_EMERGENCY_FLATTEN")
        return roles

    def log_order_ref_match_decision(self, label, record, parsed, reason, execution=None, decision=None):
        try:
            logger.warning(
                f"{label} | "
                f"trade_id={(record or {}).get('trade_id')} "
                f"record_run_id={(record or {}).get('run_id')} "
                f"current_run_id={self.run_id} "
                f"raw_order_ref={parsed.get('raw_order_ref') if isinstance(parsed, dict) else None} "
                f"parsed_run_id={parsed.get('run_id') if isinstance(parsed, dict) else None} "
                f"parsed_trade_id={parsed.get('trade_id') if isinstance(parsed, dict) else None} "
                f"parsed_symbol={parsed.get('symbol') if isinstance(parsed, dict) else None} "
                f"parsed_leg_role={parsed.get('leg_role') if isinstance(parsed, dict) else None} "
                f"order_id={getattr(execution, 'orderId', None)} "
                f"perm_id={getattr(execution, 'permId', None)} "
                f"reason={reason} "
                f"decision={decision}"
            )
        except Exception:
            logger.exception("ORDER_REF_MATCH_DECISION_LOG_FAILED")

    def execution_account_matches_record(self, record, execution):
        execution_account = (
            getattr(execution, "acctNumber", None)
            or getattr(execution, "account", None)
            or getattr(execution, "acct", None)
        )
        if not execution_account:
            return True
        expected_accounts = {
            record.get("account") if isinstance(record, dict) else None,
            record.get("broker_account") if isinstance(record, dict) else None,
            BOT_ACCOUNT_CONTEXT,
        }
        normalized_expected = {
            str(value).strip()
            for value in expected_accounts
            if value not in (None, "", "default")
        }
        if not normalized_expected:
            return True
        return str(execution_account).strip() in normalized_expected

    def execution_order_ref_matches_record(self, record, execution, contract=None):
        order_ref = self.get_order_ref(execution=execution)
        parsed = self.parse_order_ref(order_ref)
        expected_roles = self.get_expected_order_ref_roles_for_execution(record, execution)
        reconstructed_identity = self.record_allows_reconstructed_broker_identity(record)
        known_record_order_refs = self.get_record_order_refs(record)
        raw_order_ref = str(order_ref).strip() if order_ref is not None else None

        if not raw_order_ref:
            if reconstructed_identity and expected_roles:
                return True, "missing_order_ref_allowed_for_startup_reconstructed_exact_match", parsed
            contract_ok = contract is None or self.contract_fallback_matches_record(contract, record)
            account_ok = self.execution_account_matches_record(record, execution)
            if expected_roles and contract_ok and account_ok:
                self.log_order_ref_match_decision(
                    "BROKER_FILL_WITH_MISSING_ORDER_REF_ACCEPTED",
                    record,
                    parsed,
                    "missing_order_ref_exact_order_or_perm_match",
                    execution=execution,
                    decision="accept_current_lifecycle_match",
                )
                return True, "missing_order_ref_exact_order_or_perm_match", parsed
            self.log_order_ref_match_decision(
                "BROKER_FILL_WITH_MISSING_ORDER_REF",
                record,
                parsed,
                (
                    "missing_order_ref_contract_mismatch"
                    if expected_roles and not contract_ok
                    else "missing_order_ref_account_mismatch"
                    if expected_roles and not account_ok
                    else "missing_order_ref"
                ),
                execution=execution,
                decision="reject_current_lifecycle_match",
            )
            return False, (
                "missing_order_ref_contract_mismatch"
                if expected_roles and not contract_ok
                else "missing_order_ref_account_mismatch"
                if expected_roles and not account_ok
                else "missing_order_ref"
            ), parsed

        if reconstructed_identity and (
            raw_order_ref in known_record_order_refs
            or (expected_roles and (parsed.get("is_legacy") or parsed.get("run_id") != self.run_id))
        ):
            self.log_order_ref_match_decision(
                "LEGACY_ORDER_REF_DETECTED",
                record,
                parsed,
                parsed.get("parse_error") or "startup_reconstructed_broker_identity",
                execution=execution,
                decision="allow_startup_reconstructed_exact_match",
            )
            return True, "startup_reconstructed_broker_identity", parsed

        if not parsed.get("is_valid"):
            self.log_order_ref_match_decision(
                "LEGACY_ORDER_REF_DETECTED",
                record,
                parsed,
                parsed.get("parse_error") or "invalid_order_ref",
                execution=execution,
                decision="reject_current_lifecycle_match",
            )
            return False, parsed.get("parse_error") or "invalid_order_ref", parsed

        if parsed.get("is_legacy"):
            self.log_order_ref_match_decision(
                "LEGACY_ORDER_REF_DETECTED",
                record,
                parsed,
                "legacy_order_ref_current_run_rejected",
                execution=execution,
                decision="reject_current_lifecycle_match",
            )
            return False, "legacy_order_ref_current_run_rejected", parsed

        if parsed.get("run_id") != self.run_id:
            self.log_order_ref_match_decision(
                "ORDER_REF_RUN_ID_MISMATCH",
                record,
                parsed,
                "foreign_run_id",
                execution=execution,
                decision="reject_current_lifecycle_match",
            )
            return False, "foreign_run_id", parsed

        if parsed.get("trade_id") != record.get("trade_id"):
            self.log_order_ref_match_decision(
                "ORDER_REF_TRADE_ID_MISMATCH",
                record,
                parsed,
                "trade_id_mismatch",
                execution=execution,
                decision="reject_current_lifecycle_match",
            )
            return False, "trade_id_mismatch", parsed

        record_symbol = str(record.get("symbol") or "").upper()
        if parsed.get("symbol") != record_symbol:
            self.log_order_ref_match_decision(
                "ORDER_REF_TRADE_ID_MISMATCH",
                record,
                parsed,
                "symbol_mismatch",
                execution=execution,
                decision="reject_current_lifecycle_match",
            )
            return False, "symbol_mismatch", parsed

        if expected_roles and parsed.get("leg_role") not in expected_roles:
            self.log_order_ref_match_decision(
                "ORDER_REF_TRADE_ID_MISMATCH",
                record,
                parsed,
                f"leg_role_mismatch expected_roles={sorted(expected_roles)}",
                execution=execution,
                decision="reject_current_lifecycle_match",
            )
            return False, "leg_role_mismatch", parsed

        logger.info(
            "ORDER_REF_PARSED | "
            f"trade_id={record.get('trade_id')} "
            f"run_id={record.get('run_id')} "
            f"current_run_id={self.run_id} "
            f"raw_order_ref={parsed.get('raw_order_ref')} "
            f"parsed_run_id={parsed.get('run_id')} "
            f"parsed_trade_id={parsed.get('trade_id')} "
            f"symbol={parsed.get('symbol')} "
            f"leg_role={parsed.get('leg_role')} "
            "decision=accept_current_run_match"
        )
        return True, "current_run_order_ref_match", parsed

    def broker_order_ref_matches_record(self, record, order_ref):
        if not order_ref or not isinstance(record, dict):
            return False
        raw_order_ref = str(order_ref).strip()
        if raw_order_ref in self.get_record_order_refs(record):
            return True
        parsed = self.parse_order_ref(raw_order_ref)
        return bool(
            parsed.get("is_valid")
            and not parsed.get("is_legacy")
            and parsed.get("run_id") == self.run_id
            and parsed.get("trade_id") == record.get("trade_id")
            and parsed.get("symbol") == str(record.get("symbol") or "").upper()
        )

    def get_order_ref(self, order=None, execution=None):
        order_ref = getattr(order, "orderRef", None) if order is not None else None
        if order_ref:
            return order_ref
        return getattr(execution, "orderRef", None) if execution is not None else None

    def format_broker_identity_fields(self, trade=None, fill=None, execution=None, contract=None, order=None):
        if execution is None and fill is not None:
            execution = getattr(fill, "execution", None)
        if contract is None:
            contract = (
                getattr(trade, "contract", None)
                if trade is not None
                else None
            ) or (getattr(fill, "contract", None) if fill is not None else None)
        if order is None and trade is not None:
            order = getattr(trade, "order", None)

        fill_time, execution_time, callback_fill_time = self.get_effective_execution_time(fill, execution)
        return (
            f"orderId={getattr(execution, 'orderId', None)} "
            f"permId={getattr(execution, 'permId', None)} "
            f"clientId={getattr(execution, 'clientId', None)} "
            f"execId={getattr(execution, 'execId', None)} "
            f"side={getattr(execution, 'side', None)} "
            f"quantity={getattr(execution, 'shares', None)} "
            f"price={getattr(execution, 'price', None)} "
            f"time={self.to_iso(fill_time)} "
            f"execution_time={self.to_iso(execution_time)} "
            f"callback_fill_time={self.to_iso(callback_fill_time)} "
            f"symbol={getattr(contract, 'symbol', None)} "
            f"localSymbol={getattr(contract, 'localSymbol', None)} "
            f"tradingClass={getattr(contract, 'tradingClass', None)} "
            f"conId={getattr(contract, 'conId', None)} "
            f"exchange={getattr(contract, 'exchange', None)} "
            f"currency={getattr(contract, 'currency', None)} "
            f"secType={getattr(contract, 'secType', None)} "
            f"orderRef={self.get_order_ref(order, execution)}"
        )

    def log_broker_fill_without_lifecycle_event(
        self,
        trade_id,
        possible_match_reason,
        reason_not_applied,
        trade=None,
        fill=None,
        execution=None,
        contract=None,
        order=None,
    ):
        try:
            logger.warning(
                "BROKER_FILL_WITHOUT_LIFECYCLE_EVENT | "
                f"trade_id={trade_id or 'UNKNOWN'} "
                f"possible_match_reason={possible_match_reason} "
                f"reason_not_applied={reason_not_applied} "
                f"{self.format_broker_identity_fields(trade, fill, execution, contract, order)}"
            )
        except Exception:
            try:
                logger.exception("BROKER FILL WITHOUT LIFECYCLE EVENT LOG FAILED")
            except Exception:
                pass

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
        logger.info(
            f"DIAGNOSTIC PAYLOAD | {json.dumps(redact_sensitive_payload(data), sort_keys=True)}"
        )

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
        order_id = getattr(execution, "orderId", None)
        trade_id, _ = self.get_trade_by_order_id(order_id)

        with self.trade_analysis_lock:
            if execution_identity in self.processed_execution_ids:
                logger.info(
                    "DUPLICATE EXECUTION DETECTED | "
                    f"execution_identity={execution_identity} "
                    f"order_id={order_id} "
                    f"trade_id={trade_id}"
                )
                return False, execution_identity
            self.processed_execution_ids.add(execution_identity)

        return True, execution_identity

    def map_order_leg(self, record, order_id):
        if record is None:
            return "UNKNOWN"
        if order_id == record.get("parent_order_id"):
            return "ENTRY"
        if order_id == record.get("tp_order_id"):
            return "TAKE_PROFIT"
        if order_id == record.get("sl_order_id"):
            return "STOP_LOSS"
        if order_id == record.get("emergency_flatten_order_id"):
            if record.get("time_exit_order_id") == order_id or record.get("emergency_flatten_reason") == TIME_EXIT_REASON:
                return "TIME_EXIT"
            return "EMERGENCY_FLATTEN_EXIT"
        return "UNKNOWN"

    # P175-A/P175-B/P175-C/P175-D/P175-E/P175-F/P176: observability-only helpers; these do not change lifecycle behavior.
    def map_observability_order_leg(self, record, order_id=None, perm_id=None):
        if record is None:
            return "UNKNOWN"

        if order_id is not None:
            if order_id == record.get("parent_order_id"):
                return "ENTRY"
            if order_id == record.get("tp_order_id"):
                return "TP"
            if order_id == record.get("sl_order_id"):
                return "SL"
            if (
                order_id == record.get("emergency_flatten_order_id")
                or (
                    perm_id not in (None, 0)
                    and perm_id == record.get("emergency_flatten_perm_id")
                )
            ):
                if record.get("time_exit_order_id") == order_id or record.get("emergency_flatten_reason") == TIME_EXIT_REASON:
                    return "TIME_EXIT"
                return "EMERGENCY_FLATTEN_EXIT"

        if perm_id not in (None, 0):
            if perm_id == record.get("parent_perm_id"):
                return "ENTRY"
            if perm_id == record.get("tp_perm_id"):
                return "TP"
            if perm_id == record.get("sl_perm_id"):
                return "SL"
            if perm_id == record.get("emergency_flatten_perm_id"):
                if record.get("emergency_flatten_reason") == TIME_EXIT_REASON:
                    return "TIME_EXIT"
                return "EMERGENCY_FLATTEN_EXIT"

        return "UNKNOWN"

    def resolve_observability_trade_context(self, order_id=None, perm_id=None):
        with self.trade_analysis_lock:
            if order_id is not None:
                trade_id = self.order_to_trade.get(order_id)
                if trade_id is not None:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        return {
                            "trade_id": trade_id,
                            "record": record,
                            "leg": self.map_observability_order_leg(record, order_id=order_id, perm_id=perm_id),
                            "match_source": "order_id",
                            "match_reason": "matched_existing_order_to_trade_metadata",
                        }

            if perm_id not in (None, 0):
                for trade_id, record in self.trade_analysis.items():
                    if perm_id in (
                        record.get("parent_perm_id"),
                        record.get("tp_perm_id"),
                        record.get("sl_perm_id"),
                    ):
                        return {
                            "trade_id": trade_id,
                            "record": record,
                            "leg": self.map_observability_order_leg(record, order_id=order_id, perm_id=perm_id),
                            "match_source": "perm_id",
                            "match_reason": "matched_existing_perm_to_trade_metadata",
                        }

        if order_id is None and perm_id in (None, 0):
            match_reason = "missing_order_id_and_perm_id"
        else:
            match_reason = "no_existing_trade_metadata_for_order_or_perm"

        return {
            "trade_id": "UNKNOWN",
            "record": None,
            "leg": "UNKNOWN",
            "match_source": "none",
            "match_reason": match_reason,
        }

    def log_order_status_event_observability(self, trade):
        try:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            contract = getattr(trade, "contract", None)
            order_id = getattr(order, "orderId", None)
            perm_id = getattr(status, "permId", None) or getattr(order, "permId", None)
            context = self.resolve_observability_trade_context(order_id=order_id, perm_id=perm_id)

            logger.info(
                "ORDER STATUS EVENT | "
                f"trade_id={context['trade_id']} "
                f"leg={context['leg']} "
                f"orderId={order_id} "
                f"permId={perm_id} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"orderRef={self.get_order_ref(order=order)} "
                f"status={getattr(status, 'status', None)} "
                f"filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} "
                f"avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"lastFillPrice={getattr(status, 'lastFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)} "
                f"symbol={getattr(contract, 'symbol', None)} "
                f"localSymbol={getattr(contract, 'localSymbol', None)} "
                f"conId={getattr(contract, 'conId', None)} "
                f"match_source={context['match_source']} "
                f"match_reason={context['match_reason']}"
            )
        except Exception:
            logger.exception("ORDER STATUS EVENT OBSERVABILITY LOG FAILED")

    def log_exec_details_event_observability(self, trade, fill):
        try:
            execution = getattr(fill, "execution", None)
            order = getattr(trade, "order", None)
            contract = getattr(trade, "contract", None) or getattr(fill, "contract", None)
            order_id = getattr(execution, "orderId", None)
            perm_id = getattr(execution, "permId", None)
            context = self.resolve_observability_trade_context(order_id=order_id, perm_id=perm_id)

            logger.info(
                "EXEC DETAILS EVENT | "
                f"trade_id={context['trade_id']} "
                f"leg={context['leg']} "
                f"orderId={order_id} "
                f"permId={perm_id} "
                f"clientId={getattr(execution, 'clientId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"orderRef={self.get_order_ref(order, execution)} "
                f"execId={getattr(execution, 'execId', None)} "
                f"side={getattr(execution, 'side', None)} "
                f"shares={getattr(execution, 'shares', None)} "
                f"price={getattr(execution, 'price', None)} "
                f"time={self.to_iso(getattr(execution, 'time', None) or getattr(fill, 'time', None))} "
                f"symbol={getattr(contract, 'symbol', None)} "
                f"localSymbol={getattr(contract, 'localSymbol', None)} "
                f"conId={getattr(contract, 'conId', None)} "
                f"match_source={context['match_source']} "
                f"match_reason={context['match_reason']}"
            )
            if context["trade_id"] == "UNKNOWN":
                logger.warning(
                    "EXEC DETAILS UNKNOWN | "
                    f"match_source={context['match_source']} "
                    f"match_reason={context['match_reason']} "
                    f"{self.format_broker_identity_fields(trade, fill, execution, contract, order)}"
                )
        except Exception:
            logger.exception("EXEC DETAILS EVENT OBSERVABILITY LOG FAILED")

    def log_open_order_event_observability(self, trade):
        try:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            contract = getattr(trade, "contract", None)
            order_id = getattr(order, "orderId", None)
            perm_id = getattr(status, "permId", None) or getattr(order, "permId", None)
            context = self.resolve_observability_trade_context(order_id=order_id, perm_id=perm_id)

            logger.info(
                "OPEN ORDER EVENT | "
                f"trade_id={context['trade_id']} "
                f"leg={context['leg']} "
                f"orderId={order_id} "
                f"permId={perm_id} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"orderRef={self.get_order_ref(order=order)} "
                f"action={getattr(order, 'action', None)} "
                f"orderType={getattr(order, 'orderType', None)} "
                f"totalQuantity={getattr(order, 'totalQuantity', None)} "
                f"lmtPrice={getattr(order, 'lmtPrice', None)} "
                f"auxPrice={getattr(order, 'auxPrice', None)} "
                f"transmit={getattr(order, 'transmit', None)} "
                f"tif={getattr(order, 'tif', None)} "
                f"outsideRth={getattr(order, 'outsideRth', None)} "
                f"ocaGroup={getattr(order, 'ocaGroup', None)} "
                f"ocaType={getattr(order, 'ocaType', None)} "
                f"status={getattr(status, 'status', None)} "
                f"filled={getattr(status, 'filled', None)} "
                f"remaining={getattr(status, 'remaining', None)} "
                f"avgFillPrice={getattr(status, 'avgFillPrice', None)} "
                f"whyHeld={getattr(status, 'whyHeld', None)} "
                f"symbol={getattr(contract, 'symbol', None)} "
                f"localSymbol={getattr(contract, 'localSymbol', None)} "
                f"conId={getattr(contract, 'conId', None)} "
                f"match_source={context['match_source']} "
                f"match_reason={context['match_reason']}"
            )
        except Exception:
            logger.exception("OPEN ORDER EVENT OBSERVABILITY LOG FAILED")

    def get_record_order_perm_id(self, record, order_id):
        if record is None:
            return None
        if order_id == record.get("parent_order_id"):
            return record.get("parent_perm_id")
        if order_id == record.get("tp_order_id"):
            return record.get("tp_perm_id")
        if order_id == record.get("sl_order_id"):
            return record.get("sl_perm_id")
        if order_id == record.get("emergency_flatten_order_id"):
            return record.get("emergency_flatten_perm_id")
        return None

    def log_fill_event(
        self,
        record,
        order_id,
        perm_id,
        exec_id,
        leg,
        side,
        price,
        quantity,
        timestamp,
        callback_fill_time=None,
    ):
        if record is None:
            return

        logger.info(
            "FILL EVENT | "
            f"trade_id={record.get('trade_id')} "
            f"symbol={record.get('symbol')} "
            f"order_id={order_id} "
            f"perm_id={perm_id} "
            f"exec_id={exec_id} "
            f"leg={leg} "
            f"side={side} "
            f"price={price} "
            f"quantity={quantity} "
            f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
            f"cumulative_exit_quantity={record.get('cumulative_exit_quantity')} "
            f"entry_filled={record.get('entry_filled')} "
            f"state={record.get('state')} "
            f"execution_time={self.to_iso(timestamp)} "
            f"callback_fill_time={self.to_iso(callback_fill_time)}"
        )

        logger.info(
            "EXECUTION CORRELATION | "
            f"trade_id={record.get('trade_id')} "
            f"order_id={order_id} "
            f"perm_id={perm_id} "
            f"exec_id={exec_id} "
            f"mapped_leg={leg} "
            f"state={record.get('state')}"
        )

    def log_order_status_correlation(
        self,
        record,
        order_id,
        perm_id,
        leg,
        status,
        filled,
        remaining,
        avg_fill_price,
        action,
        timestamp,
    ):
        if record is None:
            return

        logger.info(
            "ORDER STATUS CORRELATION | "
            "source=orderStatusEvent "
            f"trade_id={record.get('trade_id')} "
            f"symbol={record.get('symbol')} "
            f"order_id={order_id} "
            f"perm_id={perm_id} "
            f"leg={leg} "
            f"action={action} "
            f"status={status} "
            f"filled={filled} "
            f"remaining={remaining} "
            f"avg_fill_price={avg_fill_price} "
            f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
            f"cumulative_exit_quantity={record.get('cumulative_exit_quantity')} "
            f"entry_filled={record.get('entry_filled')} "
            f"state={record.get('state')} "
            f"timestamp={self.to_iso(timestamp)}"
        )

    def log_commission_correlation(
        self,
        record,
        order_id,
        perm_id,
        exec_id,
        leg,
        side,
        price,
        quantity,
        commission,
        currency,
        realized_pnl,
        timestamp,
    ):
        if record is None:
            return

        logger.info(
            "COMMISSION CORRELATION | "
            "source=commissionReportEvent "
            f"trade_id={record.get('trade_id')} "
            f"symbol={record.get('symbol')} "
            f"order_id={order_id} "
            f"perm_id={perm_id} "
            f"exec_id={exec_id} "
            f"leg={leg} "
            f"side={side} "
            f"price={price} "
            f"quantity={quantity} "
            f"commission={commission} "
            f"currency={currency} "
            f"realized_pnl={realized_pnl} "
            f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
            f"cumulative_exit_quantity={record.get('cumulative_exit_quantity')} "
            f"entry_filled={record.get('entry_filled')} "
            f"state={record.get('state')} "
            f"timestamp={self.to_iso(timestamp)}"
        )

    def log_fill_timing(self, record, fill_time):
        if record is None:
            return

        logger.info(
            "FILL TIMING | "
            f"trade_id={record.get('trade_id')} "
            f"submit_to_fill_sec={self.seconds_between(record.get('bracket_submit_start_time'), fill_time)} "
            f"webhook_to_fill_sec={self.seconds_between(record.get('webhook_received_time'), fill_time)} "
            f"entry_to_exit_sec={self.seconds_between(record.get('entry_fill_time'), record.get('exit_fill_time'))}"
        )

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
                "dry_run_planned_count": 0,
                "dry_run_not_transmitted_count": 0,
                "filled_trades": 0,
                "tp_count": 0,
                "sl_count": 0,
                "emergency_flatten_count": 0,
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
                stats.get("emergency_flatten_count", 0) > 0 or
                stats["gross_pnl"] != 0.0 or
                stats["commission"] != 0.0 or
                stats["net_pnl"] != 0.0
            ):
                snapshot[symbol] = {
                    "total": stats["total_trades"],
                    "filled": stats["filled_trades"],
                    "tp": stats["tp_count"],
                    "sl": stats["sl_count"],
                    "emergency_flatten": stats.get("emergency_flatten_count", 0),
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
        trade_seq = self.format_trade_sequence(self.trade_seq)
        trade_id = self.build_trade_id(trade_seq)
        logger.info(
            "TRADE_ID_ALLOCATED | "
            f"run_id={self.run_id} "
            f"trade_seq={trade_seq} "
            f"trade_id={trade_id} "
            f"symbol={job.get('symbol')}"
        )

        record = {
            "run_id": self.run_id,
            "trade_seq": trade_seq,
            "trade_id": trade_id,
            "bot_stage": job.get("bot_stage", BOT_STAGE),
            "state": "SUBMITTING",
            "symbol": job["symbol"],
            "contract_con_id": job.get("contract_con_id"),
            "contract_local_symbol": job.get("contract_local_symbol"),
            "contract_trading_class": job.get("contract_trading_class"),
            "contract_exchange": job.get("contract_exchange"),
            "contract_currency": job.get("contract_currency"),
            "contract_sec_type": job.get("contract_sec_type"),
            "side": job["side"],
            "grade": job.get("grade", ""),
            "truth_classification": job.get("truth_classification", ""),
            "det_classification": job.get("det_classification", ""),
            "primary_reason": job.get("primary_reason", ""),
            "signal_id": job.get("signal_id"),
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
            "bracket_submit_transaction_status": "not_started",
            "bracket_submit_exception": None,
            "bracket_submit_uncertain": False,
            "bracket_submit_failure_handled_at": None,
            "parent_submit_attempted": False,
            "tp_submit_attempted": False,
            "sl_submit_attempted": False,
            "parent_submit_completed": False,
            "tp_submit_completed": False,
            "sl_submit_completed": False,
            "parent_submit_attempted_at": None,
            "tp_submit_attempted_at": None,
            "sl_submit_attempted_at": None,
            "parent_submit_completed_at": None,
            "tp_submit_completed_at": None,
            "sl_submit_completed_at": None,
            "parent_submit_perm_id": None,
            "tp_submit_perm_id": None,
            "sl_submit_perm_id": None,
            "entry_signal_price": job["entry"],
            "entry_spread_adjusted": spread_adjusted_entry,
            "entry_price": entry,
            "stop_price": stop,
            "target_price": target,
            "parent_order_id": parent_id,
            "tp_order_id": tp_id,
            "sl_order_id": sl_id,
            "order_ref_entry": None,
            "order_ref_tp": None,
            "order_ref_sl": None,
            "emergency_flatten_order_ref": None,
            "broker_identity_scope": "current_run",
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
            "risk_intent_profile": job.get("risk_intent_profile"),
            "risk_intent_percent": job.get("risk_intent_percent"),
            "risk_intent_status": job.get("risk_intent_status"),
            "risk_intent_reason": job.get("risk_intent_reason"),
            "theoretical_size_before_containment": job.get("theoretical_size_before_containment"),
            "containment_status": job.get("containment_status"),
            "containment_stage": job.get("containment_stage"),
            "containment_profile": job.get("containment_profile"),
            "containment_max_size": job.get("containment_max_size"),
            "containment_max_notional": job.get("containment_max_notional"),
            "containment_hierarchy": job.get("containment_hierarchy"),
            "containment_decision_source": job.get("containment_decision_source"),
            "containment_denial_source": job.get("containment_denial_source"),
            "triggering_containment_rule": job.get("triggering_containment_rule"),
            "notional_model": job.get("notional_model"),
            "notional_currency": job.get("notional_currency"),
            "capped_size_after_size_containment": job.get("capped_size_after_size_containment"),
            "estimated_notional_before_containment": job.get("estimated_notional_before_containment"),
            "estimated_notional_after_size_containment": job.get("estimated_notional_after_size_containment"),
            "estimated_notional_after_containment": job.get("estimated_notional_after_containment"),
            "containment_action": job.get("containment_action"),
            "containment_reason": job.get("containment_reason"),
            "approved_size_after_containment": job.get("approved_size_after_containment"),
            "planned_parent_quantity": job.get("planned_parent_quantity"),
            "planned_tp_quantity": job.get("planned_tp_quantity"),
            "planned_sl_quantity": job.get("planned_sl_quantity"),
            "intended_parent_quantity": float(
                job.get("approved_size_after_containment") or 0.0
            ),
            "original_intended_parent_quantity": float(
                job.get("approved_size_after_containment") or 0.0
            ),
            "planned_position_size": job.get("approved_size_after_containment"),
            "position_size": job.get("approved_size_after_containment"),
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
            "reconciliation_log_suppression": {},
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
            "entry_exposure_started_at": None,
            "time_exit_deadline_at": None,
            "time_exit_status": None,
            "time_exit_reason": None,
            "time_exit_order_id": None,
            "time_exit_attempt_count": 0,
            "time_exit_last_attempt_at": None,
            "time_exit_completed_at": None,
            "parent_last_status": None,
            "execution_validation_status": "submitted_to_ib",
            "dry_run": False,
            "transmitted": True,
            "transmission_mode": "live",
            "transmission_block_reason": None,
            "broker_order_ids_transmitted": [],
            "broker_acknowledged_at": None,
            "broker_live_at": None,
            "exit_cleanup_last_snapshot": None,
            "timeout_retained_replacement_parentless_allowed": False,
            "protective_emergency_active": False,
            "protective_emergency_reason": None,
            "protective_emergency_status": None,
            "protective_emergency_started_at": None,
            "emergency_flatten_submit_reserved_at": None,
            "emergency_flatten_submit_reservation_id": None,
            "emergency_flatten_submitted_at": None,
            "emergency_flatten_last_status": None,
            "emergency_flatten_terminal_status": None,
            "emergency_flatten_order_id": None,
            "emergency_flatten_perm_id": None,
            "emergency_flatten_action": None,
            "emergency_flatten_quantity": None,
            "emergency_flatten_attempt": None,
            "emergency_flatten_reason": None,
            "emergency_flatten_exit_quantity": 0.0,
            "emergency_flatten_exit_notional": 0.0,
            "emergency_flatten_exit_fill_price": None,
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

    def update_bracket_submit_transaction(self, trade_id, **fields):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return None
            record.update(fields)
            return dict(record)

    def mark_bracket_submit_leg_attempted(self, trade_id, leg_name):
        if leg_name not in {"parent", "tp", "sl"}:
            return None
        attempted_at = datetime.now(timezone.utc)
        snapshot = self.update_bracket_submit_transaction(
            trade_id,
            bracket_submit_transaction_status="in_progress",
            **{
                f"{leg_name}_submit_attempted": True,
                f"{leg_name}_submit_attempted_at": attempted_at,
            },
        )
        self.append_trade_event(
            trade_id,
            f"BRACKET SUBMIT LEG ATTEMPTED leg={leg_name}",
        )
        return snapshot

    def mark_bracket_submit_leg_completed(self, trade_id, leg_name, trade=None):
        if leg_name not in {"parent", "tp", "sl"}:
            return None

        order = getattr(trade, "order", None)
        status = getattr(trade, "orderStatus", None)
        perm_id = getattr(status, "permId", None) or getattr(order, "permId", None)
        completed_at = datetime.now(timezone.utc)
        snapshot = self.update_bracket_submit_transaction(
            trade_id,
            **{
                f"{leg_name}_submit_completed": True,
                f"{leg_name}_submit_completed_at": completed_at,
                f"{leg_name}_submit_perm_id": perm_id,
            },
        )
        if order is not None:
            self.update_trade_perm_id(getattr(order, "orderId", None), perm_id)
        self.append_trade_event(
            trade_id,
            f"BRACKET SUBMIT LEG COMPLETED leg={leg_name} perm_id={perm_id}",
        )
        return snapshot

    def get_bracket_submit_context_from_record(self, record):
        return {
            "parent_order_id": record.get("parent_order_id"),
            "tp_order_id": record.get("tp_order_id"),
            "sl_order_id": record.get("sl_order_id"),
            "parent_submit_attempted": record.get("parent_submit_attempted"),
            "tp_submit_attempted": record.get("tp_submit_attempted"),
            "sl_submit_attempted": record.get("sl_submit_attempted"),
            "parent_submit_completed": record.get("parent_submit_completed"),
            "tp_submit_completed": record.get("tp_submit_completed"),
            "sl_submit_completed": record.get("sl_submit_completed"),
        }

    def handle_bracket_submission_failure(self, trade_id, record_snapshot, submit_context, exc):
        symbol = (record_snapshot or {}).get("symbol")
        failure_text = str(exc)
        now_dt = datetime.now(timezone.utc)

        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is not None:
                previous_state = record.get("state")
                record["state"] = "BRACKET_SUBMIT_FAILED_UNCERTAIN"
                record["bracket_submit_transaction_status"] = "failed_uncertain"
                record["bracket_submit_exception"] = failure_text
                record["bracket_submit_uncertain"] = True
                record["bracket_submit_failure_handled_at"] = now_dt
                record["broker_ack_pending_since"] = record.get("broker_ack_pending_since") or now_dt
                record["broker_ack_pending_last_check"] = now_dt
                record["broker_ack_pending_reason"] = "bracket_submit_failed_uncertain"
                record["broker_ack_pending_category"] = "SUBMISSION_UNCERTAIN"
                record["execution_validation_status"] = "validation_incomplete"
                if "BRACKET_SUBMIT_PARTIAL_FAILURE" not in record["anomalies"]:
                    record["anomalies"].append("BRACKET_SUBMIT_PARTIAL_FAILURE")
                self.append_trade_event(
                    trade_id,
                    f"BRACKET SUBMIT FAILED UNCERTAIN previous_state={previous_state} exception={failure_text}",
                )
                record_snapshot = dict(record)

        self.external_entries_blocked_reason = "bracket_submit_failed_uncertain"
        context = submit_context or self.get_bracket_submit_context_from_record(record_snapshot or {})
        logger.critical(
            "BRACKET_SUBMISSION_FAILED_UNCERTAIN | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"parent_order_id={context.get('parent_order_id')} "
            f"tp_order_id={context.get('tp_order_id')} "
            f"sl_order_id={context.get('sl_order_id')} "
            f"parent_submit_attempted={context.get('parent_submit_attempted')} "
            f"tp_submit_attempted={context.get('tp_submit_attempted')} "
            f"sl_submit_attempted={context.get('sl_submit_attempted')} "
            f"parent_submit_completed={context.get('parent_submit_completed')} "
            f"tp_submit_completed={context.get('tp_submit_completed')} "
            f"sl_submit_completed={context.get('sl_submit_completed')} "
            f"exception={failure_text} "
            "decision=inspect_broker_reality_fail_closed"
        )

        broker_reality = None
        try:
            broker_reality = self.get_trade_broker_reality(
                record_snapshot,
                trade_analysis_lock_context="not_locked",
            )
        except Exception as reality_exc:
            logger.exception(
                "BRACKET_SUBMISSION_FAILURE_REALITY_CHECK_EXCEPTION | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"failure={reality_exc}"
            )

        confirmation = self.assess_broker_bracket_confirmation(
            record_snapshot.get("parent_order_id"),
            record_snapshot.get("tp_order_id"),
            record_snapshot.get("sl_order_id"),
            trade_analysis_lock_context="not_locked",
        )

        all_three_visible_or_confirmed = bool(
            confirmation
            and (
                confirmation.get("confirmed")
                or confirmation.get("pending_broker_ack")
            )
            and confirmation.get("parent_visible")
            and confirmation.get("tp_visible")
            and confirmation.get("sl_visible")
            and confirmation.get("parent_link_ok")
            and confirmation.get("tp_link_ok")
            and confirmation.get("sl_link_ok")
        )
        quantity_coverage = self.assess_bracket_quantity_coverage(
            record_snapshot,
            broker_confirmation=confirmation,
            broker_reality=broker_reality,
        )
        self.log_bracket_quantity_coverage(record_snapshot, quantity_coverage)
        quantity_safe_for_ack = self.is_bracket_quantity_safe_for_ack(quantity_coverage)
        all_three_visible_or_confirmed = bool(
            all_three_visible_or_confirmed
            and (
                quantity_safe_for_ack
                or quantity_coverage.get("quantity_state") == "NO_OPEN_EXPOSURE_QUANTITY_UNKNOWN"
            )
        )

        if all_three_visible_or_confirmed:
            self.external_entries_blocked_reason = None
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    previous_state = record.get("state")
                    record["state"] = "BROKER_ACK_PENDING"
                    record["bracket_submit_transaction_status"] = "broker_confirmed_after_exception"
                    record["bracket_submit_uncertain"] = False
                    record["broker_ack_pending_reason"] = (
                        "broker_confirmed_complete_after_submit_exception"
                        if quantity_safe_for_ack
                        else quantity_coverage.get("quantity_state")
                    )
                    record["broker_ack_pending_category"] = confirmation.get("broker_state_category")
                    record["broker_ack_pending_visible_order_ids"] = confirmation.get("visible_order_ids", [])
                    self.append_trade_event(
                        trade_id,
                        f"BRACKET SUBMIT FAILURE RECOVERED previous_state={previous_state} "
                        f"broker_state_category={confirmation.get('broker_state_category')} "
                        f"visible_order_ids={confirmation.get('visible_order_ids')}",
                    )
            if quantity_safe_for_ack and confirmation.get("all_broker_live"):
                self.set_execution_validation_status(
                    trade_id,
                    "broker_live",
                    "broker_confirmed_complete_after_submit_exception",
                )
            elif quantity_safe_for_ack and confirmation.get("confirmed"):
                self.set_execution_validation_status(
                    trade_id,
                    "broker_acknowledged",
                    "broker_confirmed_complete_after_submit_exception",
                )
            logger.warning(
                "BRACKET_SUBMISSION_FAILURE_RECOVERED_BY_BROKER_CONFIRMATION | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"broker_state_category={confirmation.get('broker_state_category')} "
                f"visible_order_ids={confirmation.get('visible_order_ids')} "
                "decision=broker_ack_pending"
            )
            return {
                "ok": True,
                "reason": "broker_confirmed_complete_after_submit_exception",
                "broker_reality": broker_reality,
                "broker_confirmation": confirmation,
            }

        open_position_estimate = quantity_coverage.get("open_position_estimate")
        if (
            open_position_estimate is not None
            and open_position_estimate > 0
            and not quantity_safe_for_ack
        ):
            protection_context = self.classify_position_protection_context(
                record_snapshot,
                broker_reality,
                confirmation,
            )
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["state"] = "BROKER_ACK_PENDING"
                    record["broker_ack_pending_reason"] = quantity_coverage.get("quantity_state")
                    self.append_trade_event(
                        trade_id,
                        f"BRACKET SUBMIT FAILURE QUANTITY FAIL CLOSED "
                        f"quantity_state={quantity_coverage.get('quantity_state')} "
                        f"open_position_estimate={open_position_estimate} "
                        f"protective_sl_coverage={quantity_coverage.get('protective_sl_coverage')}"
                    )
            self.emergency_flatten_unprotected_position(
                trade_id,
                symbol,
                quantity_coverage.get("quantity_state"),
                protection_context=protection_context,
                broker_reality=broker_reality,
            )
            return {
                "ok": False,
                "reason": quantity_coverage.get("quantity_state"),
                "broker_reality": broker_reality,
                "broker_confirmation": confirmation,
            }

        cleanup_result = self.cancel_unacknowledged_bracket_legs(
            trade_id,
            "bracket_submit_failed_uncertain",
            broker_reality=broker_reality,
        )
        logger.critical(
            "BRACKET_SUBMISSION_FAILURE_CLEANUP_RESULT | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"cleanup_ok={cleanup_result.get('ok')} "
            f"cleanup_reason={cleanup_result.get('reason')} "
            f"cancel_requested_count={cleanup_result.get('cancel_requested_count')} "
            f"cancel_failed_count={cleanup_result.get('cancel_failed_count')} "
            f"visible_order_ids={cleanup_result.get('visible_order_ids')} "
            "decision=preserve_failed_uncertain_lock"
        )
        return {
            "ok": False,
            "reason": "bracket_submit_failed_uncertain",
            "broker_reality": broker_reality,
            "broker_confirmation": confirmation,
            "cleanup_result": cleanup_result,
        }

    def handle_post_submit_uncertainty(self, trade_id, reason_label, exc):
        with self.trade_analysis_lock:
            record_snapshot = dict(self.trade_analysis.get(trade_id) or {})
        submit_context = self.get_bracket_submit_context_from_record(record_snapshot)
        logger.critical(
            "BRACKET_POST_SUBMIT_UNCERTAIN | "
            f"trade_id={trade_id} "
            f"symbol={record_snapshot.get('symbol')} "
            f"reason_label={reason_label} "
            f"exception={exc} "
            "decision=route_to_bracket_submit_failure_handler"
        )
        return self.handle_bracket_submission_failure(
            trade_id,
            record_snapshot,
            submit_context,
            RuntimeError(f"{reason_label}:{exc}"),
        )

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
                f"risk_intent_profile={record.get('risk_intent_profile')} "
                f"risk_intent_percent={record.get('risk_intent_percent')} "
                f"risk_intent_status={record.get('risk_intent_status')} "
                f"risk_intent_reason={record.get('risk_intent_reason')} "
                f"theoretical_size_before_containment={record.get('theoretical_size_before_containment')} "
                f"containment_status={record.get('containment_status')} "
                f"containment_stage={record.get('containment_stage')} "
                f"containment_profile={record.get('containment_profile')} "
                f"containment_max_size={record.get('containment_max_size')} "
                f"containment_max_notional={record.get('containment_max_notional')} "
                f"containment_hierarchy={record.get('containment_hierarchy')} "
                f"containment_decision_source={record.get('containment_decision_source')} "
                f"containment_denial_source={record.get('containment_denial_source')} "
                f"triggering_containment_rule={record.get('triggering_containment_rule')} "
                f"notional_model={record.get('notional_model')} "
                f"notional_currency={record.get('notional_currency')} "
                f"capped_size_after_size_containment={record.get('capped_size_after_size_containment')} "
                f"estimated_notional_before_containment={record.get('estimated_notional_before_containment')} "
                f"estimated_notional_after_size_containment={record.get('estimated_notional_after_size_containment')} "
                f"estimated_notional_after_containment={record.get('estimated_notional_after_containment')} "
                f"containment_action={record.get('containment_action')} "
                f"containment_reason={record.get('containment_reason')} "
                f"approved_size_after_containment={record.get('approved_size_after_containment')} "
                f"execution_lane={record['execution_lane']} "
                f"promoted_from_shadow={record['promoted_from_shadow']} "
                f"entry={record['entry_price']} stop={record['stop_price']} target={record['target_price']}"
            )

            return trade_id

    def register_dry_run_trade_analysis(
        self,
        job,
        signal_time,
        enqueue_time,
        execution_start_time,
        spread_adjusted_entry,
        entry,
        stop,
        target,
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
                None,
                None,
                None,
            )
            trade_id = record["trade_id"]
            record.update({
                "state": "DRY_RUN_NOT_TRANSMITTED",
                "dry_run": True,
                "transmitted": False,
                "transmission_mode": "dry_run",
                "transmission_block_reason": "prd_dry_run_no_order_transmission",
                "broker_order_ids_transmitted": [],
                "execution_validation_status": "dry_run_not_transmitted",
                "bracket_submit_start_time": execution_start_time,
                "bracket_submit_end_time": datetime.now(timezone.utc),
                "bracket_submit_transaction_status": "dry_run_not_transmitted",
                "parent_submit_attempted": False,
                "tp_submit_attempted": False,
                "sl_submit_attempted": False,
                "parent_submit_completed": False,
                "tp_submit_completed": False,
                "sl_submit_completed": False,
                "closed": True,
            })

            self.trade_analysis[trade_id] = record
            self.aggregate_stats["dry_run_planned_count"] = (
                self.aggregate_stats.get("dry_run_planned_count", 0) + 1
            )
            self.aggregate_stats["dry_run_not_transmitted_count"] = (
                self.aggregate_stats.get("dry_run_not_transmitted_count", 0) + 1
            )
            symbol_bucket = self.get_symbol_stats_bucket(record["symbol"])
            symbol_bucket["dry_run_planned_count"] = (
                symbol_bucket.get("dry_run_planned_count", 0) + 1
            )
            symbol_bucket["dry_run_not_transmitted_count"] = (
                symbol_bucket.get("dry_run_not_transmitted_count", 0) + 1
            )

            self.append_trade_event(
                trade_id,
                "DRY_RUN_NOT_TRANSMITTED "
                f"symbol={record['symbol']} side={record['side']} "
                f"entry={record['entry_price']} stop={record['stop_price']} "
                f"target={record['target_price']} "
                f"size={record.get('approved_size_after_containment')} "
                "reason=prd_dry_run_no_order_transmission",
            )
            if self.active_execution_trade_id == trade_id:
                self.active_execution_trade_id = None
            return trade_id

    def finalize_prd_dry_run_order_plan(
        self,
        job,
        symbol,
        side,
        entry,
        stop,
        target,
        spread_adjusted_entry,
        approved_size_after_containment,
    ):
        trade_id = self.register_dry_run_trade_analysis(
            job=job,
            signal_time=job.get("signal_time"),
            enqueue_time=job.get("enqueue_time"),
            execution_start_time=datetime.now(timezone.utc),
            spread_adjusted_entry=spread_adjusted_entry,
            entry=entry,
            stop=stop,
            target=target,
        )
        logger.warning(
            "PRD_DRY_RUN_ACTIVE | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"side={side} "
            f"entry={entry} "
            f"stop={stop} "
            f"target={target} "
            f"size={approved_size_after_containment} "
            "startup_reconciliation_completed="
            f"{self.startup_reconciliation_completed} "
            "reason=prd_dry_run_no_order_transmission"
        )
        logger.critical(
            "ORDER_TRANSMISSION_BLOCKED_DRY_RUN | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"side={side} "
            f"entry={entry} "
            f"stop={stop} "
            f"target={target} "
            f"size={approved_size_after_containment} "
            "broker_order_ids_transmitted=[] "
            "parent_order_transmitted=false "
            "tp_order_transmitted=false "
            "sl_order_transmitted=false "
            "reason=prd_dry_run_no_order_transmission"
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

    def get_trade_by_emergency_flatten_perm_id(self, perm_id):
        if perm_id in (None, 0):
            return None, None
        with self.trade_analysis_lock:
            for trade_id, record in self.trade_analysis.items():
                if perm_id == record.get("emergency_flatten_perm_id"):
                    return trade_id, record
        return None, None

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
            elif order_id == record.get("emergency_flatten_order_id"):
                record["emergency_flatten_perm_id"] = perm_id

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
            if current_state == "BRACKET_SUBMIT_FAILED_UNCERTAIN":
                self.append_trade_event(
                    trade_id,
                    f"BRACKET SUBMIT FAILED UNCERTAIN STATE PRESERVED orderId={order_id} status={status}"
                )
                logger.warning(
                    "BRACKET_SUBMIT_FAILED_UNCERTAIN_PRESERVED | "
                    f"trade_id={trade_id} "
                    f"order_id={order_id} "
                    f"status={status} "
                    "reason=generic_status_update_does_not_exit_submission_quarantine"
                )
                return
            if (
                current_state == "SUBMITTING"
                and record.get("bracket_submit_transaction_status") == "in_progress"
                and not (
                    record.get("parent_submit_completed")
                    and record.get("tp_submit_completed")
                    and record.get("sl_submit_completed")
                )
            ):
                self.append_trade_event(
                    trade_id,
                    f"BRACKET SUBMIT TRANSACTION IN PROGRESS STATE PRESERVED orderId={order_id} status={status}"
                )
                logger.info(
                    "BRACKET_SUBMIT_TRANSACTION_IN_PROGRESS_PRESERVED | "
                    f"trade_id={trade_id} "
                    f"order_id={order_id} "
                    f"status={status} "
                    f"parent_submit_completed={record.get('parent_submit_completed')} "
                    f"tp_submit_completed={record.get('tp_submit_completed')} "
                    f"sl_submit_completed={record.get('sl_submit_completed')} "
                    "reason=do_not_promote_before_complete_bracket_submit"
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

    def get_trade_broker_reality(self, record, trade_analysis_lock_context="maybe_locked"):
        order_ids = {
            record.get("parent_order_id"),
            record.get("tp_order_id"),
            record.get("sl_order_id"),
            record.get("emergency_flatten_order_id"),
        }
        order_ids.discard(None)

        perm_ids = {
            record.get("parent_perm_id"),
            record.get("tp_perm_id"),
            record.get("sl_perm_id"),
            record.get("emergency_flatten_perm_id"),
        }
        perm_ids.discard(None)
        perm_ids.discard(0)

        open_trade_order_ids = []
        open_trade_perm_ids = []
        symbol_open_trade_order_ids = []
        symbol_open_trade_perm_ids = []
        open_order_ids = []
        position_sizes = []
        broker_position_quantity = None

        try:
            open_trades, positions, open_orders = self.broker_read_open_trades_positions_open_orders(
                caller="get_trade_broker_reality",
                reason_label="broker_reality_check",
                trade_id=record.get("trade_id") if isinstance(record, dict) else None,
                symbol=record.get("symbol") if isinstance(record, dict) else None,
                trade_analysis_lock_context=trade_analysis_lock_context,
            )
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
                "broker_position_quantity": None,
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

        broker_position_quantity = self.get_position_quantity_from_positions(
            positions,
            record["symbol"],
        )

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
            "broker_position_quantity": broker_position_quantity,
            "check_failed": False,
        }

    def get_reconstructed_fill_identity(self, fill, execution):
        effective_fill_time, _, _ = self.get_effective_execution_time(fill, execution)
        return self.get_execution_identity(execution, effective_fill_time)

    def resolve_reconstructed_fill_leg(self, record, execution):
        order_id = getattr(execution, "orderId", None)
        perm_id = getattr(execution, "permId", None)

        order_leg = None
        if order_id == record.get("parent_order_id"):
            order_leg = "entry"
        elif order_id == record.get("tp_order_id"):
            order_leg = "tp"
        elif order_id == record.get("sl_order_id"):
            order_leg = "sl"
        elif order_id == record.get("emergency_flatten_order_id"):
            order_leg = "emergency_flatten_exit"

        perm_leg = None
        if perm_id not in (None, 0):
            if perm_id == record.get("parent_perm_id"):
                perm_leg = "entry"
            elif perm_id == record.get("tp_perm_id"):
                perm_leg = "tp"
            elif perm_id == record.get("sl_perm_id"):
                perm_leg = "sl"
            elif perm_id == record.get("emergency_flatten_perm_id"):
                perm_leg = "emergency_flatten_exit"

        if order_leg and perm_leg and order_leg != perm_leg:
            return None, "order_id_perm_id_leg_conflict"
        if order_leg:
            return order_leg, "order_id"
        if perm_leg:
            return perm_leg, "perm_id"

        return None, "no_exact_order_or_perm_match"

    def contract_fallback_matches_record(self, contract, record):
        if contract is None or record is None:
            return False

        record_symbol = str(record.get("symbol") or "").upper()
        if not record_symbol:
            return False

        contract_values = [
            getattr(contract, "symbol", None),
            getattr(contract, "localSymbol", None),
            getattr(contract, "tradingClass", None),
        ]
        for value in contract_values:
            if value is None:
                continue
            text = str(value).upper()
            if text == record_symbol or text.startswith(record_symbol):
                return True

        record_con_id = record.get("contract_con_id")
        contract_con_id = getattr(contract, "conId", None)
        if (
            record_con_id not in (None, "", 0) and
            contract_con_id not in (None, "", 0) and
            str(record_con_id) == str(contract_con_id)
        ):
            return True

        stored_contract_values = [
            record.get("contract_local_symbol"),
            record.get("contract_trading_class"),
        ]
        normalized_contract_values = {
            str(value).upper()
            for value in contract_values
            if value not in (None, "")
        }
        for value in stored_contract_values:
            if value in (None, ""):
                continue
            if str(value).upper() in normalized_contract_values:
                return True

        return False

    def execution_side_matches_leg(self, record, execution, leg):
        execution_side = str(getattr(execution, "side", "") or "").upper()
        if execution_side in {"BOT", "BUY"}:
            action = "BUY"
        elif execution_side in {"SLD", "SELL"}:
            action = "SELL"
        else:
            return False

        if leg == "entry":
            expected_action = "BUY" if record.get("side") == "long" else "SELL"
        elif leg in {"tp", "sl", "emergency_flatten_exit"}:
            expected_action = "SELL" if record.get("side") == "long" else "BUY"
        else:
            return False

        return action == expected_action

    def fill_time_within_record_window(self, record, fill_time):
        if not isinstance(fill_time, datetime):
            return True
        if fill_time.tzinfo is None:
            fill_time = fill_time.replace(tzinfo=timezone.utc)
        else:
            fill_time = fill_time.astimezone(timezone.utc)

        starts = [
            record.get("webhook_received_time"),
            record.get("classification_completed_time"),
            record.get("queue_put_time"),
            record.get("worker_pickup_time"),
            record.get("execution_start_time"),
            record.get("bracket_submit_start_time"),
        ]
        starts = [
            value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            for value in starts
            if isinstance(value, datetime)
        ]
        if not starts:
            return True

        earliest = min(starts)
        if earliest.tzinfo is None:
            earliest = earliest.replace(tzinfo=timezone.utc)
        latest = datetime.now(timezone.utc)
        exit_time = record.get("exit_fill_time")
        if isinstance(exit_time, datetime):
            latest = max(latest, exit_time.astimezone(timezone.utc) if exit_time.tzinfo else exit_time.replace(tzinfo=timezone.utc))

        return (earliest.timestamp() - 300.0) <= fill_time.timestamp() <= (latest.timestamp() + 300.0)

    def build_fallback_price_match_diagnostic(self, record, leg, price):
        diagnostic = {
            "leg": leg,
            "expected_price": None,
            "actual_price": price,
            "tick_size": None,
            "tolerance": None,
            "absolute_distance": None,
            "price_matches": False,
        }
        if price is None:
            return diagnostic
        expected_field = {
            "entry": "entry_price",
            "tp": "target_price",
            "sl": "stop_price",
        }.get(leg)
        expected_price = record.get(expected_field)
        diagnostic["expected_price"] = expected_price
        if expected_price is None:
            return diagnostic
        try:
            tick = float(self.get_tick_size(record.get("symbol")))
            tolerance = max(tick * 2.0, 1e-9)
            absolute_distance = abs(float(price) - float(expected_price))
            diagnostic["tick_size"] = tick
            diagnostic["tolerance"] = tolerance
            diagnostic["absolute_distance"] = round(absolute_distance, 10)
            diagnostic["price_matches"] = absolute_distance <= tolerance
        except Exception:
            pass
        return diagnostic

    def price_matches_expected_leg(self, record, leg, price):
        return self.build_fallback_price_match_diagnostic(
            record,
            leg,
            price,
        ).get("price_matches", False)

    def quantity_matches_expected_leg(self, record, leg, quantity):
        if quantity is None or quantity <= 0:
            return False
        if leg == "entry":
            expected_quantity = (
                record.get("original_intended_parent_quantity")
                or record.get("intended_parent_quantity")
                or record.get("planned_position_size")
                or record.get("position_size")
            )
        else:
            expected_quantity = (
                record.get("realized_entry_quantity")
                or record.get("cumulative_entry_quantity")
                or record.get("original_intended_parent_quantity")
                or record.get("intended_parent_quantity")
            )
        try:
            expected_quantity = float(expected_quantity or 0.0)
            return expected_quantity > 0 and quantity <= expected_quantity + 1e-9
        except Exception:
            return False

    def has_protective_emergency_reconstruction_context(self, record):
        return bool(
            record
            and (
                record.get("protective_emergency_active")
                or record.get("exit_reason") == PROTECTIVE_EMERGENCY_EXIT_REASON
                or record.get("protective_emergency_status") in {
                    "flatten_fill_observed",
                    "pending_position_confirmation",
                    "flat_confirmed_exit_fill_details_incomplete",
                    "flat_confirmed_stale_orders_cleared",
                    "flat_confirmed_stale_child_cleanup_failed",
                    "flat_confirmed_stale_bot_order_cleanup_failed",
                    "reconstructed_flatten_fill_observed",
                    "submit_failed_submission_uncertain",
                    "broker_submission_uncertain",
                }
            )
        )

    def contract_exactly_matches_record_symbol(self, contract, record):
        if contract is None or record is None:
            return False
        record_symbol = str(record.get("symbol") or "").upper()
        if not record_symbol:
            return False
        for value in (
            getattr(contract, "symbol", None),
            getattr(contract, "localSymbol", None),
            getattr(contract, "tradingClass", None),
        ):
            if value is not None and str(value).upper() == record_symbol:
                return True
        record_con_id = record.get("contract_con_id")
        contract_con_id = getattr(contract, "conId", None)
        return bool(
            record_con_id not in (None, "", 0)
            and contract_con_id not in (None, "", 0)
            and str(record_con_id) == str(contract_con_id)
        )

    def resolve_reconstructed_fill_leg_with_fallback(self, record, fill, execution):
        exact_leg, exact_source = self.resolve_reconstructed_fill_leg(record, execution)
        identity_ok, identity_reason, parsed_order_ref = self.execution_order_ref_matches_record(
            record,
            execution,
            contract=getattr(fill, "contract", None),
        )
        if not identity_ok:
            self.log_broker_fill_without_lifecycle_event(
                record.get("trade_id"),
                identity_reason,
                "reconciliation_fill_rejected_by_order_ref_identity_gate",
                fill=fill,
                execution=execution,
                contract=getattr(fill, "contract", None),
            )
            if identity_reason == "foreign_run_id":
                self.log_order_ref_match_decision(
                    "BROKER_FILL_ORPHANED_BY_RUN_ID_MISMATCH",
                    record,
                    parsed_order_ref,
                    identity_reason,
                    execution=execution,
                    decision="preserve_current_lifecycle",
                )
            return None, identity_reason, []
        if exact_leg is not None:
            return exact_leg, exact_source, []
        if self.record_allows_reconstructed_broker_identity(record):
            return None, "startup_reconstructed_requires_exact_order_or_perm_match", []

        contract = getattr(fill, "contract", None)
        fill_time, _, _ = self.get_effective_execution_time(fill, execution)
        quantity = self.normalize_fill_quantity(getattr(execution, "shares", None))
        price = getattr(execution, "price", None)

        if not self.contract_fallback_matches_record(contract, record):
            return None, "fallback_symbol_mismatch", []
        if not self.fill_time_within_record_window(record, fill_time):
            return None, "fallback_time_window_mismatch", []

        candidates = []
        diagnostics = []
        side_mismatch_count = 0
        quantity_mismatch_count = 0
        price_mismatch_count = 0
        for leg in ("entry", "tp", "sl"):
            if not self.execution_side_matches_leg(record, execution, leg):
                side_mismatch_count += 1
                diagnostics.append({"leg": leg, "reason": "fallback_side_mismatch"})
                continue
            if not self.quantity_matches_expected_leg(record, leg, quantity):
                quantity_mismatch_count += 1
                diagnostics.append(
                    {
                        "leg": leg,
                        "reason": "fallback_quantity_mismatch",
                        "actual_quantity": quantity,
                    }
                )
                continue
            price_diagnostic = self.build_fallback_price_match_diagnostic(record, leg, price)
            if not price_diagnostic.get("price_matches"):
                price_mismatch_count += 1
                price_diagnostic["reason"] = "fallback_price_outside_tolerance"
                diagnostics.append(price_diagnostic)
                continue
            candidates.append(leg)

        emergency_candidates = []
        if (
            not candidates
            and self.has_protective_emergency_reconstruction_context(record)
            and self.contract_exactly_matches_record_symbol(contract, record)
            and self.execution_side_matches_leg(record, execution, "emergency_flatten_exit")
            and self.quantity_matches_expected_leg(record, "emergency_flatten_exit", quantity)
            and not self.price_matches_expected_leg(record, "tp", price)
            and not self.price_matches_expected_leg(record, "sl", price)
        ):
            emergency_candidates.append("emergency_flatten_exit")

        if len(emergency_candidates) == 1:
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_RECONSTRUCTED_FILL | "
                f"trade_id={record.get('trade_id')} "
                f"symbol={record.get('symbol')} "
                f"exec_id={getattr(execution, 'execId', None)} "
                f"order_id={getattr(execution, 'orderId', None)} "
                f"perm_id={getattr(execution, 'permId', None)} "
                f"side={getattr(execution, 'side', None)} "
                f"quantity={quantity} "
                f"price={price} "
                "match_source=fallback_protective_emergency_symbol_side_time_quantity "
                f"fallback_candidates={emergency_candidates} "
                "decision=map_unique_emergency_fallback_as_exit"
            )
            return (
                "emergency_flatten_exit",
                "fallback_protective_emergency_symbol_side_time_quantity",
                emergency_candidates,
            )
        if len(emergency_candidates) > 1:
            logger.warning(
                "PROTECTIVE_EMERGENCY_FLATTEN_RECONSTRUCTION_AMBIGUOUS | "
                f"trade_id={record.get('trade_id')} "
                f"symbol={record.get('symbol')} "
                f"fallback_candidates={emergency_candidates} "
                "decision=preserve_current_lifecycle"
            )
            return None, "fallback_protective_emergency_ambiguous", emergency_candidates

        if len(candidates) == 1:
            return candidates[0], "fallback_symbol_side_time_quantity_price", candidates
        if len(candidates) > 1:
            return None, "fallback_ambiguous_multiple_legs", candidates
        if side_mismatch_count == 3:
            return None, "fallback_side_mismatch", diagnostics
        if quantity_mismatch_count > 0 and price_mismatch_count == 0:
            return None, "fallback_quantity_mismatch", diagnostics
        if price_mismatch_count > 0:
            return None, "fallback_price_outside_tolerance", diagnostics
        return None, "fallback_no_leg_candidate", []

    def is_safe_no_fill_reconciliation_noise(self, record, reason):
        return bool(
            reason == "no_exact_matching_fills" and
            record is not None and
            record.get("state") in {"ENTRY_WORKING", "BROKER_ACK_PENDING"} and
            not record.get("entry_filled") and
            not record.get("closed") and
            float(record.get("cumulative_entry_quantity") or 0.0) == 0.0 and
            float(record.get("cumulative_exit_quantity") or 0.0) == 0.0
        )

    def should_log_reconciliation_noise(self, record, reason, level_key, interval_seconds=60.0):
        if not self.is_safe_no_fill_reconciliation_noise(record, reason):
            return True, 0, False

        trade_id = record.get("trade_id")
        now = time.time()

        with self.trade_analysis_lock:
            live_record = self.trade_analysis.get(trade_id)
            if live_record is None:
                return True, 0, False
            if not self.is_safe_no_fill_reconciliation_noise(live_record, reason):
                return True, 0, False

            suppression = live_record.setdefault("reconciliation_log_suppression", {})
            suppression_key = f"{level_key}:{reason}"
            bucket = suppression.setdefault(
                suppression_key,
                {
                    "last_logged_at": None,
                    "suppressed_count": 0,
                }
            )

            last_logged_at = bucket.get("last_logged_at")
            if last_logged_at is None:
                bucket["last_logged_at"] = now
                bucket["suppressed_count"] = 0
                return True, 0, True

            if now - last_logged_at >= interval_seconds:
                suppressed_count = bucket.get("suppressed_count", 0)
                bucket["last_logged_at"] = now
                bucket["suppressed_count"] = 0
                return True, suppressed_count, True

            bucket["suppressed_count"] = bucket.get("suppressed_count", 0) + 1
            return False, bucket["suppressed_count"], True

    def get_reconciliation_execution_filter_start(self, record):
        starts = [
            record.get("webhook_received_time"),
            record.get("classification_completed_time"),
            record.get("queue_put_time"),
            record.get("worker_pickup_time"),
            record.get("execution_start_time"),
            record.get("bracket_submit_start_time"),
        ]
        starts = [
            value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            for value in starts
            if isinstance(value, datetime)
        ]
        if not starts:
            return datetime.now(timezone.utc) - timedelta(hours=1)
        return min(starts) - timedelta(minutes=5)

    def build_execution_filter_for_record(self, record):
        exec_filter = ExecutionFilter()
        since_time = self.get_reconciliation_execution_filter_start(record)
        exec_filter.time = since_time.strftime("%Y%m%d %H:%M:%S")
        account = record.get("account") or record.get("broker_account") or BOT_ACCOUNT_CONTEXT
        if account not in (None, "", "default"):
            exec_filter.acctCode = str(account)
        return exec_filter

    def broker_fill_references_record_identity(self, record, fill):
        execution = getattr(fill, "execution", None)
        if execution is None:
            return False
        order_id = getattr(execution, "orderId", None)
        perm_id = getattr(execution, "permId", None)
        if order_id in {
            record.get("parent_order_id"),
            record.get("tp_order_id"),
            record.get("sl_order_id"),
            record.get("emergency_flatten_order_id"),
        }:
            return True
        if perm_id not in (None, 0) and perm_id in {
            record.get("parent_perm_id"),
            record.get("tp_perm_id"),
            record.get("sl_perm_id"),
            record.get("emergency_flatten_perm_id"),
        }:
            return True
        order_ref = self.get_order_ref(execution=execution)
        return self.broker_order_ref_matches_record(record, order_ref)

    def get_fill_source_identity(self, fill):
        execution = getattr(fill, "execution", None)
        if execution is None:
            return f"missing_execution:{id(fill)}"
        fill_time, _, _ = self.get_effective_execution_time(fill, execution)
        return self.get_execution_identity(execution, fill_time)

    def read_req_execution_fills_for_record(self, record):
        exec_filter = self.build_execution_filter_for_record(record)
        try:
            fills = self.broker_read_executions(
                exec_filter,
                caller="build_in_session_broker_fill_reconciliation_candidate",
                reason_label="broker_fill_reconciliation_req_executions",
                trade_id=record.get("trade_id"),
                symbol=record.get("symbol"),
                trade_analysis_lock_context="not_locked",
            )
        except Exception as exc:
            logger.warning(
                "BROKER_FILL_RECOVERY_REQ_EXECUTIONS_FAILED | "
                f"trade_id={record.get('trade_id')} "
                f"symbol={record.get('symbol')} "
                f"failure={exc}"
            )
            return []
        logger.warning(
            "BROKER_FILL_RECOVERY_REQ_EXECUTIONS_READ | "
            f"trade_id={record.get('trade_id')} "
            f"symbol={record.get('symbol')} "
            f"returned_count={len(fills or [])} "
            f"filter_time={getattr(exec_filter, 'time', None)} "
            f"filter_account={getattr(exec_filter, 'acctCode', None)}"
        )
        return fills or []

    def build_reconciliation_fill_sources(self, record, local_fills):
        fill_sources = []
        seen_identities = set()
        local_relevant_count = 0

        for fill in local_fills or []:
            identity = self.get_fill_source_identity(fill)
            if identity in seen_identities:
                continue
            seen_identities.add(identity)
            if self.broker_fill_references_record_identity(record, fill):
                local_relevant_count += 1
            fill_sources.append(("local_cache", fill))

        if local_relevant_count <= 0:
            logger.warning(
                "BROKER_FILL_LOCAL_CACHE_NO_RELEVANT_FILLS | "
                f"trade_id={record.get('trade_id')} "
                f"symbol={record.get('symbol')} "
                f"local_fill_count={len(local_fills or [])} "
                "decision=req_executions_recovery"
            )
            for fill in self.read_req_execution_fills_for_record(record):
                identity = self.get_fill_source_identity(fill)
                if identity in seen_identities:
                    logger.info(
                        "BROKER_FILL_RECOVERY_DUPLICATE_SOURCE_IGNORED | "
                        f"trade_id={record.get('trade_id')} "
                        f"symbol={record.get('symbol')} "
                        f"fill_source=req_executions "
                        f"identity={identity}"
                    )
                    continue
                seen_identities.add(identity)
                fill_sources.append(("req_executions", fill))

        return fill_sources

    def build_in_session_broker_fill_reconciliation_candidate(self, record):
        trade_id = record.get("trade_id")
        start_should_log, start_suppressed_count, start_suppressed = self.should_log_reconciliation_noise(
            record,
            "no_exact_matching_fills",
            "start",
        )
        start_message = (
            "BROKER_FILL_RECONCILIATION_START | "
            f"trade_id={trade_id} "
            f"symbol={record.get('symbol')} "
            f"state={record.get('state')} "
            f"parent_order_id={record.get('parent_order_id')} "
            f"tp_order_id={record.get('tp_order_id')} "
            f"sl_order_id={record.get('sl_order_id')} "
            f"parent_perm_id={record.get('parent_perm_id')} "
            f"tp_perm_id={record.get('tp_perm_id')} "
            f"sl_perm_id={record.get('sl_perm_id')}"
        )
        if start_should_log:
            if start_suppressed and start_suppressed_count > 0:
                logger.info(
                    f"{start_message} "
                    "log_category=expected_noop "
                    "decision=preserve_current_lifecycle "
                    "reason=no_exact_matching_fills "
                    "expected_resting_no_fills=true "
                    f"suppressed_count={start_suppressed_count}"
                )
            elif start_suppressed:
                logger.info(
                    f"{start_message} "
                    "log_category=expected_noop "
                    "decision=preserve_current_lifecycle "
                    "reason=no_exact_matching_fills "
                    "expected_resting_no_fills=true"
                )
            else:
                logger.warning(
                    f"{start_message} "
                    "log_category=reconciliation_start "
                    "decision=inspect_broker_fills "
                    "reason=active_reconciliation_candidate"
                )

        try:
            fills = self.broker_read_fills(
                caller="build_in_session_broker_fill_reconciliation_candidate",
                reason_label="broker_fill_reconciliation",
                trade_id=record.get("trade_id") if isinstance(record, dict) else None,
                symbol=record.get("symbol") if isinstance(record, dict) else None,
                trade_analysis_lock_context="not_locked",
            )
        except Exception as exc:
            logger.exception(
                "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                f"trade_id={trade_id} "
                f"symbol={record.get('symbol')} "
                "log_category=broker_read_failure "
                "decision=preserve_current_lifecycle "
                "reason=fills_unavailable"
            )
            return {
                "ok": False,
                "reason": f"fills_unavailable:{exc}",
            }

        buckets = {
            "entry": {"quantity": 0.0, "notional": 0.0, "price": None, "first_time": None, "last_time": None},
            "tp": {"quantity": 0.0, "notional": 0.0, "price": None, "first_time": None, "last_time": None},
            "sl": {"quantity": 0.0, "notional": 0.0, "price": None, "first_time": None, "last_time": None},
            "emergency_flatten_exit": {"quantity": 0.0, "notional": 0.0, "price": None, "first_time": None, "last_time": None},
        }
        matched_identities = set()
        match_sources = {
            "order_id": 0,
            "perm_id": 0,
            "fallback_symbol_side_time_quantity_price": 0,
        }
        matched_count = 0
        fallback_ambiguous_count = 0

        fill_sources = self.build_reconciliation_fill_sources(record, fills)

        for fill_source, fill in fill_sources:
            execution = getattr(fill, "execution", None)
            if execution is None:
                continue

            leg, match_source, fallback_candidates = self.resolve_reconstructed_fill_leg_with_fallback(
                record,
                fill,
                execution,
            )
            if leg is None:
                if str(match_source or "").startswith("fallback_"):
                    fallback_diagnostics = None
                    if fallback_candidates:
                        try:
                            fallback_diagnostics = json.dumps(
                                fallback_candidates,
                                sort_keys=True,
                                default=str,
                            )
                        except Exception:
                            fallback_diagnostics = str(fallback_candidates)
                    self.log_broker_fill_without_lifecycle_event(
                        trade_id,
                        match_source,
                        (
                            f"reconciliation_fallback_not_applied fill_source={fill_source}"
                            if fallback_diagnostics is None
                            else f"reconciliation_fallback_not_applied fill_source={fill_source} diagnostics={fallback_diagnostics}"
                        ),
                        fill=fill,
                        execution=execution,
                        contract=getattr(fill, "contract", None),
                    )
                if match_source == "fallback_ambiguous_multiple_legs":
                    fallback_ambiguous_count += 1
                    logger.warning(
                        "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                        f"trade_id={trade_id} "
                        f"symbol={record.get('symbol')} "
                        "log_category=ambiguous "
                        "decision=preserve_current_lifecycle "
                        f"reason={match_source} "
                        f"fill_source={fill_source} "
                        f"fallback_candidates={fallback_candidates} "
                        f"{self.format_broker_identity_fields(fill=fill, execution=execution, contract=getattr(fill, 'contract', None))}"
                    )
                continue

            identity = self.get_reconstructed_fill_identity(fill, execution)
            if identity in matched_identities:
                logger.warning(
                    "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                    f"trade_id={trade_id} "
                    f"symbol={record.get('symbol')} "
                    "log_category=ambiguous "
                    "decision=preserve_current_lifecycle "
                    f"reason=duplicate_fill_identity identity={identity}"
                )
                return {
                    "ok": False,
                    "reason": "duplicate_fill_identity",
                    "duplicate_identity": identity,
                }
            matched_identities.add(identity)

            quantity = self.normalize_fill_quantity(getattr(execution, "shares", None))
            price = getattr(execution, "price", None)
            if quantity is None or price is None:
                logger.warning(
                    "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                    f"trade_id={trade_id} "
                    f"symbol={record.get('symbol')} "
                    "log_category=ambiguous "
                    "decision=preserve_current_lifecycle "
                    f"reason=invalid_fill_quantity_or_price identity={identity}"
                )
                return {
                    "ok": False,
                    "reason": "invalid_fill_quantity_or_price",
                    "fill_identity": identity,
                }

            fill_time, execution_time, callback_fill_time = self.get_effective_execution_time(fill, execution)
            bucket = buckets[leg]
            bucket["quantity"] += quantity
            bucket["notional"] += quantity * float(price)
            bucket["price"] = round(bucket["notional"] / bucket["quantity"], 10)
            if bucket["first_time"] is None or (
                isinstance(fill_time, datetime) and
                isinstance(bucket["first_time"], datetime) and
                fill_time < bucket["first_time"]
            ):
                bucket["first_time"] = fill_time
            if bucket["last_time"] is None or (
                isinstance(fill_time, datetime) and
                isinstance(bucket["last_time"], datetime) and
                fill_time > bucket["last_time"]
            ):
                bucket["last_time"] = fill_time
            matched_count += 1
            match_sources[match_source] = match_sources.get(match_source, 0) + 1
            logger.info(
                "BROKER FILL RECONCILIATION MATCH | "
                f"trade_id={trade_id} "
                f"symbol={record.get('symbol')} "
                f"leg={leg} "
                f"fill_source={fill_source} "
                f"match_source={match_source} "
                f"identity={identity} "
                f"quantity={quantity} "
                f"price={price} "
                f"execution_time={self.to_iso(execution_time)} "
                f"callback_fill_time={self.to_iso(callback_fill_time)}"
            )

        if matched_count <= 0:
            reason = (
                "fallback_ambiguous_unmatched_broker_fills"
                if fallback_ambiguous_count > 0
                else "no_exact_matching_fills"
            )
            should_log, suppressed_count, suppressed = self.should_log_reconciliation_noise(
                record,
                reason,
                "ambiguous",
            )
            if should_log:
                if suppressed and suppressed_count > 0:
                    logger.info(
                        "BROKER FILL RECONCILIATION SUPPRESSED SUMMARY | "
                        f"trade_id={trade_id} "
                        f"symbol={record.get('symbol')} "
                        f"state={record.get('state')} "
                        "log_category=expected_noop "
                        "decision=preserve_current_lifecycle "
                        f"reason={reason} "
                        "expected_resting_no_fills=true "
                        f"suppressed_count={suppressed_count}"
                    )
                elif suppressed:
                    logger.info(
                        "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                        f"trade_id={trade_id} "
                        f"symbol={record.get('symbol')} "
                        f"state={record.get('state')} "
                        "log_category=expected_noop "
                        "decision=preserve_current_lifecycle "
                        f"reason={reason} "
                        "expected_resting_no_fills=true"
                    )
                else:
                    logger.warning(
                        "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                        f"trade_id={trade_id} "
                        f"symbol={record.get('symbol')} "
                        "log_category=ambiguous "
                        "decision=preserve_current_lifecycle "
                        f"reason={reason}"
                    )
            return {
                "ok": False,
                "reason": reason,
                "fallback_ambiguous_count": fallback_ambiguous_count,
            }

        entry_quantity = round(buckets["entry"]["quantity"], 10)
        tp_quantity = round(buckets["tp"]["quantity"], 10)
        sl_quantity = round(buckets["sl"]["quantity"], 10)
        emergency_exit_quantity = round(buckets["emergency_flatten_exit"]["quantity"], 10)
        exit_quantity = round(tp_quantity + sl_quantity + emergency_exit_quantity, 10)
        exit_reason = None
        if emergency_exit_quantity > 0:
            exit_reason = PROTECTIVE_EMERGENCY_EXIT_REASON
        elif tp_quantity > 0 and sl_quantity > 0:
            exit_reason = "MIXED_EXIT"
        elif tp_quantity > 0:
            exit_reason = "TP"
        elif sl_quantity > 0:
            exit_reason = "SL"

        candidate = {
            "ok": True,
            "reason": "candidate_built",
            "matched_count": matched_count,
            "match_sources": match_sources,
            "fallback_ambiguous_count": fallback_ambiguous_count,
            "matched_identities": matched_identities,
            "entry_quantity": entry_quantity,
            "entry_notional": buckets["entry"]["notional"],
            "entry_fill_price": buckets["entry"]["price"],
            "entry_first_time": buckets["entry"]["first_time"],
            "entry_last_time": buckets["entry"]["last_time"],
            "tp_quantity": tp_quantity,
            "tp_notional": buckets["tp"]["notional"],
            "tp_fill_price": buckets["tp"]["price"],
            "tp_first_time": buckets["tp"]["first_time"],
            "tp_last_time": buckets["tp"]["last_time"],
            "sl_quantity": sl_quantity,
            "sl_notional": buckets["sl"]["notional"],
            "sl_fill_price": buckets["sl"]["price"],
            "sl_first_time": buckets["sl"]["first_time"],
            "sl_last_time": buckets["sl"]["last_time"],
            "emergency_flatten_exit_quantity": emergency_exit_quantity,
            "emergency_flatten_exit_notional": buckets["emergency_flatten_exit"]["notional"],
            "emergency_flatten_exit_fill_price": buckets["emergency_flatten_exit"]["price"],
            "emergency_flatten_exit_first_time": buckets["emergency_flatten_exit"]["first_time"],
            "emergency_flatten_exit_last_time": buckets["emergency_flatten_exit"]["last_time"],
            "exit_quantity": exit_quantity,
            "exit_notional": (
                buckets["tp"]["notional"]
                + buckets["sl"]["notional"]
                + buckets["emergency_flatten_exit"]["notional"]
            ),
            "exit_fill_price": None,
            "exit_last_time": None,
            "exit_reason": exit_reason,
        }
        if exit_quantity > 0:
            candidate["exit_fill_price"] = round(candidate["exit_notional"] / exit_quantity, 10)
            exit_times = [
                value for value in (
                    buckets["tp"]["last_time"],
                    buckets["sl"]["last_time"],
                    buckets["emergency_flatten_exit"]["last_time"],
                )
                if value is not None
            ]
            if exit_times:
                candidate["exit_last_time"] = max(exit_times)

        logger.warning(
            "BROKER_FILL_RECONCILIATION_CANDIDATE | "
            f"trade_id={trade_id} symbol={record.get('symbol')} "
            f"matched_count={matched_count} "
            f"entry_quantity={entry_quantity} "
            f"tp_quantity={tp_quantity} "
            f"sl_quantity={sl_quantity} "
            f"emergency_flatten_exit_quantity={emergency_exit_quantity} "
            f"exit_quantity={exit_quantity} "
            f"exit_reason={exit_reason} "
            f"match_sources={json.dumps(match_sources, sort_keys=True)}"
        )
        return candidate

    def validate_in_session_broker_fill_reconciliation_candidate(self, record, candidate):
        broker_reality = self.get_trade_broker_reality(
            record,
            trade_analysis_lock_context="not_locked",
        )
        return self.validate_in_session_broker_fill_reconciliation_candidate_with_broker_reality(
            record,
            candidate,
            broker_reality,
        )

    # P185: move high-risk broker reads out of trade_analysis_lock; no lifecycle semantics change.
    def validate_in_session_broker_fill_reconciliation_candidate_with_broker_reality(self, record, candidate, broker_reality):
        trade_id = record.get("trade_id")
        if record.get("summary_logged"):
            return False, "trade_already_finalized", None
        if not candidate.get("ok"):
            return False, candidate.get("reason") or "candidate_unavailable", None
        if int(candidate.get("fallback_ambiguous_count") or 0) > 0:
            return False, "fallback_ambiguous_unmatched_broker_fills", None

        entry_quantity = float(candidate.get("entry_quantity") or 0.0)
        exit_quantity = float(candidate.get("exit_quantity") or 0.0)
        exit_reason = candidate.get("exit_reason")
        if entry_quantity <= 0:
            return False, "reconstructed_entry_missing", None
        if exit_quantity <= 0:
            return False, "reconstructed_exit_missing", None
        if abs(entry_quantity - exit_quantity) > 1e-9:
            return False, "reconstructed_entry_exit_quantity_mismatch", None
        if exit_reason not in {"TP", "SL", "MIXED_EXIT", PROTECTIVE_EMERGENCY_EXIT_REASON}:
            return False, "reconstructed_exit_reason_missing", None

        if broker_reality is None:
            return False, "broker_reality_missing", None
        if broker_reality.get("check_failed"):
            return False, "broker_reality_check_failed", broker_reality
        if broker_reality.get("has_position_match"):
            return False, "broker_position_not_flat", broker_reality
        if broker_reality.get("has_direct_open_trade_match") or broker_reality.get("has_open_order_match"):
            return False, "direct_parent_child_open_risk_remains", broker_reality
        if broker_reality.get("has_symbol_open_trade_match"):
            return False, "same_symbol_unmapped_open_risk_remains", broker_reality

        logger.warning(
            "BROKER_FILL_RECONCILIATION_FLAT | "
            f"trade_id={trade_id} symbol={record.get('symbol')} "
            f"entry_quantity={entry_quantity} "
            f"exit_quantity={exit_quantity} "
            f"exit_reason={exit_reason} "
            "position_flat=true direct_open_risk=false symbol_open_risk=false"
        )
        return True, "broker_reconciled_flat", broker_reality

    def log_broker_fill_reconciliation_apply_drift(self, record_snapshot, record, reason_label):
        try:
            logger.warning(
                "BROKER FILL RECONCILIATION APPLY DRIFT | "
                f"trade_id={record_snapshot.get('trade_id')} "
                f"symbol={record_snapshot.get('symbol')} "
                f"snapshot_state={record_snapshot.get('state')} "
                f"live_state={record.get('state') if record else None} "
                f"snapshot_summary_logged={record_snapshot.get('summary_logged')} "
                f"live_summary_logged={record.get('summary_logged') if record else None} "
                f"reason_label={reason_label} "
                "decision=preserve_current_lifecycle"
            )
        except Exception:
            try:
                logger.exception("BROKER FILL RECONCILIATION APPLY DRIFT LOG FAILED")
            except Exception:
                pass

    def broker_fill_reconciliation_snapshot_matches_live_record(self, record_snapshot, record):
        if record is None:
            return False
        if record.get("summary_logged"):
            return False

        fields = (
            "state",
            "entry_filled",
            "closed",
            "exit_reason",
            "exit_fill_price",
            "realized_entry_quantity",
            "realized_exit_quantity",
            "cumulative_entry_quantity",
            "cumulative_exit_quantity",
            "parent_order_id",
            "tp_order_id",
            "sl_order_id",
            "emergency_flatten_order_id",
            "emergency_flatten_perm_id",
            "parent_perm_id",
            "tp_perm_id",
            "sl_perm_id",
        )
        for field in fields:
            if record_snapshot.get(field) != record.get(field):
                return False
        return True

    def apply_in_session_broker_fill_reconciliation(
        self,
        trade_id,
        candidate,
        reason_label,
        record_snapshot=None,
        validation_reason=None,
        broker_reality=None,
    ):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None or record.get("summary_logged"):
                return False

            if record_snapshot is not None and not self.broker_fill_reconciliation_snapshot_matches_live_record(
                record_snapshot,
                record,
            ):
                self.log_broker_fill_reconciliation_apply_drift(
                    record_snapshot,
                    record,
                    reason_label,
                )
                return False

            if broker_reality is None:
                logger.warning(
                    "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                    f"trade_id={trade_id} symbol={record.get('symbol')} "
                    "reason=broker_reality_missing_at_apply"
                )
                self.append_trade_event(
                    trade_id,
                    "BROKER FILL RECONCILIATION NOT APPLIED reason=broker_reality_missing_at_apply"
                )
                return False

            valid = validation_reason == "broker_reconciled_flat"
            if not valid:
                logger.warning(
                    "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                    f"trade_id={trade_id} symbol={record.get('symbol')} "
                    f"reason={validation_reason}"
                )
                self.append_trade_event(
                    trade_id,
                    f"BROKER FILL RECONCILIATION NOT APPLIED reason={validation_reason}"
                )
                return False

            previous_state = record.get("state")
            record["cumulative_entry_quantity"] = candidate["entry_quantity"]
            record["cumulative_entry_notional"] = candidate["entry_notional"]
            record["entry_fill_price"] = candidate["entry_fill_price"]
            record["entry_fill_time"] = candidate["entry_last_time"] or candidate["entry_first_time"]
            record["realized_entry_quantity"] = candidate["entry_quantity"]
            record["planned_position_size"] = candidate["entry_quantity"]
            record["position_size"] = candidate["entry_quantity"]
            record["entry_filled"] = True

            record["cumulative_exit_quantity"] = candidate["exit_quantity"]
            record["cumulative_exit_notional"] = candidate["exit_notional"]
            record["exit_fill_price"] = candidate["exit_fill_price"]
            record["exit_fill_time"] = candidate["exit_last_time"]
            record["realized_exit_quantity"] = candidate["exit_quantity"]
            record["tp_exit_quantity"] = candidate["tp_quantity"]
            record["tp_exit_notional"] = candidate["tp_notional"]
            record["tp_exit_fill_price"] = candidate["tp_fill_price"]
            record["sl_exit_quantity"] = candidate["sl_quantity"]
            record["sl_exit_notional"] = candidate["sl_notional"]
            record["sl_exit_fill_price"] = candidate["sl_fill_price"]
            record["emergency_flatten_exit_quantity"] = candidate.get("emergency_flatten_exit_quantity", 0.0)
            record["emergency_flatten_exit_notional"] = candidate.get("emergency_flatten_exit_notional", 0.0)
            record["emergency_flatten_exit_fill_price"] = candidate.get("emergency_flatten_exit_fill_price")
            record["exit_reason"] = candidate["exit_reason"]
            record["execution_validation_status"] = "broker_reconciled_flat"
            if candidate["exit_reason"] == "TP":
                computed_next_state = "TP_FILLED"
            elif candidate["exit_reason"] == "SL":
                computed_next_state = "SL_FILLED"
            elif candidate["exit_reason"] == PROTECTIVE_EMERGENCY_EXIT_REASON:
                computed_next_state = "CLOSED"
                record["protective_emergency_status"] = "flat_confirmed_stale_orders_cleared"
                record["protective_emergency_active"] = False
            else:
                computed_next_state = "CLOSED"
            self.log_lifecycle_mutation_context(
                mutation_point="broker_fill_reconciliation_apply",
                reason_label=reason_label,
                trade_id=trade_id,
                symbol=record.get("symbol") if isinstance(record, dict) else None,
                previous_state=record.get("state") if isinstance(record, dict) else None,
                next_state=computed_next_state,
            )
            if candidate["exit_reason"] == "TP":
                record["state"] = "TP_FILLED"
            elif candidate["exit_reason"] == "SL":
                record["state"] = "SL_FILLED"
            else:
                record["state"] = "CLOSED"
            self.clear_partial_entry_timeout(record)
            self.append_trade_event(
                trade_id,
                f"BROKER FILL RECONCILIATION APPLIED "
                f"reason={reason_label} previous_state={previous_state} "
                f"entry_quantity={candidate['entry_quantity']} "
                f"exit_quantity={candidate['exit_quantity']} "
                f"exit_reason={candidate['exit_reason']}"
            )
            if candidate["exit_reason"] == PROTECTIVE_EMERGENCY_EXIT_REASON:
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_RECONSTRUCTED_FILL | "
                    f"trade_id={trade_id} "
                    f"symbol={record.get('symbol')} "
                    f"exit_quantity={candidate['exit_quantity']} "
                    f"exit_fill_price={candidate['exit_fill_price']} "
                    "exit_reason=EMERGENCY_FLATTEN_SL_MISSING "
                    "protective_emergency_status=flat_confirmed_stale_orders_cleared"
                )
            applied_log = {
                "trade_id": trade_id,
                "symbol": record.get("symbol"),
                "previous_state": previous_state,
                "state": record.get("state"),
                "entry_quantity": candidate["entry_quantity"],
                "exit_quantity": candidate["exit_quantity"],
                "exit_reason": candidate["exit_reason"],
                "broker_real": broker_reality.get("broker_real"),
                "open_trade_match": broker_reality.get("has_open_trade_match"),
                "open_order_match": broker_reality.get("has_open_order_match"),
                "position_match": broker_reality.get("has_position_match"),
            }

        logger.warning(
            "BROKER FILL RECONCILIATION APPLIED | "
            f"trade_id={applied_log['trade_id']} "
            f"symbol={applied_log['symbol']} "
            f"previous_state={applied_log['previous_state']} "
            f"state={applied_log['state']} "
            "decision=apply_and_finalize "
            f"entry_quantity={applied_log['entry_quantity']} "
            f"exit_quantity={applied_log['exit_quantity']} "
            f"exit_reason={applied_log['exit_reason']} "
            f"broker_real={applied_log['broker_real']} "
            f"open_trade_match={applied_log['open_trade_match']} "
            f"open_order_match={applied_log['open_order_match']} "
            f"position_match={applied_log['position_match']} "
            f"reason={reason_label}"
        )
        logger.warning(
            "MISSED EXIT FILL RECONSTRUCTED | "
            f"trade_id={applied_log['trade_id']} "
            f"symbol={applied_log['symbol']} "
            f"state={applied_log['state']} "
            "decision=finalize_from_broker_fills "
            f"entry_quantity={applied_log['entry_quantity']} "
            f"exit_quantity={applied_log['exit_quantity']} "
            f"exit_reason={applied_log['exit_reason']} "
            f"reason={reason_label}"
        )
        self.finalize_trade_if_complete(trade_id)
        return True

    def reconcile_trade_from_in_session_broker_fills(self, trade_id, reason_label):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None or record.get("summary_logged"):
                return False
            record_snapshot = dict(record)

        candidate = self.build_in_session_broker_fill_reconciliation_candidate(record_snapshot)
        if not candidate.get("ok"):
            reason = candidate.get("reason") or "candidate_unavailable"
            should_log, suppressed_count, suppressed = self.should_log_reconciliation_noise(
                record_snapshot,
                reason,
                "skipped",
            )
            if should_log:
                if suppressed and suppressed_count > 0:
                    logger.info(
                        "BROKER FILL RECONCILIATION SUPPRESSED SUMMARY | "
                        f"trade_id={trade_id} "
                        f"symbol={record_snapshot.get('symbol')} "
                        f"state={record_snapshot.get('state')} "
                        "log_category=expected_noop "
                        "decision=preserve_current_lifecycle "
                        f"reason={reason} "
                        "expected_resting_no_fills=true "
                        f"suppressed_count={suppressed_count}"
                    )
                elif suppressed:
                    logger.info(
                        "BROKER FILL RECONCILIATION SKIPPED | "
                        f"trade_id={trade_id} "
                        f"symbol={record_snapshot.get('symbol')} "
                        f"state={record_snapshot.get('state')} "
                        "log_category=expected_noop "
                        "decision=preserve_current_lifecycle "
                        f"entry_quantity={candidate.get('entry_quantity')} "
                        f"exit_quantity={candidate.get('exit_quantity')} "
                        f"exit_reason={candidate.get('exit_reason')} "
                        f"reason={reason} "
                        "expected_resting_no_fills=true"
                    )
                else:
                    logger.warning(
                        "BROKER FILL RECONCILIATION SKIPPED | "
                        f"trade_id={trade_id} "
                        f"symbol={record_snapshot.get('symbol')} "
                        f"state={record_snapshot.get('state')} "
                        "log_category=skipped "
                        "decision=preserve_current_lifecycle "
                        f"entry_quantity={candidate.get('entry_quantity')} "
                        f"exit_quantity={candidate.get('exit_quantity')} "
                        f"exit_reason={candidate.get('exit_reason')} "
                        f"reason={reason}"
                    )
            return False

        valid, validation_reason, broker_reality = self.validate_in_session_broker_fill_reconciliation_candidate(
            record_snapshot,
            candidate,
        )
        if not valid:
            logger.warning(
                "BROKER_FILL_RECONCILIATION_AMBIGUOUS | "
                f"trade_id={trade_id} "
                f"symbol={record_snapshot.get('symbol')} "
                "log_category=ambiguous "
                "decision=fail_closed_preserve_lifecycle "
                f"reason={validation_reason}"
            )
            logger.warning(
                "BROKER FILL RECONCILIATION SKIPPED | "
                f"trade_id={trade_id} "
                f"symbol={record_snapshot.get('symbol')} "
                f"state={record_snapshot.get('state')} "
                "log_category=validation_failure "
                "decision=fail_closed_preserve_lifecycle "
                f"entry_quantity={candidate.get('entry_quantity')} "
                f"exit_quantity={candidate.get('exit_quantity')} "
                f"exit_reason={candidate.get('exit_reason')} "
                f"broker_real={broker_reality.get('broker_real') if broker_reality else None} "
                f"open_trade_match={broker_reality.get('has_open_trade_match') if broker_reality else None} "
                f"open_order_match={broker_reality.get('has_open_order_match') if broker_reality else None} "
                f"position_match={broker_reality.get('has_position_match') if broker_reality else None} "
                f"reason={validation_reason}"
            )
            return False

        return self.apply_in_session_broker_fill_reconciliation(
            trade_id,
            candidate,
            reason_label,
            record_snapshot=record_snapshot,
            validation_reason=validation_reason,
            broker_reality=broker_reality,
        )

    def reconcile_active_trades_from_in_session_broker_fills(self, reason_label):
        with self.trade_analysis_lock:
            trade_ids = [
                record["trade_id"]
                for record in self.trade_analysis.values()
                if not record.get("summary_logged") and record.get("state") in ACTIVE_TRADE_STATES
            ]

        if trade_ids:
            logger.info(
                "BROKER FILL RECONCILIATION ACTIVE SWEEP | "
                f"reason={reason_label} "
                f"candidate_count={len(trade_ids)}"
            )
        reconciled_count = 0
        for trade_id in trade_ids:
            if self.reconcile_trade_from_in_session_broker_fills(trade_id, reason_label):
                reconciled_count += 1
        if trade_ids or reconciled_count:
            sweep_result_logger = logger.warning if reconciled_count > 0 else logger.info
            sweep_result_logger(
                "BROKER FILL RECONCILIATION ACTIVE SWEEP RESULT | "
                f"reason={reason_label} "
                f"candidate_count={len(trade_ids)} "
                f"reconciled_count={reconciled_count}"
            )
        return reconciled_count

    def reconcile_active_trade_lifecycle_from_broker_fills(self, reason_label):
        return self.reconcile_active_trades_from_in_session_broker_fills(reason_label)

    def post_reconnect_fill_reconstruction_sweep(self, reason_label, since_time=None):
        if not isinstance(since_time, datetime):
            since_time = self.get_reconnect_fill_reconstruction_since()

        try:
            fills = self.broker_read_fills(
                caller="post_reconnect_fill_reconstruction_sweep",
                reason_label=reason_label,
                trade_analysis_lock_context="not_locked",
            )
        except Exception as exc:
            logger.exception(
                "POST RECONNECT FILL RECONSTRUCTION FAILED | "
                f"reason={reason_label} failure={exc}"
            )
            return 0

        with self.trade_analysis_lock:
            active_records = [
                dict(record)
                for record in self.trade_analysis.values()
                if not record.get("summary_logged") and record.get("state") in ACTIVE_TRADE_STATES
            ]

        fills_inspected = 0
        exact_matches = 0
        fallback_matches = 0
        ambiguous_unmatched = 0
        unknown_unmatched = 0
        ambiguous_examples = []
        unknown_examples = []

        for fill in fills:
            execution = getattr(fill, "execution", None)
            if execution is None:
                continue
            fill_time, _, _ = self.get_effective_execution_time(fill, execution)
            if isinstance(since_time, datetime) and isinstance(fill_time, datetime):
                normalized_since = since_time.astimezone(timezone.utc) if since_time.tzinfo else since_time.replace(tzinfo=timezone.utc)
                normalized_fill_time = fill_time.astimezone(timezone.utc) if fill_time.tzinfo else fill_time.replace(tzinfo=timezone.utc)
                if normalized_fill_time < normalized_since:
                    continue

            fills_inspected += 1
            fill_exact = False
            fill_fallback = False
            fill_ambiguous = False
            for record in active_records:
                leg, match_source, fallback_candidates = self.resolve_reconstructed_fill_leg_with_fallback(
                    record,
                    fill,
                    execution,
                )
                if leg is None:
                    if match_source == "fallback_ambiguous_multiple_legs":
                        fill_ambiguous = True
                    continue
                if match_source in {"order_id", "perm_id"}:
                    fill_exact = True
                elif match_source == "fallback_symbol_side_time_quantity_price":
                    fill_fallback = True

            if fill_exact:
                exact_matches += 1
            elif fill_fallback:
                fallback_matches += 1
            elif fill_ambiguous:
                ambiguous_unmatched += 1
                if (
                    len(ambiguous_examples) < 3 and
                    self.should_log_reconnect_unmatched_fill_diagnostic(
                        fill,
                        execution,
                        "post_reconnect_fallback_ambiguous",
                    )
                ):
                    ambiguous_examples.append((fill, execution, getattr(fill, "contract", None)))
            else:
                unknown_unmatched += 1
                if (
                    len(unknown_examples) < 3 and
                    self.should_log_reconnect_unmatched_fill_diagnostic(
                        fill,
                        execution,
                        "post_reconnect_no_active_trade_match",
                    )
                ):
                    unknown_examples.append((fill, execution, getattr(fill, "contract", None)))

        repaired_count = self.reconcile_active_trades_from_in_session_broker_fills(
            f"{reason_label}:post_reconnect_fill_reconstruction"
        )
        for fill, execution, contract in ambiguous_examples:
            self.log_broker_fill_without_lifecycle_event(
                "UNKNOWN",
                "post_reconnect_fallback_ambiguous",
                "not_applied_during_sweep_sampled",
                fill=fill,
                execution=execution,
                contract=contract,
            )
        for fill, execution, contract in unknown_examples:
            self.log_broker_fill_without_lifecycle_event(
                "UNKNOWN",
                "post_reconnect_no_active_trade_match",
                "not_applied_during_sweep_sampled",
                fill=fill,
                execution=execution,
                contract=contract,
            )
        logger.warning(
            "POST RECONNECT FILL RECONSTRUCTION SWEEP | "
            f"reason={reason_label} "
            f"since_time={self.to_iso(since_time)} "
            f"fills_inspected={fills_inspected} "
            f"exact_matches={exact_matches} "
            f"fallback_matches={fallback_matches} "
            f"ambiguous_unmatched={ambiguous_unmatched} "
            f"unknown_unmatched={unknown_unmatched} "
            f"lifecycle_records_repaired={repaired_count} "
            f"sampled_ambiguous_examples_count={len(ambiguous_examples)} "
            f"sampled_unknown_examples_count={len(unknown_examples)}"
        )
        return repaired_count

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
            self.log_lifecycle_mutation_context(
                mutation_point="cleanup_stale_active_trade_lock",
                reason_label=reason_label,
                trade_id=trade_id,
                symbol=record.get("symbol") if isinstance(record, dict) else None,
                previous_state=record.get("state") if isinstance(record, dict) else None,
                next_state="INCOMPLETE",
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

    def cancel_unacknowledged_bracket_legs(self, trade_id, reason_label, broker_reality=None):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return {
                    "ok": False,
                    "reason": "trade_record_missing",
                    "cancel_requested_count": 0,
                    "cancel_failed_count": 0,
                    "visible_order_ids": [],
                    "failed_order_ids": [],
                }
            record_snapshot = dict(record)

        leg_order_ids = {
            "parent": record_snapshot.get("parent_order_id"),
            "tp": record_snapshot.get("tp_order_id"),
            "sl": record_snapshot.get("sl_order_id"),
        }
        expected_order_ids = {
            order_id
            for order_id in leg_order_ids.values()
            if order_id is not None
        }
        visible_orders = {}
        visible_order_ids = set()
        failed_order_ids = []
        cancel_requested_count = 0
        cancel_failed_count = 0

        try:
            open_trades, open_orders = self.broker_read_open_trades_open_orders(
                caller="cancel_unacknowledged_bracket_legs",
                reason_label=reason_label,
                trade_id=trade_id,
                symbol=record_snapshot.get("symbol") if isinstance(record_snapshot, dict) else None,
                trade_analysis_lock_context="not_locked",
            )
            for trade in open_trades:
                order = getattr(trade, "order", None)
                order_id = getattr(order, "orderId", None)
                if order_id in expected_order_ids and order_id not in visible_orders:
                    visible_orders[order_id] = order
                    visible_order_ids.add(order_id)

            for order in open_orders:
                order_id = getattr(order, "orderId", None)
                if order_id in expected_order_ids and order_id not in visible_orders:
                    visible_orders[order_id] = order
                    visible_order_ids.add(order_id)
        except Exception as exc:
            logger.exception(
                "UNACKNOWLEDGED_BRACKET_CANCEL_VISIBILITY_CHECK_FAILED | "
                f"trade_id={trade_id} "
                f"symbol={record_snapshot.get('symbol')} "
                f"reason={reason_label}"
            )
            return {
                "ok": False,
                "reason": f"visibility_check_failed:{exc}",
                "cancel_requested_count": 0,
                "cancel_failed_count": len(expected_order_ids),
                "visible_order_ids": sorted(visible_order_ids),
                "failed_order_ids": sorted(expected_order_ids),
            }

        if broker_reality is None:
            broker_reality = self.get_trade_broker_reality(
                record_snapshot,
                trade_analysis_lock_context="not_locked",
            )

        if self.should_suppress_unacknowledged_cleanup_for_protection(record_snapshot, broker_reality):
            visible_parent = leg_order_ids["parent"] in visible_order_ids
            visible_tp = leg_order_ids["tp"] in visible_order_ids
            visible_sl = leg_order_ids["sl"] in visible_order_ids
            if visible_sl:
                log_label = "PROTECTIVE_EXIT_CLEANUP_SUPPRESSED"
            elif visible_tp:
                log_label = "PROTECTIVE_SL_MISSING_WITH_POSITION"
            else:
                log_label = "URGENT_RISK_STATE_UNPROTECTED_POSITION"
            log_message = (
                f"{log_label} | "
                f"trade_id={trade_id} "
                f"symbol={record_snapshot.get('symbol')} "
                f"parent_order_id={record_snapshot.get('parent_order_id')} "
                f"tp_order_id={record_snapshot.get('tp_order_id')} "
                f"sl_order_id={record_snapshot.get('sl_order_id')} "
                f"entry_filled={record_snapshot.get('entry_filled')} "
                f"realized_entry_quantity={record_snapshot.get('realized_entry_quantity')} "
                f"cumulative_entry_quantity={record_snapshot.get('cumulative_entry_quantity')} "
                f"position_match={broker_reality.get('has_position_match')} "
                f"parent_visible={visible_parent} "
                f"tp_visible={visible_tp} "
                f"sl_visible={visible_sl} "
                f"visible_order_ids={sorted(visible_order_ids)} "
                f"matching_position_sizes={broker_reality.get('matching_position_sizes')} "
                "protective_context=True "
                "cleanup_forbidden=True "
                "decision=preserve_exits_no_generic_cleanup "
                f"reason={reason_label}"
            )
            if visible_sl:
                logger.warning(log_message)
            else:
                logger.error(log_message)
            self.append_trade_event(
                trade_id,
                f"{log_label} decision=preserve_exits_no_generic_cleanup "
                f"tp_visible={visible_tp} sl_visible={visible_sl} "
                f"visible_order_ids={sorted(visible_order_ids)} reason={reason_label}"
            )
            if not visible_sl and PROTECTIVE_SL_MISSING_POLICY == "EMERGENCY_FLATTEN":
                self.emergency_flatten_unprotected_position(
                    trade_id,
                    record_snapshot.get("symbol"),
                    f"cleanup_suppression_sl_missing:{reason_label}",
                    protection_context={
                        "protection_class": (
                            "URGENT_RISK_STATE_SL_MISSING"
                            if visible_tp
                            else "URGENT_RISK_STATE_UNPROTECTED_POSITION"
                        ),
                        "parent_visible": visible_parent,
                        "tp_visible": visible_tp,
                        "sl_visible": visible_sl,
                    },
                    broker_reality=broker_reality,
                )
            return {
                "ok": True,
                "reason": "protective_exit_cleanup_suppressed",
                "cancel_requested_count": 0,
                "cancel_failed_count": 0,
                "visible_order_ids": sorted(visible_order_ids),
                "failed_order_ids": [],
                "protective_cleanup_suppressed": True,
                "parent_visible": visible_parent,
                "tp_visible": visible_tp,
                "sl_visible": visible_sl,
            }

        for leg_name, order_id in leg_order_ids.items():
            if order_id is None:
                continue

            order = visible_orders.get(order_id)
            if order is None:
                logger.warning(
                    "UNACKNOWLEDGED_BRACKET_CANCEL_NOT_VISIBLE | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot.get('symbol')} "
                    f"leg={leg_name} "
                    f"order_id={order_id} "
                    f"reason={reason_label}"
                )
                self.append_trade_event(
                    trade_id,
                    f"UNACKNOWLEDGED BRACKET CANCEL NOT VISIBLE leg={leg_name} order_id={order_id} reason={reason_label}"
                )
                continue

            try:
                self.broker_write_cancel_order(
                    order,
                    caller="cancel_unacknowledged_bracket_legs",
                    reason_label=reason_label,
                    trade_id=trade_id,
                    symbol=record_snapshot.get("symbol"),
                    real_broker_exposure=True,
                )
                cancel_requested_count += 1
                logger.warning(
                    "UNACKNOWLEDGED_BRACKET_CANCEL_REQUESTED | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot.get('symbol')} "
                    f"leg={leg_name} "
                    f"order_id={order_id} "
                    f"reason={reason_label}"
                )
                self.append_trade_event(
                    trade_id,
                    f"UNACKNOWLEDGED BRACKET CANCEL REQUESTED leg={leg_name} order_id={order_id} reason={reason_label}"
                )
            except Exception as exc:
                cancel_failed_count += 1
                failed_order_ids.append(order_id)
                logger.exception(
                    "UNACKNOWLEDGED_BRACKET_CANCEL_FAILED | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot.get('symbol')} "
                    f"leg={leg_name} "
                    f"order_id={order_id} "
                    f"reason={reason_label}"
                )
                self.append_trade_event(
                    trade_id,
                    f"UNACKNOWLEDGED BRACKET CANCEL FAILED leg={leg_name} order_id={order_id} reason=cancel_exception:{exc}"
                )

        return {
            "ok": cancel_failed_count == 0,
            "reason": "cancel_requests_submitted" if cancel_failed_count == 0 else "cancel_request_failures",
            "cancel_requested_count": cancel_requested_count,
            "cancel_failed_count": cancel_failed_count,
            "visible_order_ids": sorted(visible_order_ids),
            "failed_order_ids": sorted(failed_order_ids),
        }

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
                record_snapshot["sl_order_id"],
                trade_analysis_lock_context="not_locked",
            )
            protection_context = self.classify_position_protection_context(
                record_snapshot,
                broker_reality,
                confirmation,
            )
            quantity_coverage = self.assess_bracket_quantity_coverage(
                record_snapshot,
                broker_confirmation=confirmation,
                broker_reality=broker_reality,
            )
            self.log_bracket_quantity_coverage(record_snapshot, quantity_coverage)
            broker_state_category = confirmation.get("broker_state_category", "BROKEN_OR_TERMINAL")
            if (
                broker_reality.get("check_failed")
                and (record_snapshot.get("entry_filled") or self.has_realized_parent_entry(record_snapshot))
            ):
                self.emergency_flatten_unprotected_position(
                    trade_id,
                    record_snapshot["symbol"],
                    "protective_emergency_post_reconnect_required_broker_read_failed",
                    protection_context=protection_context,
                    broker_reality=broker_reality,
                )
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_POST_RECONNECT_REQUIRED | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"reason={reason_label} "
                    "operator_action_required=True "
                    "decision=preserve_active_lock_until_broker_reality_available"
                )
                return True

            if protection_context["protection_class"] in {
                "IN_POSITION_WITH_PROTECTIVE_EXITS",
                "IN_POSITION_WITH_PRIMARY_SL_PROTECTION",
            }:
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is None or record["summary_logged"] or record["state"] != "BROKER_ACK_PENDING":
                        return False
                    previous_state = record["state"]
                    record["state"] = "EXIT_WORKING"
                    record["broker_ack_pending_last_check"] = now_dt
                    record["broker_ack_pending_reason"] = protection_context["reason"]
                    record["broker_ack_pending_category"] = broker_state_category
                    record["broker_ack_pending_visible_order_ids"] = confirmation["visible_order_ids"]
                    self.append_trade_event(
                        trade_id,
                        f"BROKER ACK PENDING RECOVERED TO EXIT WORKING previous_state={previous_state} "
                        f"broker_state_category={broker_state_category} "
                        f"protection_class={protection_context['protection_class']} "
                        f"reason={protection_context['reason']} "
                        f"visible_order_ids={confirmation['visible_order_ids']}"
                    )
                self.set_execution_validation_status(
                    trade_id,
                    "broker_live" if confirmation.get("all_broker_live") else "broker_acknowledged",
                    protection_context["reason"],
                )
                logger.warning(
                    "BROKER_ACK_PENDING_RECOVERED_TO_EXIT_WORKING | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"parent_order_id={record_snapshot.get('parent_order_id')} "
                    f"tp_order_id={record_snapshot.get('tp_order_id')} "
                    f"sl_order_id={record_snapshot.get('sl_order_id')} "
                    f"parent_visible={protection_context['parent_visible']} "
                    f"tp_visible={protection_context['tp_visible']} "
                    f"sl_visible={protection_context['sl_visible']} "
                    f"entry_filled={record_snapshot.get('entry_filled')} "
                    f"cumulative_entry_quantity={record_snapshot.get('cumulative_entry_quantity')} "
                    f"realized_entry_quantity={record_snapshot.get('realized_entry_quantity')} "
                    f"position_match={protection_context['position_match']} "
                    f"position_sizes={broker_reality.get('matching_position_sizes')} "
                    f"visible_order_ids={confirmation['visible_order_ids']} "
                    f"broker_state_category={broker_state_category} "
                    f"protection_class={protection_context['protection_class']} "
                    "decision=preserve_protective_exits "
                    f"reason={protection_context['reason']}"
                )
                if not protection_context["parent_visible"]:
                    logger.info(
                        "PARENT_MISSING_AFTER_FULL_FILL_NORMAL | "
                        f"trade_id={trade_id} "
                        f"symbol={record_snapshot['symbol']} "
                        f"parent_order_id={record_snapshot.get('parent_order_id')} "
                        f"tp_order_id={record_snapshot.get('tp_order_id')} "
                        f"sl_order_id={record_snapshot.get('sl_order_id')} "
                        f"visible_order_ids={confirmation['visible_order_ids']} "
                        "decision=parent_absence_accepted_after_realized_entry"
                    )
                return True

            if protection_context["protection_class"] in {
                "URGENT_RISK_STATE_SL_MISSING",
                "URGENT_RISK_STATE_UNPROTECTED_POSITION",
            }:
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is None or record["summary_logged"] or record["state"] != "BROKER_ACK_PENDING":
                        return False
                    previous_state = record["state"]
                    record["state"] = protection_context["target_state"] or record["state"]
                    record["broker_ack_pending_last_check"] = now_dt
                    record["broker_ack_pending_reason"] = protection_context["reason"]
                    record["broker_ack_pending_category"] = broker_state_category
                    record["broker_ack_pending_visible_order_ids"] = confirmation["visible_order_ids"]
                    self.append_trade_event(
                        trade_id,
                        f"{protection_context['protection_class']} previous_state={previous_state} "
                        f"new_state={record['state']} broker_state_category={broker_state_category} "
                        f"visible_order_ids={confirmation['visible_order_ids']} "
                        f"reason={protection_context['reason']}"
                    )
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    protection_context["reason"],
                )
                log_label = (
                    "PROTECTIVE_SL_MISSING_WITH_POSITION"
                    if protection_context["protection_class"] == "URGENT_RISK_STATE_SL_MISSING"
                    else "URGENT_RISK_STATE_UNPROTECTED_POSITION"
                )
                logger.critical(
                    f"{log_label} | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"parent_order_id={record_snapshot.get('parent_order_id')} "
                    f"tp_order_id={record_snapshot.get('tp_order_id')} "
                    f"sl_order_id={record_snapshot.get('sl_order_id')} "
                    f"parent_visible={protection_context['parent_visible']} "
                    f"tp_visible={protection_context['tp_visible']} "
                    f"sl_visible={protection_context['sl_visible']} "
                    f"entry_filled={record_snapshot.get('entry_filled')} "
                    f"cumulative_entry_quantity={record_snapshot.get('cumulative_entry_quantity')} "
                    f"realized_entry_quantity={record_snapshot.get('realized_entry_quantity')} "
                    f"position_match={protection_context['position_match']} "
                    f"position_sizes={broker_reality.get('matching_position_sizes')} "
                    f"visible_order_ids={confirmation['visible_order_ids']} "
                    f"broker_state_category={broker_state_category} "
                    f"protection_class={protection_context['protection_class']} "
                    "decision=preserve_active_unresolved_no_generic_cleanup "
                    "operator_action_required=True "
                    f"reason={protection_context['reason']}"
                )
                self.emergency_flatten_unprotected_position(
                    trade_id,
                    record_snapshot["symbol"],
                    protection_context["reason"],
                    protection_context=protection_context,
                    broker_reality=broker_reality,
                )
                return True

            broken_or_terminal_fail_closed = False
            broken_or_terminal_previous_state = None
            quantity_gate_emergency_required = False
            quantity_gate_hold = False
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is None or record["summary_logged"] or record["state"] != "BROKER_ACK_PENDING":
                    return False
                record["broker_ack_pending_last_check"] = now_dt
                record["broker_ack_pending_reason"] = confirmation["reason"]
                record["broker_ack_pending_category"] = confirmation.get("broker_state_category")
                record["broker_ack_pending_visible_order_ids"] = confirmation["visible_order_ids"]
                if broker_state_category == "ACKNOWLEDGED" and not self.is_bracket_quantity_safe_for_ack(quantity_coverage):
                    open_position_estimate = quantity_coverage.get("open_position_estimate")
                    previous_state = record["state"]
                    record["broker_ack_pending_last_check"] = now_dt
                    record["broker_ack_pending_reason"] = quantity_coverage.get("quantity_state")
                    record["broker_ack_pending_category"] = broker_state_category
                    record["broker_ack_pending_visible_order_ids"] = confirmation["visible_order_ids"]
                    self.append_trade_event(
                        trade_id,
                        f"BROKER ACK PENDING QUANTITY GATE HOLDS previous_state={previous_state} "
                        f"quantity_state={quantity_coverage.get('quantity_state')} "
                        f"open_position_estimate={open_position_estimate} "
                        f"protective_sl_coverage={quantity_coverage.get('protective_sl_coverage')}"
                    )
                    if open_position_estimate is not None and open_position_estimate > 0:
                        quantity_gate_emergency_required = True
                    else:
                        quantity_gate_hold = True
                if quantity_gate_emergency_required or quantity_gate_hold:
                    pass
                elif broker_state_category == "ACKNOWLEDGED":
                    previous_state = record["state"]
                    self.log_lifecycle_mutation_context(
                        mutation_point="broker_ack_pending_recovery_exit",
                        reason_label=reason_label,
                        trade_id=trade_id,
                        symbol=record.get("symbol") if isinstance(record, dict) else None,
                        previous_state=record.get("state") if isinstance(record, dict) else None,
                        next_state="ENTRY_WORKING",
                    )
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
                    self.log_lifecycle_mutation_context(
                        mutation_point="broker_ack_pending_recovery_fail_closed",
                        reason_label=reason_label,
                        trade_id=trade_id,
                        symbol=record.get("symbol") if isinstance(record, dict) else None,
                        previous_state=record.get("state") if isinstance(record, dict) else None,
                        next_state="INCOMPLETE",
                    )
                    record["state"] = "INCOMPLETE"
                    self.append_trade_event(
                        trade_id,
                        f"BROKER ACK PENDING RECOVERY FAIL-CLOSED previous_state={broken_or_terminal_previous_state} "
                        f"new_state={record['state']} broker_state_category={broker_state_category} "
                        f"reason={confirmation['reason']}"
                    )
                    broken_or_terminal_fail_closed = True

            if quantity_gate_emergency_required:
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    quantity_coverage.get("quantity_state"),
                )
                self.emergency_flatten_unprotected_position(
                    trade_id,
                    record_snapshot["symbol"],
                    quantity_coverage.get("quantity_state"),
                    protection_context=protection_context,
                    broker_reality=broker_reality,
                )
                return True
            if quantity_gate_hold:
                return False

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
                cleanup_reason = "broker_ack_pending_timeout_unacknowledged_bracket_cleanup"
                cancel_result = self.cancel_unacknowledged_bracket_legs(
                    trade_id,
                    cleanup_reason,
                    broker_reality=broker_reality,
                )
                if cancel_result.get("protective_cleanup_suppressed"):
                    with self.trade_analysis_lock:
                        record = self.trade_analysis.get(trade_id)
                        if record is not None and not record["summary_logged"] and record["state"] == "BROKER_ACK_PENDING":
                            previous_state = record["state"]
                            if cancel_result.get("sl_visible"):
                                record["state"] = "EXIT_WORKING"
                            record["broker_ack_pending_last_check"] = now_dt
                            record["broker_ack_pending_reason"] = cancel_result["reason"]
                            record["broker_ack_pending_category"] = broker_state_category
                            record["broker_ack_pending_visible_order_ids"] = cancel_result["visible_order_ids"]
                            self.append_trade_event(
                                trade_id,
                                f"BROKER ACK PENDING PROTECTIVE CLEANUP SUPPRESSED previous_state={previous_state} "
                                f"new_state={record['state']} broker_state_category={broker_state_category} "
                                f"pending_age_sec={pending_age} "
                                f"visible_order_ids={cancel_result['visible_order_ids']}"
                            )
                    logger.warning(
                        "BROKER_ACK_PENDING_PROTECTIVE_CLEANUP_SUPPRESSED | "
                        f"trade_id={trade_id} "
                        f"symbol={record_snapshot['symbol']} "
                        f"broker_state_category={broker_state_category} "
                        f"pending_age_sec={pending_age} "
                        f"tp_visible={cancel_result.get('tp_visible')} "
                        f"sl_visible={cancel_result.get('sl_visible')} "
                        f"visible_order_ids={cancel_result['visible_order_ids']} "
                        "decision=preserve_active_protection "
                        f"reason={cleanup_reason}"
                    )
                    return True
                self.append_anomaly(trade_id, "BRACKET_UNACKNOWLEDGED_TIMEOUT_CLEANUP_REQUESTED")
                self.set_execution_validation_status(
                    trade_id,
                    "validation_incomplete",
                    "broker_ack_pending_timeout_unacknowledged_bracket"
                )
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None and not record["summary_logged"] and record["state"] == "BROKER_ACK_PENDING":
                        previous_state = record["state"]
                        record["state"] = "INCOMPLETE"
                        record["broker_ack_pending_last_check"] = now_dt
                        record["broker_ack_pending_reason"] = cleanup_reason
                        record["broker_ack_pending_category"] = broker_state_category
                        record["broker_ack_pending_visible_order_ids"] = confirmation["visible_order_ids"]
                        self.append_trade_event(
                            trade_id,
                            f"BRACKET UNACKNOWLEDGED TIMEOUT CLEANUP previous_state={previous_state} "
                            f"new_state={record['state']} broker_state_category={broker_state_category} "
                            f"pending_age_sec={pending_age} "
                            f"cancel_requested_count={cancel_result['cancel_requested_count']} "
                            f"cancel_failed_count={cancel_result['cancel_failed_count']} "
                            f"visible_order_ids={cancel_result['visible_order_ids']}"
                        )
                logger.warning(
                    "BRACKET_UNACKNOWLEDGED_CLEANUP_REQUESTED | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"broker_state_category={broker_state_category} "
                    f"pending_age_sec={pending_age} "
                    f"visible_order_ids={confirmation['visible_order_ids']} "
                    f"cancel_requested_count={cancel_result['cancel_requested_count']} "
                    f"cancel_failed_count={cancel_result['cancel_failed_count']} "
                    f"reason={cleanup_reason}"
                )
                logger.error(
                    "BRACKET_SINGLE_ENTRY_RISK_FAIL_CLOSED | "
                    f"trade_id={trade_id} "
                    f"symbol={record_snapshot['symbol']} "
                    f"parent_order_id={record_snapshot.get('parent_order_id')} "
                    f"tp_order_id={record_snapshot.get('tp_order_id')} "
                    f"sl_order_id={record_snapshot.get('sl_order_id')} "
                    f"broker_state_category={broker_state_category} "
                    f"visible_order_ids={confirmation['visible_order_ids']} "
                    f"visible_statuses={confirmation.get('visible_statuses')} "
                    f"reason=broker_ack_pending_timeout_unacknowledged_bracket"
                )
                self.finalize_trade_if_complete(trade_id)
                return True
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

        if stage == "TST":
            stage_timeout_decision = "mark_incomplete_tst_no_broker_reality"
        elif stage == "ACC":
            stage_timeout_decision = "mark_incomplete_acc_no_broker_reality"
        else:
            stage_timeout_decision = "mark_incomplete_prd_no_broker_reality"

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

        if stage == "TST":
            policy = {
                "stage": stage,
                "mode": "unrestricted",
                "description": "TST allows different-symbol concurrency, but same-symbol active execution remains hard-blocked",
            }
        elif stage == "ACC":
            policy = {
                "stage": stage,
                "mode": "per_instrument",
                "description": "ACC allows different symbols in parallel and blocks same-symbol active trade conflicts",
            }
        else:
            policy = {
                "stage": stage,
                "mode": "global_single",
                "description": "PRD blocks on any real active trade globally",
            }

        logger.info(
            "CONCURRENCY POLICY | "
            f"stage={policy['stage']} "
            f"mode={policy['mode']} "
            f"description={policy['description']}"
        )
        return policy

    def get_active_trade_candidates(self, reason_label="UNSPECIFIED"):
        # Intentionally mutating pre-check: active-candidate discovery first reconciles broker fills/lifecycle
        # so stale locks can be safely released before concurrency decisions.
        self.log_lifecycle_mutation_context(
            mutation_point="active_candidate_discovery_pre_reconciliation",
            reason_label=reason_label,
        )
        self.reconcile_active_trade_lifecycle_from_broker_fills(
            f"{reason_label}:pre_active_lock"
        )

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
                    "exit_reason": record.get("exit_reason"),
                    "exit_fill_price": record.get("exit_fill_price"),
                    "tp_exit_quantity": record.get("tp_exit_quantity"),
                    "sl_exit_quantity": record.get("sl_exit_quantity"),
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
            broker_reality = self.get_trade_broker_reality(
                record_snapshot,
                trade_analysis_lock_context="not_locked",
            )

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
                    "SYMBOL AMBIGUITY RELEASE POLICY | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    "release_policy=symbol_ambiguity_only "
                    "symbol_match=true "
                    "direct_open_trade_match=false "
                    "direct_broker_match=false "
                    "open_order_match=false "
                    "position_match=false "
                    "decision=release "
                    "risk_note=release_despite_symbol_level_ambiguity "
                    f"reason_label={reason_label}"
                )
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

            if self.has_exit_complete_child_cleanup(record_snapshot):
                entry_quantity = self.get_lifecycle_entry_quantity(record_snapshot)
                exit_quantity = float(
                    record_snapshot.get("realized_exit_quantity")
                    or record_snapshot.get("cumulative_exit_quantity")
                    or 0.0
                )
                logger.warning(
                    "OCO SIBLING CANCEL OBSERVED AFTER EXIT | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    "decision=finalize_exit_complete_trade "
                    f"entry_quantity={entry_quantity} "
                    f"exit_quantity={exit_quantity} "
                    f"exit_reason={record_snapshot.get('exit_reason')} "
                    f"broker_real={broker_reality['broker_real']} "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    f"reason={reason_label}"
                )
                logger.warning(
                    "EXIT COMPLETE CHILD CLEANUP CONFIRMED | "
                    f"trade_id={record_snapshot['trade_id']} "
                    f"symbol={record_snapshot['symbol']} "
                    f"state={record_snapshot['state']} "
                    "decision=finalize_without_stale_cleanup "
                    f"entry_quantity={entry_quantity} "
                    f"exit_quantity={exit_quantity} "
                    f"exit_reason={record_snapshot.get('exit_reason')} "
                    f"broker_real={broker_reality['broker_real']} "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    f"reason={reason_label}"
                )
                self.finalize_trade_if_complete(record_snapshot["trade_id"])
                with self.trade_analysis_lock:
                    refreshed_record = self.trade_analysis.get(record_snapshot["trade_id"])
                    if refreshed_record is None or refreshed_record["summary_logged"]:
                        continue

            if record_snapshot["state"] in ACTIVE_TRADE_STATES:
                if record_snapshot["state"] == "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL":
                    if self.reconcile_trade_from_in_session_broker_fills(
                        record_snapshot["trade_id"],
                        f"{reason_label}:partial_timeout_parent_finality_active_lock"
                    ):
                        logger.warning(
                            "ACTIVE LOCK RELEASED BY BROKER FILL RECONCILIATION | "
                            f"trade_id={record_snapshot['trade_id']} "
                            f"symbol={record_snapshot['symbol']} "
                            f"reason={reason_label}"
                        )
                        continue

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
                        "reason=tst_stage_unrestricted_concurrency"
                    )
            else:
                logger.info(
                    "CONCURRENCY CHECK | "
                    f"stage={policy['stage']} "
                    f"incoming_symbol={incoming_symbol} "
                    "decision=allow "
                    "reason=tst_stage_unrestricted_concurrency_no_active_candidates"
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
                "reason=prd_global_single_active_trade"
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
                trade_id, record = self.get_trade_by_emergency_flatten_perm_id(perm_id)
            if record is None:
                self.log_broker_fill_without_lifecycle_event(
                    "UNKNOWN",
                    "no_trade_analysis_order_id_match",
                    "update_trade_from_status_skipped_record_missing",
                    fill=None,
                    execution=None,
                    contract=getattr(trade, "contract", None),
                    order=order,
                )
                return

            self.update_trade_perm_id(order_id, perm_id)
            if order_id == record.get("emergency_flatten_order_id"):
                with self.trade_analysis_lock:
                    live_record = self.trade_analysis.get(trade_id)
                    if live_record is not None:
                        live_record["emergency_flatten_last_status"] = state
                        emergency_already_resolved = bool(
                            live_record.get("closed")
                            and live_record.get("protective_emergency_status") == "flat_confirmed_stale_orders_cleared"
                        )
                        if state in PROTECTIVE_EMERGENCY_TERMINAL_STATUSES:
                            live_record["emergency_flatten_terminal_status"] = state
                            if not emergency_already_resolved:
                                live_record["protective_emergency_status"] = "flatten_order_terminal_not_flat_confirmed"
                        elif state in {"PendingSubmit", "ApiPending", "PreSubmitted", "Submitted", "PendingCancel"}:
                            if not emergency_already_resolved:
                                live_record["protective_emergency_status"] = "in_flight"
                        elif state == "Filled":
                            if not emergency_already_resolved:
                                live_record["protective_emergency_status"] = "pending_position_confirmation"
                        if not emergency_already_resolved:
                            live_record["state"] = "BROKER_ACK_PENDING"
                        self.append_trade_event(
                            trade_id,
                            f"PROTECTIVE EMERGENCY FLATTEN STATUS orderId={order_id} status={state}"
                        )
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_ORDER_STATUS | "
                    f"trade_id={trade_id} "
                    f"symbol={record.get('symbol')} "
                    f"emergency_flatten_order_id={order_id} "
                    f"emergency_flatten_perm_id={perm_id} "
                    f"emergency_flatten_last_status={state} "
                    f"protective_emergency_status={(self.trade_analysis.get(trade_id) or {}).get('protective_emergency_status')} "
                    f"protective_emergency_active={(self.trade_analysis.get(trade_id) or {}).get('protective_emergency_active')} "
                    "reason=order_status_event "
                    "operator_action_required=False"
                )
            self.mark_trade_state(order_id, state)
            leg = self.map_order_leg(record, order_id)
            self.log_order_status_correlation(
                record,
                order_id,
                perm_id,
                leg,
                state,
                getattr(status, "filled", None),
                getattr(status, "remaining", None),
                getattr(status, "avgFillPrice", None),
                getattr(order, "action", None),
                datetime.now(timezone.utc),
            )
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

            if (
                state in ("Cancelled", "ApiCancelled", "Inactive") and
                order_id in (record["tp_order_id"], record["sl_order_id"]) and
                self.has_exit_complete_child_cleanup(record)
            ):
                entry_quantity = self.get_lifecycle_entry_quantity(record)
                exit_quantity = float(
                    record.get("realized_exit_quantity")
                    or record.get("cumulative_exit_quantity")
                    or 0.0
                )
                broker_reality = self.get_trade_broker_reality(
                    dict(record),
                    trade_analysis_lock_context="not_locked",
                )
                logger.warning(
                    "OCO SIBLING CANCEL OBSERVED AFTER EXIT | "
                    f"trade_id={trade_id} "
                    f"symbol={record['symbol']} "
                    f"state={record['state']} "
                    "decision=finalize_if_flat "
                    f"entry_quantity={entry_quantity} "
                    f"exit_quantity={exit_quantity} "
                    f"exit_reason={record.get('exit_reason')} "
                    f"broker_real={broker_reality['broker_real']} "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    f"reason=order_status_child_cancel_after_exit"
                )
                logger.warning(
                    "EXIT COMPLETE CHILD CLEANUP CONFIRMED | "
                    f"trade_id={trade_id} "
                    f"symbol={record['symbol']} "
                    f"state={record['state']} "
                    "decision=avoid_stale_cleanup "
                    f"entry_quantity={entry_quantity} "
                    f"exit_quantity={exit_quantity} "
                    f"exit_reason={record.get('exit_reason')} "
                    f"broker_real={broker_reality['broker_real']} "
                    f"open_trade_match={broker_reality['has_open_trade_match']} "
                    f"open_order_match={broker_reality['has_open_order_match']} "
                    f"position_match={broker_reality['has_position_match']} "
                    f"reason=order_status_child_cancel_after_exit"
                )
                self.finalize_trade_if_complete(trade_id)
                return

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
            ("emergency_flatten_exit_fill_price", "emergency_flatten_exit_quantity"),
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

    def normalize_broker_quantity_or_none(self, value):
        try:
            if value is None:
                return None
            quantity = abs(float(value))
            if quantity < 1e-9:
                return 0.0
            return quantity
        except Exception:
            return None

    def subtract_known_quantities(self, first, *others):
        if first is None or any(value is None for value in others):
            return None
        result = float(first)
        for value in others:
            result -= float(value)
        return max(result, 0.0)

    def get_broker_confirmation_leg_quantity(self, broker_confirmation, leg_name, quantity_name):
        if not isinstance(broker_confirmation, dict):
            return None
        leg_data = (broker_confirmation.get("broker_orders") or {}).get(leg_name) or {}
        return self.normalize_broker_quantity_or_none(leg_data.get(quantity_name))

    def get_broker_confirmation_leg_value(self, broker_confirmation, leg_name, value_name):
        if not isinstance(broker_confirmation, dict):
            return None
        leg_data = (broker_confirmation.get("broker_orders") or {}).get(leg_name) or {}
        return leg_data.get(value_name)

    def build_quantity_coverage_log_fields(self, record, coverage):
        fields = {
            "trade_id": (record or {}).get("trade_id"),
            "symbol": (record or {}).get("symbol"),
        }
        for key in (
            "quantity_state",
            "open_position_estimate",
            "protective_sl_coverage",
            "planned_parent_quantity",
            "planned_tp_quantity",
            "planned_sl_quantity",
            "parent_total_quantity",
            "parent_filled_quantity",
            "parent_remaining_quantity",
            "tp_total_quantity",
            "tp_filled_quantity",
            "tp_remaining_quantity",
            "sl_total_quantity",
            "sl_filled_quantity",
            "sl_remaining_quantity",
            "quantity_mismatch_reason",
        ):
            fields[key] = (coverage or {}).get(key)
        return fields

    def log_bracket_quantity_coverage(self, record, coverage):
        quantity_state = (coverage or {}).get("quantity_state")
        if quantity_state in {"FULL_FILL_COVERAGE_OK", "NO_OPEN_EXPOSURE_QUANTITY_OK"}:
            label = "BRACKET_QUANTITY_COVERAGE_OK"
            log_fn = logger.info
        elif quantity_state == "TRANSIENT_PARTIAL_FILL_COVERED":
            label = "BRACKET_QUANTITY_COVERAGE_TRANSIENT_PARTIAL_FILL"
            log_fn = logger.warning
        elif quantity_state == "NO_OPEN_EXPOSURE_QUANTITY_UNKNOWN":
            label = "BRACKET_QUANTITY_UNKNOWN_WAITING"
            log_fn = logger.warning
        elif quantity_state == "BROKER_QUANTITY_UNKNOWN_FAIL_CLOSED":
            label = "BRACKET_QUANTITY_UNKNOWN_FAIL_CLOSED"
            log_fn = logger.critical
        elif quantity_state == "PROTECTIVE_COVERAGE_INSUFFICIENT":
            label = "PROTECTIVE_COVERAGE_INSUFFICIENT"
            log_fn = logger.critical
        elif quantity_state == "BRACKET_QUANTITY_MISMATCH":
            label = "BRACKET_QUANTITY_MISMATCH"
            log_fn = logger.critical
        else:
            label = "BRACKET_QUANTITY_MISMATCH"
            log_fn = logger.warning

        log_fn(f"{label} | {json.dumps(self.build_quantity_coverage_log_fields(record, coverage), sort_keys=True)}")

    def assess_bracket_quantity_coverage(self, record, broker_confirmation=None, broker_reality=None):
        record = record or {}
        planned_parent_quantity = self.normalize_broker_quantity_or_none(record.get("planned_parent_quantity"))
        planned_tp_quantity = self.normalize_broker_quantity_or_none(record.get("planned_tp_quantity"))
        planned_sl_quantity = self.normalize_broker_quantity_or_none(record.get("planned_sl_quantity"))

        parent_total_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "parent", "totalQuantity")
        parent_filled_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "parent", "filled")
        parent_remaining_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "parent", "remaining")
        tp_total_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "tp", "totalQuantity")
        tp_filled_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "tp", "filled")
        tp_remaining_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "tp", "remaining")
        sl_total_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "sl", "totalQuantity")
        sl_filled_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "sl", "filled")
        sl_remaining_quantity = self.get_broker_confirmation_leg_quantity(broker_confirmation, "sl", "remaining")

        cumulative_entry_quantity = self.normalize_broker_quantity_or_none(record.get("cumulative_entry_quantity"))
        cumulative_exit_quantity = self.normalize_broker_quantity_or_none(record.get("cumulative_exit_quantity"))
        broker_position_quantity = None
        if isinstance(broker_reality, dict) and broker_reality.get("broker_position_quantity") is not None:
            broker_position_quantity = abs(float(broker_reality.get("broker_position_quantity")))

        exposure_candidates = []
        if broker_position_quantity is not None:
            exposure_candidates.append(broker_position_quantity)
        record_exposure = self.subtract_known_quantities(cumulative_entry_quantity, cumulative_exit_quantity)
        if record_exposure is not None:
            exposure_candidates.append(record_exposure)
        broker_fill_exposure = self.subtract_known_quantities(
            parent_filled_quantity,
            tp_filled_quantity,
            sl_filled_quantity,
        )
        if broker_fill_exposure is not None:
            exposure_candidates.append(broker_fill_exposure)

        open_position_estimate = max(exposure_candidates) if exposure_candidates else None
        if open_position_estimate is not None and open_position_estimate < 1e-9:
            open_position_estimate = 0.0

        protective_sl_coverage = sl_remaining_quantity
        if protective_sl_coverage is None:
            protective_sl_coverage = self.subtract_known_quantities(sl_total_quantity, sl_filled_quantity)

        mismatch_reasons = []
        for leg_name, planned_quantity, total_quantity in (
            ("parent", planned_parent_quantity, parent_total_quantity),
            ("tp", planned_tp_quantity, tp_total_quantity),
            ("sl", planned_sl_quantity, sl_total_quantity),
        ):
            if planned_quantity is not None and total_quantity is not None and abs(planned_quantity - total_quantity) > 1e-9:
                mismatch_reasons.append(f"{leg_name}_planned_total_mismatch")

        child_quantities_known = (
            (tp_total_quantity is not None or tp_remaining_quantity is not None) and
            (sl_total_quantity is not None or sl_remaining_quantity is not None)
        )
        child_quantities_consistent = not any(
            reason.endswith("_planned_total_mismatch")
            for reason in mismatch_reasons
            if reason.startswith("tp_") or reason.startswith("sl_")
        )

        broker_has_real_signal = bool(
            broker_reality and (
                broker_reality.get("broker_real")
                or broker_reality.get("has_position_match")
                or broker_reality.get("has_open_trade_match")
                or broker_reality.get("has_open_order_match")
            )
        ) or bool(broker_confirmation and broker_confirmation.get("any_visible"))

        quantity_state = "BROKER_QUANTITY_UNKNOWN_FAIL_CLOSED"
        quantity_coverage_ok = False
        protective_sl_coverage_ok = False
        quantity_mismatch_reason = ",".join(mismatch_reasons) if mismatch_reasons else None

        if open_position_estimate is None:
            if broker_has_real_signal:
                quantity_state = "BROKER_QUANTITY_UNKNOWN_FAIL_CLOSED"
                quantity_mismatch_reason = quantity_mismatch_reason or "open_exposure_unknown_with_broker_visibility"
            else:
                quantity_state = "NO_OPEN_EXPOSURE_QUANTITY_UNKNOWN"
                quantity_mismatch_reason = quantity_mismatch_reason or "open_exposure_unknown_no_broker_visibility"
        elif open_position_estimate == 0.0:
            if mismatch_reasons:
                quantity_state = "BRACKET_QUANTITY_MISMATCH"
                quantity_mismatch_reason = ",".join(mismatch_reasons)
            elif child_quantities_known and child_quantities_consistent:
                quantity_state = "NO_OPEN_EXPOSURE_QUANTITY_OK"
                quantity_coverage_ok = True
                protective_sl_coverage_ok = True
            else:
                quantity_state = "NO_OPEN_EXPOSURE_QUANTITY_UNKNOWN"
                quantity_mismatch_reason = quantity_mismatch_reason or "no_open_exposure_child_quantities_unknown"
        elif protective_sl_coverage is None:
            quantity_state = "BROKER_QUANTITY_UNKNOWN_FAIL_CLOSED"
            quantity_mismatch_reason = quantity_mismatch_reason or "open_exposure_sl_coverage_unknown"
        elif protective_sl_coverage + 1e-9 >= open_position_estimate:
            protective_sl_coverage_ok = True
            quantity_coverage_ok = True
            parent_total_known = parent_total_quantity if parent_total_quantity is not None else planned_parent_quantity
            if parent_total_known is not None and parent_filled_quantity is not None and parent_filled_quantity + 1e-9 < parent_total_known:
                quantity_state = "TRANSIENT_PARTIAL_FILL_COVERED"
            else:
                quantity_state = "FULL_FILL_COVERAGE_OK"
        else:
            quantity_state = "PROTECTIVE_COVERAGE_INSUFFICIENT"
            quantity_mismatch_reason = quantity_mismatch_reason or "sl_coverage_below_open_exposure"

        return {
            "quantity_state": quantity_state,
            "quantity_coverage_ok": quantity_coverage_ok,
            "protective_sl_coverage_ok": protective_sl_coverage_ok,
            "quantity_mismatch_reason": quantity_mismatch_reason,
            "planned_parent_quantity": planned_parent_quantity,
            "planned_tp_quantity": planned_tp_quantity,
            "planned_sl_quantity": planned_sl_quantity,
            "parent_total_quantity": parent_total_quantity,
            "parent_filled_quantity": parent_filled_quantity,
            "parent_remaining_quantity": parent_remaining_quantity,
            "tp_total_quantity": tp_total_quantity,
            "tp_filled_quantity": tp_filled_quantity,
            "tp_remaining_quantity": tp_remaining_quantity,
            "sl_total_quantity": sl_total_quantity,
            "sl_filled_quantity": sl_filled_quantity,
            "sl_remaining_quantity": sl_remaining_quantity,
            "cumulative_entry_quantity": cumulative_entry_quantity,
            "cumulative_exit_quantity": cumulative_exit_quantity,
            "broker_position_quantity": broker_position_quantity,
            "open_position_estimate": open_position_estimate,
            "protective_sl_coverage": protective_sl_coverage,
        }

    def is_bracket_quantity_safe_for_ack(self, coverage):
        return bool(
            coverage and (
                coverage.get("quantity_coverage_ok")
                or coverage.get("quantity_state") in {
                    "NO_OPEN_EXPOSURE_QUANTITY_OK",
                    "TRANSIENT_PARTIAL_FILL_COVERED",
                    "FULL_FILL_COVERAGE_OK",
                }
            )
        )

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

    def is_position_with_protective_exit_context(self, record, broker_reality):
        return bool(
            record and
            broker_reality and
            (record.get("entry_filled") or self.has_realized_parent_entry(record)) and
            broker_reality.get("has_position_match")
        )

    def should_suppress_unacknowledged_cleanup_for_protection(self, record, broker_reality):
        return self.is_position_with_protective_exit_context(record, broker_reality)

    def get_datetime_age_seconds(self, value, now_value=None):
        if not isinstance(value, datetime):
            return None
        if now_value is None:
            now_value = datetime.now(timezone.utc)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        if now_value.tzinfo is None:
            now_value = now_value.replace(tzinfo=timezone.utc)
        else:
            now_value = now_value.astimezone(timezone.utc)
        return max(0.0, round((now_value - value).total_seconds(), 3))

    def get_emergency_order_ref(self, record):
        if not record:
            return None
        trade_id = record.get("trade_id")
        symbol = record.get("symbol")
        if not trade_id or not symbol:
            return None
        return self.build_order_ref(trade_id, symbol, "PROTECTIVE_EMERGENCY_FLATTEN")

    def get_emergency_order_status_from_snapshot(self, record, broker_snapshot=None):
        if not record or not broker_snapshot:
            return None, False

        emergency_order_id = record.get("emergency_flatten_order_id")
        emergency_perm_id = record.get("emergency_flatten_perm_id")
        emergency_order_ref = self.get_emergency_order_ref(record)

        for trade in broker_snapshot.get("open_trades") or []:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            if order is None and status is None:
                continue
            order_id = getattr(order, "orderId", None)
            perm_id = getattr(status, "permId", None) or getattr(order, "permId", None)
            order_ref = self.get_order_ref(order=order)
            if (
                (emergency_order_id is not None and order_id == emergency_order_id)
                or (emergency_perm_id not in (None, 0) and perm_id == emergency_perm_id)
                or (emergency_order_ref and order_ref == emergency_order_ref)
            ):
                return getattr(status, "status", None), True

        for order in broker_snapshot.get("open_orders") or []:
            order_id = getattr(order, "orderId", None)
            perm_id = getattr(order, "permId", None)
            order_ref = self.get_order_ref(order=order)
            if (
                (emergency_order_id is not None and order_id == emergency_order_id)
                or (emergency_perm_id not in (None, 0) and perm_id == emergency_perm_id)
                or (emergency_order_ref and order_ref == emergency_order_ref)
            ):
                return getattr(order, "status", None), True

        return None, False

    def get_emergency_flatten_inflight_state(self, record, broker_snapshot=None, now_value=None):
        now_value = now_value or datetime.now(timezone.utc)
        if record is None:
            return {
                "has_existing_emergency_order": False,
                "emergency_order_id": None,
                "emergency_perm_id": None,
                "emergency_status": None,
                "order_inflight": False,
                "order_terminal": False,
                "flat_confirmed": False,
                "safe_to_submit_new_flatten": False,
                "submitted_age_sec": None,
                "reserved_age_sec": None,
                "reason": "record_missing",
            }

        emergency_order_id = record.get("emergency_flatten_order_id")
        emergency_perm_id = record.get("emergency_flatten_perm_id")
        record_status = record.get("emergency_flatten_last_status") or record.get("protective_emergency_status")
        broker_status, broker_visible = self.get_emergency_order_status_from_snapshot(record, broker_snapshot)
        emergency_status = broker_status if broker_visible else record_status
        submitted_age_sec = self.get_datetime_age_seconds(record.get("emergency_flatten_submitted_at"), now_value)
        reserved_age_sec = self.get_datetime_age_seconds(record.get("emergency_flatten_submit_reserved_at"), now_value)
        recent_submit = (
            (submitted_age_sec is not None and submitted_age_sec <= PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS)
            or (reserved_age_sec is not None and reserved_age_sec <= PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS)
        )
        has_existing_emergency_order = emergency_order_id is not None or emergency_perm_id not in (None, 0)
        order_terminal = emergency_status in PROTECTIVE_EMERGENCY_TERMINAL_STATUSES
        flat_confirmed = record.get("protective_emergency_status") == "flat_confirmed_stale_orders_cleared"

        order_inflight = False
        reason = "no_existing_emergency_order"
        if flat_confirmed:
            reason = "flat_confirmed_stale_orders_cleared"
        elif emergency_status in PROTECTIVE_EMERGENCY_INFLIGHT_STATUSES:
            order_inflight = True
            reason = f"inflight_status:{emergency_status}"
        elif broker_visible and not order_terminal:
            order_inflight = True
            reason = f"broker_visible_non_terminal:{emergency_status}"
        elif recent_submit and not order_terminal:
            order_inflight = True
            reason = "unknown_recent"
            emergency_status = emergency_status or "unknown_recent"
        elif emergency_status in {"submit_failed", "submit_failed_submission_uncertain", "broker_submission_uncertain"}:
            order_inflight = True
            reason = f"broker_submission_uncertain:{emergency_status}"
        elif emergency_status == "Filled":
            reason = "filled_requires_broker_flat_and_stale_child_confirmation"
        elif has_existing_emergency_order and not order_terminal:
            order_inflight = True
            reason = f"existing_emergency_order_non_terminal:{emergency_status}"
        elif order_terminal:
            reason = f"terminal_status:{emergency_status}"

        return {
            "has_existing_emergency_order": has_existing_emergency_order,
            "emergency_order_id": emergency_order_id,
            "emergency_perm_id": emergency_perm_id,
            "emergency_status": emergency_status,
            "order_inflight": order_inflight,
            "order_terminal": order_terminal,
            "flat_confirmed": flat_confirmed,
            "safe_to_submit_new_flatten": not order_inflight and not flat_confirmed,
            "submitted_age_sec": submitted_age_sec,
            "reserved_age_sec": reserved_age_sec,
            "reason": reason,
        }

    def get_symbol_emergency_flatten_inflight_state(self, symbol, exclude_trade_id=None, broker_snapshot=None):
        with self.trade_analysis_lock:
            records = [
                dict(record)
                for record in self.trade_analysis.values()
                if (
                    record.get("symbol") == symbol
                    and record.get("trade_id") != exclude_trade_id
                    and not record.get("summary_logged")
                    and (
                        record.get("protective_emergency_active")
                        or record.get("emergency_flatten_order_id") is not None
                        or record.get("emergency_flatten_perm_id") not in (None, 0)
                    )
                )
            ]

        for record in records:
            state = self.get_emergency_flatten_inflight_state(record, broker_snapshot)
            status = record.get("protective_emergency_status")
            active_unresolved = bool(
                record.get("protective_emergency_active")
                and status in {
                    "submit_in_progress",
                    "flatten_order_submitted",
                    "in_flight",
                    "pending_position_confirmation",
                    "blocked_broker_read_failed",
            "failed_position_still_open",
            "flat_confirmed_stale_child_cleanup_failed",
            "flat_confirmed_stale_bot_order_cleanup_failed",
            "submit_failed_submission_uncertain",
            "broker_submission_uncertain",
        }
            )
            if state.get("order_inflight") or active_unresolved:
                return {
                    "symbol_has_inflight_emergency": True,
                    "blocking_trade_id": record.get("trade_id"),
                    "blocking_order_id": record.get("emergency_flatten_order_id"),
                    "blocking_perm_id": record.get("emergency_flatten_perm_id"),
                    "blocking_status": state.get("emergency_status") or status,
                    "reason": state.get("reason") if state.get("order_inflight") else f"same_symbol_active:{status}",
                }

        return {
            "symbol_has_inflight_emergency": False,
            "blocking_trade_id": None,
            "blocking_order_id": None,
            "blocking_perm_id": None,
            "blocking_status": None,
            "reason": "no_symbol_emergency_inflight",
        }

    def read_protective_emergency_broker_snapshot(self, symbol, reason_label, trade_id=None):
        open_trades, positions, open_orders = self.broker_read_open_trades_positions_open_orders(
            caller="protective_emergency_flatten",
            reason_label=reason_label,
            trade_id=trade_id,
            symbol=symbol,
            trade_analysis_lock_context="not_locked",
        )
        return {
            "open_trades": open_trades,
            "positions": positions,
            "open_orders": open_orders,
        }

    def get_stale_bot_owned_child_orders(self, record, broker_snapshot):
        return self.get_stale_bot_owned_orders_for_trade(
            record,
            broker_snapshot,
            include_parent=False,
            include_children=True,
        ).get("orders", {})

    def get_stale_bot_owned_orders_for_trade(
        self,
        record,
        broker_snapshot,
        include_parent=True,
        include_children=True,
    ):
        empty_result = {
            "orders": {},
            "order_ids": [],
            "parent_order_ids": [],
            "child_order_ids": [],
            "order_refs": [],
            "perm_ids": [],
            "scan_scope": "none",
        }
        if not record or not broker_snapshot:
            return empty_result

        trade_id = record.get("trade_id")
        symbol = record.get("symbol")
        parent_order_ids = {record.get("parent_order_id")} if include_parent else set()
        child_order_ids = (
            {record.get("tp_order_id"), record.get("sl_order_id")}
            if include_children
            else set()
        )
        emergency_order_ids = {record.get("emergency_flatten_order_id")}
        candidate_order_ids = set()
        candidate_order_ids.update(parent_order_ids)
        candidate_order_ids.update(child_order_ids)
        candidate_order_ids.update(emergency_order_ids)
        candidate_order_ids.discard(None)

        candidate_perm_ids = {
            record.get("parent_perm_id") if include_parent else None,
            record.get("tp_perm_id") if include_children else None,
            record.get("sl_perm_id") if include_children else None,
            record.get("emergency_flatten_perm_id"),
        }
        candidate_perm_ids.discard(None)
        candidate_perm_ids.discard(0)

        with self.trade_analysis_lock:
            order_to_trade_snapshot = dict(self.order_to_trade)

        stale_orders = {}
        stale_parent_order_ids = set()
        stale_child_order_ids = set()
        stale_order_refs = set()
        stale_perm_ids = set()

        def contract_symbol_ok(contract):
            if contract is None:
                return True
            return self.contract_matches_symbol(contract, symbol)

        def bot_owned_order_match(order, status=None, contract=None):
            if order is None:
                return False, "order_missing"
            if not contract_symbol_ok(contract):
                return False, "symbol_mismatch"
            order_id = getattr(order, "orderId", None)
            perm_id = (
                getattr(status, "permId", None)
                if status is not None
                else None
            ) or getattr(order, "permId", None)
            order_ref = self.get_order_ref(order=order)

            if order_id in candidate_order_ids:
                return True, "order_id"
            if perm_id in candidate_perm_ids:
                return True, "perm_id"
            if order_id is not None and order_to_trade_snapshot.get(order_id) == trade_id:
                return True, "order_to_trade"
            if self.broker_order_ref_matches_record(record, order_ref):
                return True, "order_ref_trade_id"
            return False, "no_bot_ownership_evidence"

        for trade in broker_snapshot.get("open_trades") or []:
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)
            contract = getattr(trade, "contract", None)
            if not self.order_is_open_for_session_close(order, status):
                continue
            order_id = getattr(order, "orderId", None)
            matched, _ = bot_owned_order_match(order, status=status, contract=contract)
            if matched:
                stale_orders[order_id] = order
                if order_id in parent_order_ids:
                    stale_parent_order_ids.add(order_id)
                if order_id in child_order_ids:
                    stale_child_order_ids.add(order_id)
                order_ref = self.get_order_ref(order=order)
                perm_id = getattr(status, "permId", None) or getattr(order, "permId", None)
                if order_ref:
                    stale_order_refs.add(order_ref)
                if perm_id not in (None, 0):
                    stale_perm_ids.add(perm_id)

        for order in broker_snapshot.get("open_orders") or []:
            order_id = getattr(order, "orderId", None)
            matched, _ = bot_owned_order_match(order)
            if matched:
                stale_orders[order_id] = order
                if order_id in parent_order_ids:
                    stale_parent_order_ids.add(order_id)
                if order_id in child_order_ids:
                    stale_child_order_ids.add(order_id)
                order_ref = self.get_order_ref(order=order)
                perm_id = getattr(order, "permId", None)
                if order_ref:
                    stale_order_refs.add(order_ref)
                if perm_id not in (None, 0):
                    stale_perm_ids.add(perm_id)

        result = {
            "orders": stale_orders,
            "order_ids": sorted(order_id for order_id in stale_orders.keys() if order_id is not None),
            "parent_order_ids": sorted(stale_parent_order_ids),
            "child_order_ids": sorted(stale_child_order_ids),
            "order_refs": sorted(stale_order_refs),
            "perm_ids": sorted(stale_perm_ids),
            "scan_scope": (
                f"include_parent={include_parent};"
                f"include_children={include_children};"
                "ownership=order_id|perm_id|order_to_trade|orderRef_exact_trade_id"
            ),
        }
        logger.critical(
            "PROTECTIVE_EMERGENCY_STALE_BOT_ORDER_SCAN | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"stale_bot_owned_order_ids={result['order_ids']} "
            f"stale_parent_order_ids={result['parent_order_ids']} "
            f"stale_child_order_ids={result['child_order_ids']} "
            f"stale_order_refs={result['order_refs']} "
            f"stale_perm_ids={result['perm_ids']} "
            f"stale_scan_scope={result['scan_scope']} "
            "decision=scan_known_bot_owned_orders_only"
        )
        return result

    def get_emergency_flatten_broker_fill_evidence(self, record, reason_label):
        result = {
            "broker_fill_visible": False,
            "fill_read_attempted": True,
            "fill_read_failed": False,
            "failure": None,
            "matched_exec_ids": [],
        }
        try:
            fills = self.broker_read_fills(
                caller="protective_emergency_flatten",
                reason_label=reason_label,
                trade_id=record.get("trade_id") if isinstance(record, dict) else None,
                symbol=record.get("symbol") if isinstance(record, dict) else None,
                trade_analysis_lock_context="not_locked",
            )
        except Exception as exc:
            result["fill_read_failed"] = True
            result["failure"] = str(exc)
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_FILL_EVIDENCE_READ_FAILED | "
                f"trade_id={record.get('trade_id')} "
                f"symbol={record.get('symbol')} "
                f"failure={exc} "
                "fill_read_attempted=True "
                "decision=allow_only_if_other_broker_flat_evidence_is_clean"
            )
            return result

        emergency_order_id = record.get("emergency_flatten_order_id")
        emergency_perm_id = record.get("emergency_flatten_perm_id")
        emergency_order_ref = self.get_emergency_order_ref(record)
        for fill in fills or []:
            execution = getattr(fill, "execution", None)
            if execution is None:
                continue
            order_id = getattr(execution, "orderId", None)
            perm_id = getattr(execution, "permId", None)
            order_ref = self.get_order_ref(execution=execution)
            if (
                (emergency_order_id is not None and order_id == emergency_order_id)
                or (emergency_perm_id not in (None, 0) and perm_id == emergency_perm_id)
                or (emergency_order_ref and order_ref == emergency_order_ref)
            ):
                result["broker_fill_visible"] = True
                result["matched_exec_ids"].append(getattr(execution, "execId", None))

        logger.critical(
            "PROTECTIVE_EMERGENCY_FLATTEN_FILL_EVIDENCE_CHECK | "
            f"trade_id={record.get('trade_id')} "
            f"symbol={record.get('symbol')} "
            f"broker_fill_visible={result['broker_fill_visible']} "
            f"matched_exec_ids={result['matched_exec_ids']} "
            "fill_read_attempted=True"
        )
        return result

    def can_finalize_emergency_flat_fill_details_incomplete(
        self,
        record,
        broker_snapshot,
        emergency_state,
        symbol_state,
        stale_scan_result,
        fill_evidence,
        position_qty,
    ):
        missing_details = []
        broker_status, broker_visible = self.get_emergency_order_status_from_snapshot(record, broker_snapshot)
        stale_orders_clear = not stale_scan_result.get("orders")
        submitted_age_sec = emergency_state.get("submitted_age_sec")
        reserved_age_sec = emergency_state.get("reserved_age_sec")
        age_candidates = [
            value for value in (submitted_age_sec, reserved_age_sec)
            if value is not None
        ]
        age_past_grace = bool(
            age_candidates
            and max(age_candidates) > PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS
        )
        broker_position_flat = position_qty == 0.0
        symbol_inflight = bool(symbol_state.get("symbol_has_inflight_emergency"))
        broker_fill_visible = bool(fill_evidence.get("broker_fill_visible"))
        fill_read_attempted = bool(fill_evidence.get("fill_read_attempted"))

        checks = {
            "broker_position_flat": broker_position_flat,
            "stale_orders_clear": stale_orders_clear,
            "broker_order_visible": broker_visible,
            "symbol_level_inflight": symbol_inflight,
            "age_past_grace": age_past_grace,
            "fill_read_attempted": fill_read_attempted,
            "broker_fill_visible": broker_fill_visible,
        }
        for key, value in checks.items():
            if key in {"broker_order_visible", "symbol_level_inflight", "broker_fill_visible"}:
                if value:
                    missing_details.append(key)
            elif not value:
                missing_details.append(key)

        can_finalize = not missing_details
        reason = (
            "broker_flat_stale_clean_order_not_visible_grace_elapsed_fill_details_incomplete"
            if can_finalize
            else f"missing_required_evidence:{','.join(missing_details)}"
        )
        return {
            "can_finalize": can_finalize,
            "reason": reason,
            "missing_details": missing_details,
            "required_evidence": {
                "broker_position_qty": position_qty,
                "broker_status": broker_status,
                "broker_order_visible": broker_visible,
                "submitted_age_sec": submitted_age_sec,
                "reserved_age_sec": reserved_age_sec,
                "inflight_grace_seconds": PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS,
                "stale_bot_owned_order_ids": stale_scan_result.get("order_ids"),
                "symbol_level_inflight": symbol_inflight,
                "broker_fill_visible": broker_fill_visible,
                "fill_read_failed": fill_evidence.get("fill_read_failed"),
            },
        }

    def confirm_protective_emergency_flat_with_stale_child_gate(self, trade_id, symbol, reason_label, position_qty, confirm_reason, protection_context=None):
        try:
            snapshot = self.read_protective_emergency_broker_snapshot(
                symbol,
                f"protective_emergency_flat_confirmed:{confirm_reason}",
                trade_id=trade_id,
            )
        except Exception as exc:
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["protective_emergency_active"] = True
                    record["protective_emergency_status"] = "flat_confirmed_stale_child_cleanup_failed"
                    record["state"] = "BROKER_ACK_PENDING"
                    record["closed"] = False
            logger.critical(
                "PROTECTIVE_EMERGENCY_STALE_CHILD_ORDER_AFTER_FLATTEN | "
                f"trade_id={trade_id} symbol={symbol} reason={confirm_reason} "
                f"failure={exc} stale_child_order_ids=[] stale_child_cleanup_result=broker_read_failed "
                "operator_action_required=True"
            )
            return {
                "ok": False,
                "reason": f"stale_child_cleanup_broker_read_failed:{exc}",
                "flat_confirmed": True,
                "stale_child_cleanup_result": "broker_read_failed",
            }

        with self.trade_analysis_lock:
            record_snapshot = dict(self.trade_analysis.get(trade_id) or {})

        emergency_state = self.get_emergency_flatten_inflight_state(record_snapshot, snapshot)
        symbol_state = self.get_symbol_emergency_flatten_inflight_state(
            symbol,
            exclude_trade_id=trade_id,
            broker_snapshot=snapshot,
        )
        stale_scan_result = self.get_stale_bot_owned_orders_for_trade(
            record_snapshot,
            snapshot,
            include_parent=True,
            include_children=True,
        )
        fill_evidence = self.get_emergency_flatten_broker_fill_evidence(
            record_snapshot,
            f"protective_emergency_fill_details_incomplete_check:{confirm_reason}",
        )
        incomplete_finalize = self.can_finalize_emergency_flat_fill_details_incomplete(
            record_snapshot,
            snapshot,
            emergency_state,
            symbol_state,
            stale_scan_result,
            fill_evidence,
            position_qty,
        )
        if (
            emergency_state.get("order_inflight")
            and position_qty != 0.0
            and not incomplete_finalize.get("can_finalize")
        ):
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["protective_emergency_active"] = True
                    record["protective_emergency_status"] = "pending_position_confirmation"
                    record["state"] = "BROKER_ACK_PENDING"
                    record["closed"] = False
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_RETRY_BLOCKED_INFLIGHT | "
                f"trade_id={trade_id} symbol={symbol} "
                f"emergency_flatten_order_id={emergency_state.get('emergency_order_id')} "
                f"emergency_flatten_perm_id={emergency_state.get('emergency_perm_id')} "
                f"emergency_flatten_last_status={emergency_state.get('emergency_status')} "
                f"position_qty_after={position_qty} "
                f"submitted_age_sec={emergency_state.get('submitted_age_sec')} "
                f"reserved_age_sec={emergency_state.get('reserved_age_sec')} "
                f"inflight_grace_seconds={PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS} "
                f"broker_order_visible={self.get_emergency_order_status_from_snapshot(record_snapshot, snapshot)[1]} "
                f"broker_fill_visible={fill_evidence.get('broker_fill_visible')} "
                f"stale_bot_owned_order_ids={stale_scan_result.get('order_ids')} "
                "safe_to_submit_new_flatten=False "
                "idempotency_decision=block_flat_release_existing_order_inflight "
                f"reason={incomplete_finalize.get('reason') or emergency_state.get('reason')} "
                "operator_action_required=False"
            )
            return {
                "ok": True,
                "reason": "pending_position_confirmation",
                "flat_confirmed": True,
                "pending": True,
            }

        stale_orders = stale_scan_result.get("orders", {})
        stale_child_order_ids = stale_scan_result.get("child_order_ids", [])
        stale_parent_order_ids = stale_scan_result.get("parent_order_ids", [])
        stale_bot_owned_order_ids = stale_scan_result.get("order_ids", [])
        cancel_failed = False
        for order_id, order in list(stale_orders.items()):
            try:
                logger.critical(
                    "PROTECTIVE_EMERGENCY_STALE_CHILD_CANCEL_REQUESTED | "
                    f"trade_id={trade_id} symbol={symbol} order_id={order_id} "
                    f"reason={confirm_reason} "
                    f"stale_bot_owned_order_ids={stale_bot_owned_order_ids} "
                    f"stale_parent_order_ids={stale_parent_order_ids} "
                    f"stale_child_order_ids={stale_child_order_ids} "
                    f"stale_order_refs={stale_scan_result.get('order_refs')} "
                    f"stale_perm_ids={stale_scan_result.get('perm_ids')} "
                    f"stale_scan_scope={stale_scan_result.get('scan_scope')} "
                    "decision=cancel_known_bot_owned_order_after_flat"
                )
                self.broker_write_cancel_order(
                    order,
                    caller="protective_emergency_flatten",
                    reason_label=f"protective_emergency_stale_child_cleanup:{confirm_reason}",
                    trade_id=trade_id,
                    symbol=symbol,
                    real_broker_exposure=True,
                )
            except Exception as exc:
                cancel_failed = True
                logger.exception(
                    "PROTECTIVE_EMERGENCY_STALE_CHILD_CANCEL_FAILED | "
                    f"trade_id={trade_id} symbol={symbol} order_id={order_id} reason={confirm_reason} failure={exc}"
                )

        if stale_orders:
            self.broker_write_sleep(
                PROTECTIVE_SL_MISSING_RECHECK_SECONDS,
                caller="protective_emergency_flatten",
                reason_label=f"protective_emergency_stale_child_cleanup_recheck:{confirm_reason}",
                trade_id=trade_id,
                symbol=symbol,
            )

        try:
            recheck_snapshot = self.read_protective_emergency_broker_snapshot(
                symbol,
                f"protective_emergency_stale_child_cleanup_recheck:{confirm_reason}",
                trade_id=trade_id,
            )
            with self.trade_analysis_lock:
                record_snapshot = dict(self.trade_analysis.get(trade_id) or {})
            remaining_stale_scan_result = self.get_stale_bot_owned_orders_for_trade(
                record_snapshot,
                recheck_snapshot,
                include_parent=True,
                include_children=True,
            )
            remaining_stale_orders = remaining_stale_scan_result.get("orders", {})
        except Exception as exc:
            cancel_failed = True
            remaining_stale_orders = stale_orders
            remaining_stale_scan_result = stale_scan_result
            logger.exception(
                "PROTECTIVE_EMERGENCY_STALE_CHILD_RECHECK_FAILED | "
                f"trade_id={trade_id} symbol={symbol} reason={confirm_reason} failure={exc}"
            )

        remaining_stale_child_order_ids = remaining_stale_scan_result.get("child_order_ids", [])
        remaining_stale_parent_order_ids = remaining_stale_scan_result.get("parent_order_ids", [])
        remaining_stale_bot_owned_order_ids = remaining_stale_scan_result.get("order_ids", [])
        if cancel_failed or remaining_stale_orders:
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["protective_emergency_active"] = True
                    record["protective_emergency_status"] = "flat_confirmed_stale_bot_order_cleanup_failed"
                    record["state"] = "BROKER_ACK_PENDING"
                    record["closed"] = False
                    record["exit_reason"] = self.get_time_exit_effective_exit_reason(record)
            logger.critical(
                "PROTECTIVE_EMERGENCY_STALE_BOT_ORDER_AFTER_FLATTEN | "
                f"trade_id={trade_id} symbol={symbol} reason={confirm_reason} "
                f"stale_bot_owned_order_ids={remaining_stale_bot_owned_order_ids} "
                f"stale_parent_order_ids={remaining_stale_parent_order_ids} "
                f"stale_child_order_ids={remaining_stale_child_order_ids} "
                f"stale_order_refs={remaining_stale_scan_result.get('order_refs')} "
                f"stale_perm_ids={remaining_stale_scan_result.get('perm_ids')} "
                f"stale_scan_scope={remaining_stale_scan_result.get('scan_scope')} "
                "stale_bot_order_cleanup_result=failed "
                "operator_action_required=True "
                "decision=preserve_active_lock_reversal_risk"
            )
            return {
                "ok": False,
                "reason": "stale_child_cleanup_failed",
                "flat_confirmed": True,
                "stale_child_cleanup_result": "failed",
                "stale_child_order_ids": remaining_stale_child_order_ids,
                "stale_parent_order_ids": remaining_stale_parent_order_ids,
                "stale_bot_owned_order_ids": remaining_stale_bot_owned_order_ids,
            }

        if record_snapshot.get("exit_fill_price") is None:
            post_cleanup_emergency_state = self.get_emergency_flatten_inflight_state(
                record_snapshot,
                recheck_snapshot,
            )
            post_cleanup_symbol_state = self.get_symbol_emergency_flatten_inflight_state(
                symbol,
                exclude_trade_id=trade_id,
                broker_snapshot=recheck_snapshot,
            )
            post_cleanup_fill_evidence = self.get_emergency_flatten_broker_fill_evidence(
                record_snapshot,
                f"protective_emergency_post_cleanup_fill_details_incomplete_check:{confirm_reason}",
            )
            post_cleanup_finalize = self.can_finalize_emergency_flat_fill_details_incomplete(
                record_snapshot,
                recheck_snapshot,
                post_cleanup_emergency_state,
                post_cleanup_symbol_state,
                remaining_stale_scan_result,
                post_cleanup_fill_evidence,
                position_qty,
            )
            if not post_cleanup_finalize.get("can_finalize"):
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        record["protective_emergency_active"] = True
                        record["protective_emergency_status"] = "pending_position_confirmation"
                        record["state"] = "BROKER_ACK_PENDING"
                        record["closed"] = False
                        record["exit_reason"] = self.get_time_exit_effective_exit_reason(record)
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_RETRY_EVIDENCE_MISSING | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"emergency_flatten_order_id={post_cleanup_emergency_state.get('emergency_order_id')} "
                    f"emergency_flatten_perm_id={post_cleanup_emergency_state.get('emergency_perm_id')} "
                    f"emergency_flatten_last_status={post_cleanup_emergency_state.get('emergency_status')} "
                    f"protective_emergency_status=pending_position_confirmation "
                    "protective_emergency_active=True "
                    f"submitted_age_sec={post_cleanup_emergency_state.get('submitted_age_sec')} "
                    f"reserved_age_sec={post_cleanup_emergency_state.get('reserved_age_sec')} "
                    f"inflight_grace_seconds={PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS} "
                    f"broker_position_qty={position_qty} "
                    f"stale_bot_owned_order_ids={remaining_stale_scan_result.get('order_ids')} "
                    "retry_allowed=False "
                    "retry_evidence=missing_flat_fill_details_incomplete_release_evidence "
                    f"reason={post_cleanup_finalize.get('reason')} "
                    "operator_action_required=False "
                    "decision=preserve_active_lock_after_stale_cleanup"
                )
                return {
                    "ok": True,
                    "reason": post_cleanup_finalize.get("reason"),
                    "flat_confirmed": True,
                    "pending": True,
                }

        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is not None:
                record["protective_emergency_active"] = False
                record["protective_emergency_status"] = (
                    "flat_confirmed_exit_fill_details_incomplete"
                    if record.get("exit_fill_price") is None
                    else "flat_confirmed_stale_orders_cleared"
                )
                record["protective_emergency_reason"] = reason_label
                record["state"] = "CLOSED"
                record["closed"] = True
                record["exit_reason"] = self.get_time_exit_effective_exit_reason(record)
                if record["exit_reason"] == TIME_EXIT_REASON:
                    record["time_exit_status"] = "TIME_EXIT_COMPLETED"
                    record["time_exit_completed_at"] = datetime.now(timezone.utc)
                    record["time_exit_order_id"] = record.get("emergency_flatten_order_id")
                if record.get("realized_exit_quantity") is None:
                    record["realized_exit_quantity"] = float(record.get("cumulative_exit_quantity") or 0.0)
                if record.get("exit_fill_price") is None:
                    record["execution_validation_status"] = "broker_flat_confirmed_exit_fill_details_incomplete"
                    logger.critical(
                        "PROTECTIVE_EMERGENCY_FLATTEN_CONFIRMED_FILL_DETAILS_INCOMPLETE | "
                        f"trade_id={trade_id} "
                        f"symbol={symbol} "
                        f"emergency_flatten_order_id={record.get('emergency_flatten_order_id')} "
                        f"emergency_flatten_perm_id={record.get('emergency_flatten_perm_id')} "
                        f"emergency_flatten_last_status={record.get('emergency_flatten_last_status')} "
                        f"protective_emergency_status={record.get('protective_emergency_status')} "
                        "protective_emergency_active=False "
                        f"submitted_age_sec={emergency_state.get('submitted_age_sec')} "
                        f"reserved_age_sec={emergency_state.get('reserved_age_sec')} "
                        f"inflight_grace_seconds={PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS} "
                        f"broker_position_qty={position_qty} "
                        f"stale_bot_owned_order_ids={remaining_stale_scan_result.get('order_ids')} "
                        f"broker_order_visible={self.get_emergency_order_status_from_snapshot(record_snapshot, recheck_snapshot)[1]} "
                        f"broker_fill_visible={fill_evidence.get('broker_fill_visible')} "
                        "fill_details_complete=False "
                        "broker_flat_confirmed=True "
                        "stale_bot_orders_cleared=True "
                        "execution_validation_status=broker_flat_confirmed_exit_fill_details_incomplete "
                        f"reason={confirm_reason} "
                        "operator_action_required=False "
                        "decision=close_on_broker_flat_evidence_without_fill_details"
                    )
                self.append_trade_event(
                    trade_id,
                    f"PROTECTIVE EMERGENCY FLATTEN CONFIRMED position_qty={position_qty} "
                    f"reason={confirm_reason} stale_child_cleanup=cleared"
                )
        logger.critical(
            "PROTECTIVE_EMERGENCY_FLATTEN_CONFIRMED | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"protection_class={(protection_context or {}).get('protection_class')} "
            f"reason={reason_label} "
            f"broker_position_qty={position_qty} "
            f"position_qty_after={position_qty} "
            "protective_emergency_active=False "
            f"protective_emergency_status={(self.trade_analysis.get(trade_id) or {}).get('protective_emergency_status')} "
            "stale_bot_order_cleanup_result=cleared "
            f"stale_bot_owned_order_ids={remaining_stale_scan_result.get('order_ids')} "
            f"stale_parent_order_ids={remaining_stale_scan_result.get('parent_order_ids')} "
            f"stale_child_order_ids={remaining_stale_scan_result.get('child_order_ids')} "
            f"fill_details_complete={(self.trade_analysis.get(trade_id) or {}).get('exit_fill_price') is not None} "
            "broker_flat_confirmed=True "
            "stale_bot_orders_cleared=True "
            "operator_action_required=False "
            "decision=broker_flat_confirmed_stale_bot_orders_cleared"
        )
        return {
            "ok": True,
            "reason": confirm_reason,
            "flat_confirmed": True,
            "stale_child_cleanup_result": "cleared",
        }

    def emergency_flatten_unprotected_position(
        self,
        trade_id,
        symbol,
        reason_label,
        protection_context=None,
        broker_reality=None,
    ):
        now_dt = datetime.now(timezone.utc)
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return {
                    "ok": False,
                    "reason": "trade_record_missing",
                    "flat_confirmed": False,
                }
            previous_status = record.get("protective_emergency_status")
            record["protective_emergency_active"] = True
            record["protective_emergency_reason"] = reason_label
            if previous_status not in {
                "submit_in_progress",
                "flatten_order_submitted",
                "in_flight",
                "pending_position_confirmation",
                "flat_confirmed_stale_child_cleanup_failed",
            }:
                record["protective_emergency_status"] = "started"
            record["protective_emergency_started_at"] = record.get("protective_emergency_started_at") or now_dt
            record["state"] = "BROKER_ACK_PENDING"
            record_snapshot = dict(record)
            self.append_trade_event(
                trade_id,
                f"PROTECTIVE EMERGENCY FLATTEN STARTED reason={reason_label}"
            )

        logger.critical(
            "PROTECTIVE_EMERGENCY_FLATTEN_STARTED | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"protection_class={(protection_context or {}).get('protection_class')} "
            f"reason={reason_label} "
            f"parent_order_id={record_snapshot.get('parent_order_id')} "
            f"tp_order_id={record_snapshot.get('tp_order_id')} "
            f"sl_order_id={record_snapshot.get('sl_order_id')} "
            f"parent_visible={(protection_context or {}).get('parent_visible')} "
            f"tp_visible={(protection_context or {}).get('tp_visible')} "
            f"sl_visible={(protection_context or {}).get('sl_visible')} "
            f"entry_filled={record_snapshot.get('entry_filled')} "
            f"cumulative_entry_quantity={record_snapshot.get('cumulative_entry_quantity')} "
            f"realized_entry_quantity={record_snapshot.get('realized_entry_quantity')} "
            "protective_emergency_active=True "
            "protective_emergency_status=started "
            "decision=emergency_flatten_actual_broker_position"
        )

        def read_position_qty_from_snapshot(snapshot):
            return self.get_position_quantity_from_positions(snapshot.get("positions"), symbol)

        def log_idempotency_check(trade_state, symbol_state, position_qty=None, decision="inspect"):
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_IDEMPOTENCY_CHECK | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"blocking_trade_id={symbol_state.get('blocking_trade_id')} "
                f"emergency_flatten_order_id={trade_state.get('emergency_order_id')} "
                f"emergency_flatten_perm_id={trade_state.get('emergency_perm_id')} "
                f"emergency_flatten_last_status={trade_state.get('emergency_status')} "
                f"protective_emergency_status={record_snapshot.get('protective_emergency_status')} "
                f"protective_emergency_active={record_snapshot.get('protective_emergency_active')} "
                f"inflight_grace_seconds={PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS} "
                f"submitted_age_sec={trade_state.get('submitted_age_sec')} "
                f"reserved_age_sec={trade_state.get('reserved_age_sec')} "
                f"position_qty_before={position_qty} "
                f"safe_to_submit_new_flatten={trade_state.get('safe_to_submit_new_flatten')} "
                f"symbol_level_inflight={symbol_state.get('symbol_has_inflight_emergency')} "
                f"idempotency_decision={decision} "
                f"reason={trade_state.get('reason')} "
                "operator_action_required=False"
            )

        def handle_inflight_block(trade_state, symbol_state, snapshot, block_reason):
            position_qty = read_position_qty_from_snapshot(snapshot) if snapshot else None
            log_label = (
                "PROTECTIVE_EMERGENCY_FLATTEN_SYMBOL_INFLIGHT_BLOCK"
                if symbol_state.get("symbol_has_inflight_emergency")
                else "PROTECTIVE_EMERGENCY_FLATTEN_ALREADY_IN_FLIGHT"
            )
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["protective_emergency_active"] = True
                    if record.get("protective_emergency_status") not in {"flat_confirmed_stale_child_cleanup_failed"}:
                        record["protective_emergency_status"] = "pending_position_confirmation"
                    record["state"] = "BROKER_ACK_PENDING"
            logger.critical(
                f"{log_label} | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"blocking_trade_id={symbol_state.get('blocking_trade_id')} "
                f"blocking_order_id={symbol_state.get('blocking_order_id')} "
                f"blocking_perm_id={symbol_state.get('blocking_perm_id')} "
                f"blocking_status={symbol_state.get('blocking_status')} "
                f"emergency_flatten_order_id={trade_state.get('emergency_order_id')} "
                f"emergency_flatten_perm_id={trade_state.get('emergency_perm_id')} "
                f"emergency_flatten_last_status={trade_state.get('emergency_status')} "
                "protective_emergency_active=True "
                "protective_emergency_status=pending_position_confirmation "
                f"inflight_grace_seconds={PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS} "
                f"submitted_age_sec={trade_state.get('submitted_age_sec')} "
                f"reserved_age_sec={trade_state.get('reserved_age_sec')} "
                f"position_qty_after={position_qty} "
                "safe_to_submit_new_flatten=False "
                f"symbol_level_inflight={symbol_state.get('symbol_has_inflight_emergency')} "
                f"idempotency_decision=block_new_order "
                f"reason={block_reason} "
                "operator_action_required=False"
            )
            if position_qty == 0.0:
                return self.confirm_protective_emergency_flat_with_stale_child_gate(
                    trade_id,
                    symbol,
                    reason_label,
                    position_qty,
                    "flat_confirmed_while_existing_emergency_inflight",
                    protection_context=protection_context,
                )
            return {
                "ok": True,
                "reason": block_reason,
                "flat_confirmed": False,
                "pending": True,
            }

        if broker_reality and broker_reality.get("check_failed"):
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["protective_emergency_status"] = "blocked_broker_read_failed"
                    record["protective_emergency_reason"] = reason_label
                    record["state"] = "BROKER_ACK_PENDING"
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_BLOCKED_BROKER_READ_FAILED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"reason={reason_label} "
                f"failure={broker_reality.get('failure')} "
                "operator_action_required=True "
                "decision=preserve_active_lock_no_blind_flatten"
            )
            return {
                "ok": False,
                "reason": "broker_reality_check_failed",
                "flat_confirmed": False,
            }

        def read_position_qty(reason_suffix):
            positions = self.broker_read_positions(
                caller="protective_emergency_flatten",
                reason_label=f"{reason_label}:{reason_suffix}",
                trade_id=trade_id,
                symbol=symbol,
                trade_analysis_lock_context="not_locked",
            )
            return self.get_position_quantity_from_positions(positions, symbol)

        def mark_broker_read_failed(exc):
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["protective_emergency_status"] = "blocked_broker_read_failed"
                    record["protective_emergency_reason"] = reason_label
                    record["state"] = "BROKER_ACK_PENDING"
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_BLOCKED_BROKER_READ_FAILED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"reason={reason_label} "
                f"failure={exc} "
                "operator_action_required=True "
                "decision=preserve_active_lock_no_blind_flatten"
            )
            return {
                "ok": False,
                "reason": f"broker_position_read_failed:{exc}",
                "flat_confirmed": False,
            }

        def confirm_flat(position_qty, confirm_reason):
            return self.confirm_protective_emergency_flat_with_stale_child_gate(
                trade_id,
                symbol,
                reason_label,
                position_qty,
                confirm_reason,
                protection_context=protection_context,
            )

        for attempt in range(1, PROTECTIVE_SL_MISSING_MAX_ATTEMPTS + 1):
            try:
                broker_snapshot = self.read_protective_emergency_broker_snapshot(
                    symbol,
                    f"{reason_label}:attempt_{attempt}:idempotency_snapshot",
                    trade_id=trade_id,
                )
            except Exception as exc:
                return mark_broker_read_failed(exc)

            with self.trade_analysis_lock:
                live_record = dict(self.trade_analysis.get(trade_id) or {})
            trade_inflight_state = self.get_emergency_flatten_inflight_state(
                live_record,
                broker_snapshot,
            )
            symbol_inflight_state = self.get_symbol_emergency_flatten_inflight_state(
                symbol,
                exclude_trade_id=trade_id,
                broker_snapshot=broker_snapshot,
            )
            initial_position_qty = read_position_qty_from_snapshot(broker_snapshot)
            log_idempotency_check(
                trade_inflight_state,
                symbol_inflight_state,
                position_qty=initial_position_qty,
                decision="pre_attempt_check",
            )
            if (
                trade_inflight_state.get("order_inflight")
                or (
                    trade_inflight_state.get("emergency_status") == "Filled"
                    and initial_position_qty != 0.0
                )
                or symbol_inflight_state.get("symbol_has_inflight_emergency")
            ):
                return handle_inflight_block(
                    trade_inflight_state,
                    symbol_inflight_state,
                    broker_snapshot,
                    "emergency_flatten_already_inflight",
                )

            try:
                position_qty_before = read_position_qty(f"attempt_{attempt}:initial_position_read")
            except Exception as exc:
                return mark_broker_read_failed(exc)

            logger.critical(
                "PROTECTIVE_EMERGENCY_POSITION_RECHECK | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"attempt={attempt} "
                f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                f"broker_position_qty={position_qty_before} "
                f"position_qty_before={position_qty_before} "
                f"protection_class={(protection_context or {}).get('protection_class')} "
                f"reason={reason_label} "
                "decision=position_authority_before_emergency_flatten"
            )

            if position_qty_before == 0.0:
                return confirm_flat(position_qty_before, "already_flat_after_recheck")

            conflicting_order_ids = []
            if PROTECTIVE_SL_MISSING_CANCEL_CONFLICTING_ORDERS:
                try:
                    open_trades, positions, open_orders = self.broker_read_open_trades_positions_open_orders(
                        caller="protective_emergency_flatten",
                        reason_label=f"protective_emergency_conflicting_order_scan:{reason_label}",
                        symbol=symbol,
                        trade_analysis_lock_context="not_locked",
                    )
                    snapshot = {
                        "open_trades": open_trades,
                        "positions": positions,
                        "open_orders": open_orders,
                    }
                    working_entries, child_orders, _ = self.collect_session_close_orders_for_symbol(symbol, snapshot)
                    current_bot_order_ids = {
                        record_snapshot.get("parent_order_id"),
                        record_snapshot.get("tp_order_id"),
                        record_snapshot.get("sl_order_id"),
                    }
                    orders_to_cancel = {}
                    orders_to_cancel.update({
                        order_id: order
                        for order_id, order in working_entries.items()
                        if order_id in current_bot_order_ids
                    })
                    orders_to_cancel.update({
                        order_id: order
                        for order_id, order in child_orders.items()
                        if order_id in current_bot_order_ids
                    })
                    for order_id, order in list(orders_to_cancel.items()):
                        if order is None:
                            continue
                        conflicting_order_ids.append(order_id)
                        logger.critical(
                            "PROTECTIVE_EMERGENCY_CONFLICTING_ORDER_CANCEL_REQUESTED | "
                            f"trade_id={trade_id} "
                            f"symbol={symbol} "
                            f"order_id={order_id} "
                            f"attempt={attempt} "
                            f"reason={reason_label} "
                            "decision=cancel_known_bot_owned_order_before_mkt_flatten"
                        )
                        self.broker_write_cancel_order(
                            order,
                            caller="protective_emergency_flatten",
                            reason_label=reason_label,
                            trade_id=trade_id,
                            symbol=symbol,
                            real_broker_exposure=True,
                        )
                except Exception as exc:
                    logger.exception(
                        "PROTECTIVE_EMERGENCY_CONFLICTING_ORDER_CANCEL_FAILED | "
                        f"trade_id={trade_id} symbol={symbol} attempt={attempt} reason={reason_label} failure={exc}"
                    )

            self.broker_write_sleep(
                PROTECTIVE_SL_MISSING_RECHECK_SECONDS,
                caller="protective_emergency_flatten",
                reason_label=f"{reason_label}:post_cancel_recheck_wait",
                trade_id=trade_id,
                symbol=symbol,
            )

            try:
                position_qty = read_position_qty(f"attempt_{attempt}:pre_mkt_position_recheck")
            except Exception as exc:
                return mark_broker_read_failed(exc)

            logger.critical(
                "PROTECTIVE_EMERGENCY_POSITION_RECHECK | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"attempt={attempt} "
                f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                f"broker_position_qty={position_qty} "
                f"position_qty_before={position_qty_before} "
                f"conflicting_order_ids_cancel_requested={conflicting_order_ids} "
                f"protection_class={(protection_context or {}).get('protection_class')} "
                f"reason={reason_label} "
                "decision=position_authority_immediately_before_mkt_flatten"
            )

            if position_qty == 0.0:
                return confirm_flat(position_qty, "flat_after_conflicting_order_cancel_recheck")

            try:
                pre_submit_snapshot = self.read_protective_emergency_broker_snapshot(
                    symbol,
                    f"{reason_label}:attempt_{attempt}:pre_submit_idempotency_snapshot",
                    trade_id=trade_id,
                )
            except Exception as exc:
                return mark_broker_read_failed(exc)
            with self.trade_analysis_lock:
                live_record = dict(self.trade_analysis.get(trade_id) or {})
            trade_inflight_state = self.get_emergency_flatten_inflight_state(
                live_record,
                pre_submit_snapshot,
            )
            symbol_inflight_state = self.get_symbol_emergency_flatten_inflight_state(
                symbol,
                exclude_trade_id=trade_id,
                broker_snapshot=pre_submit_snapshot,
            )
            log_idempotency_check(
                trade_inflight_state,
                symbol_inflight_state,
                position_qty=position_qty,
                decision="pre_submit_check",
            )
            if (
                trade_inflight_state.get("order_inflight")
                or (
                    trade_inflight_state.get("emergency_status") == "Filled"
                    and position_qty != 0.0
                )
                or symbol_inflight_state.get("symbol_has_inflight_emergency")
            ):
                return handle_inflight_block(
                    trade_inflight_state,
                    symbol_inflight_state,
                    pre_submit_snapshot,
                    "emergency_flatten_already_inflight_pre_submit",
                )

            if not self.ensure_symbol_contract_ready(symbol):
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        record["protective_emergency_status"] = "blocked_contract_not_ready"
                        record["state"] = "BROKER_ACK_PENDING"
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_FAILED | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"attempt={attempt} "
                    f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                    "reason=contract_not_ready "
                    "operator_action_required=True "
                    "decision=preserve_active_lock"
                )
                return {
                    "ok": False,
                    "reason": "contract_not_ready",
                    "flat_confirmed": False,
                }

            action = "SELL" if position_qty > 0 else "BUY"
            quantity = abs(position_qty)
            if self.is_prd_dry_run_enabled():
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_BLOCKED_DRY_RUN | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"attempt={attempt} "
                    f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                    f"flatten_action={action} "
                    f"flatten_quantity={quantity} "
                    f"position_qty_before={position_qty} "
                    f"reason={reason_label} "
                    "operator_action_required=True "
                    "decision=preserve_active_lock_no_dry_run_flatten"
                )
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        record["protective_emergency_active"] = True
                        record["protective_emergency_status"] = "blocked_prd_dry_run"
                        record["protective_emergency_reason"] = reason_label
                return {
                    "ok": False,
                    "reason": "prd_dry_run_no_emergency_flatten_transmission",
                    "flat_confirmed": False,
                    "operator_action_required": True,
                }
            contract = self.get_contract(symbol)
            order = MarketOrder(action, quantity)
            with self.order_id_lock:
                if self.next_order_id is None:
                    self.next_order_id = self.broker_get_req_id(
                        caller="protective_emergency_flatten",
                        reason_label=reason_label,
                        trade_id=trade_id,
                        symbol=symbol,
                    )
                order.orderId = self.next_order_id
                self.next_order_id += 1
            order.tif = self.get_order_tif(symbol, "protective_emergency_flatten") or "DAY"
            order.orderRef = self.build_order_ref(trade_id, symbol, "PROTECTIVE_EMERGENCY_FLATTEN")
            logger.info(
                "ORDER_REF_ASSIGNED | "
                f"run_id={self.run_id} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                "role=PROTECTIVE_EMERGENCY_FLATTEN "
                f"order_id={getattr(order, 'orderId', None)} "
                f"orderRef={getattr(order, 'orderRef', None)}"
            )

            reservation_id = f"{trade_id}:{symbol}:{order.orderId}:{time.time_ns()}"
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is None:
                    return {
                        "ok": False,
                        "reason": "trade_record_missing_before_submit",
                        "flat_confirmed": False,
                    }
                atomic_trade_state = self.get_emergency_flatten_inflight_state(record)
                atomic_symbol_state = self.get_symbol_emergency_flatten_inflight_state(
                    symbol,
                    exclude_trade_id=trade_id,
                )
                if (
                    atomic_trade_state.get("order_inflight")
                    or atomic_symbol_state.get("symbol_has_inflight_emergency")
                ):
                    logger.critical(
                        "PROTECTIVE_EMERGENCY_FLATTEN_ALREADY_IN_FLIGHT | "
                        f"trade_id={trade_id} "
                        f"symbol={symbol} "
                        f"blocking_trade_id={atomic_symbol_state.get('blocking_trade_id')} "
                        f"emergency_flatten_order_id={atomic_trade_state.get('emergency_order_id')} "
                        f"emergency_flatten_perm_id={atomic_trade_state.get('emergency_perm_id')} "
                        f"emergency_flatten_last_status={atomic_trade_state.get('emergency_status')} "
                        "safe_to_submit_new_flatten=False "
                        f"symbol_level_inflight={atomic_symbol_state.get('symbol_has_inflight_emergency')} "
                        "idempotency_decision=atomic_reservation_block "
                        "operator_action_required=False"
                    )
                    return {
                        "ok": True,
                        "reason": "atomic_reservation_blocked_existing_inflight",
                        "flat_confirmed": False,
                        "pending": True,
                    }

                previous_emergency_order_id = record.get("emergency_flatten_order_id")
                self.order_to_trade[order.orderId] = trade_id
                record["protective_emergency_active"] = True
                record["protective_emergency_status"] = "submit_in_progress"
                record["emergency_flatten_submit_reserved_at"] = datetime.now(timezone.utc)
                record["emergency_flatten_submit_reservation_id"] = reservation_id
                record["emergency_flatten_order_id"] = order.orderId
                record["emergency_flatten_order_ref"] = order.orderRef
                record["emergency_flatten_action"] = action
                record["emergency_flatten_quantity"] = quantity
                record["emergency_flatten_attempt"] = attempt
                record["emergency_flatten_reason"] = reason_label
                record["emergency_flatten_last_status"] = "submit_in_progress"
                record["state"] = "BROKER_ACK_PENDING"

            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_SUBMIT_RESERVED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"attempt={attempt} "
                f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                f"emergency_flatten_order_id={order.orderId} "
                f"previous_emergency_order_id={previous_emergency_order_id} "
                f"flatten_action={action} "
                f"flatten_quantity={quantity} "
                f"position_qty_before={position_qty} "
                f"reservation_id={reservation_id} "
                "protective_emergency_status=submit_in_progress "
                "idempotency_decision=reserve_submit_slot "
                "operator_action_required=False"
            )

            try:
                self.broker_write_place_order(
                    contract,
                    order,
                    caller="protective_emergency_flatten",
                    reason_label=reason_label,
                    trade_id=trade_id,
                    symbol=symbol,
                )
            except Exception as exc:
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        record["protective_emergency_status"] = "submit_failed_submission_uncertain"
                        record["emergency_flatten_last_status"] = "submit_failed_submission_uncertain"
                        record["state"] = "BROKER_ACK_PENDING"
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_SUBMIT_UNCERTAIN | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"attempt={attempt} "
                    f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                    f"flatten_action={action} "
                    f"flatten_quantity={quantity} "
                    f"emergency_flatten_order_id={order.orderId} "
                    f"submit_reservation_id={reservation_id} "
                    f"reserved_order_id={order.orderId} "
                    f"broker_write_exception={exc} "
                    "broker_submission_uncertain=True "
                    "retry_allowed=False "
                    "retry_evidence=missing "
                    f"reason=flatten_order_submit_uncertain:{exc} "
                    "operator_action_required=True "
                    "decision=preserve_active_lock"
                )
                return {
                    "ok": False,
                    "reason": f"flatten_order_submit_failed:{exc}",
                    "flat_confirmed": False,
                }
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    record["protective_emergency_status"] = "flatten_order_submitted"
                    record["emergency_flatten_last_status"] = "Submitted"
                    record["emergency_flatten_submitted_at"] = datetime.now(timezone.utc)
                    record["state"] = "BROKER_ACK_PENDING"
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_ORDER_SUBMITTED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"attempt={attempt} "
                f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                f"flatten_action={action} "
                f"flatten_quantity={quantity} "
                f"emergency_flatten_order_id={order.orderId} "
                f"position_qty_before={position_qty} "
                f"previous_emergency_order_id={previous_emergency_order_id} "
                f"conflicting_order_ids_cancel_requested={conflicting_order_ids} "
                f"reason={reason_label} "
                "protective_emergency_active=True "
                "protective_emergency_status=flatten_order_submitted "
                "idempotency_decision=submit_new_order "
                "decision=offset_actual_broker_position_with_mkt_order"
            )

            self.broker_write_sleep(
                PROTECTIVE_SL_MISSING_RECHECK_SECONDS,
                caller="protective_emergency_flatten",
                reason_label=f"{reason_label}:post_mkt_recheck_wait",
                trade_id=trade_id,
                symbol=symbol,
            )

            try:
                post_qty = read_position_qty(f"attempt_{attempt}:post_mkt_position_recheck")
            except Exception as exc:
                return mark_broker_read_failed(exc)

            logger.critical(
                "PROTECTIVE_EMERGENCY_POSITION_RECHECK | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"attempt={attempt} "
                f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                f"broker_position_qty={post_qty} "
                f"position_qty_before={position_qty} "
                f"position_qty_after={post_qty} "
                f"flatten_action={action} "
                f"flatten_quantity={quantity} "
                f"emergency_flatten_order_id={order.orderId} "
                f"reason={reason_label} "
                "decision=post_mkt_flatten_position_check"
            )

            if post_qty == 0.0:
                return confirm_flat(post_qty, "flat_confirmed_after_mkt_flatten")

            try:
                post_submit_snapshot = self.read_protective_emergency_broker_snapshot(
                    symbol,
                    f"{reason_label}:attempt_{attempt}:post_submit_inflight_snapshot",
                    trade_id=trade_id,
                )
            except Exception as exc:
                return mark_broker_read_failed(exc)
            with self.trade_analysis_lock:
                live_record = dict(self.trade_analysis.get(trade_id) or {})
            post_submit_state = self.get_emergency_flatten_inflight_state(
                live_record,
                post_submit_snapshot,
            )
            if post_submit_state.get("order_inflight") or not post_submit_state.get("order_terminal"):
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is not None:
                        record["protective_emergency_active"] = True
                        record["protective_emergency_status"] = "pending_position_confirmation"
                        record["state"] = "BROKER_ACK_PENDING"
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_RETRY_BLOCKED_INFLIGHT | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"attempt={attempt} "
                    f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                    f"emergency_flatten_order_id={post_submit_state.get('emergency_order_id')} "
                    f"emergency_flatten_perm_id={post_submit_state.get('emergency_perm_id')} "
                    f"emergency_flatten_last_status={post_submit_state.get('emergency_status')} "
                    f"position_qty_after={post_qty} "
                    f"inflight_grace_seconds={PROTECTIVE_EMERGENCY_FLATTEN_INFLIGHT_GRACE_SECONDS} "
                    f"submitted_age_sec={post_submit_state.get('submitted_age_sec')} "
                    f"reserved_age_sec={post_submit_state.get('reserved_age_sec')} "
                    "safe_to_submit_new_flatten=False "
                    "idempotency_decision=block_retry_existing_order_inflight "
                    f"reason={post_submit_state.get('reason')} "
                    "operator_action_required=False"
                )
                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_RETRY_EVIDENCE_MISSING | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"emergency_flatten_order_id={post_submit_state.get('emergency_order_id')} "
                    f"emergency_flatten_last_status={post_submit_state.get('emergency_status')} "
                    f"broker_position_qty={post_qty} "
                    "retry_allowed=False "
                    "retry_evidence=missing_terminal_non_fill_or_reconciliation "
                    f"reason={post_submit_state.get('reason')} "
                    "operator_action_required=False"
                )
                return {
                    "ok": True,
                    "reason": "pending_position_confirmation",
                    "flat_confirmed": False,
                    "pending": True,
                }

            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_RETRY_ALLOWED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"attempt={attempt} "
                f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
                f"emergency_flatten_order_id={post_submit_state.get('emergency_order_id')} "
                f"emergency_flatten_last_status={post_submit_state.get('emergency_status')} "
                f"position_qty_after={post_qty} "
                "safe_to_submit_new_flatten=True "
                "reason=previous_emergency_order_terminal_position_still_open"
            )
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_RETRY_EVIDENCE_ACCEPTED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"emergency_flatten_order_id={post_submit_state.get('emergency_order_id')} "
                f"emergency_flatten_last_status={post_submit_state.get('emergency_status')} "
                f"broker_position_qty={post_qty} "
                "retry_allowed=True "
                "retry_evidence=terminal_non_fill_status "
                "operator_action_required=False"
            )

        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is not None:
                record["protective_emergency_active"] = True
                record["protective_emergency_status"] = "failed_position_still_open"
                record["protective_emergency_reason"] = reason_label
                record["state"] = "BROKER_ACK_PENDING"

        logger.critical(
            "PROTECTIVE_EMERGENCY_FLATTEN_FAILED | "
            f"trade_id={trade_id} "
            f"symbol={symbol} "
            f"reason={reason_label} "
            f"max_attempts={PROTECTIVE_SL_MISSING_MAX_ATTEMPTS} "
            "protective_emergency_active=True "
            "protective_emergency_status=failed_position_still_open "
            "operator_action_required=True "
            "decision=preserve_active_lock_position_still_open"
        )
        return {
            "ok": False,
            "reason": "failed_position_still_open",
            "flat_confirmed": False,
        }

    def classify_position_protection_context(self, record, broker_reality, broker_confirmation):
        entry_realized = bool(
            record and
            (record.get("entry_filled") or self.has_realized_parent_entry(record))
        )
        position_match = bool(broker_reality and broker_reality.get("has_position_match"))
        parent_visible = bool(broker_confirmation and broker_confirmation.get("parent_visible"))
        tp_visible = bool(broker_confirmation and broker_confirmation.get("tp_visible"))
        sl_visible = bool(broker_confirmation and broker_confirmation.get("sl_visible"))
        quantity_coverage = self.assess_bracket_quantity_coverage(
            record,
            broker_confirmation=broker_confirmation,
            broker_reality=broker_reality,
        )
        sl_coverage_ok = bool(quantity_coverage.get("protective_sl_coverage_ok"))

        protection_class = "NO_POSITION_PROTECTION_CONTEXT"
        target_state = None
        reason = "no_realized_entry_or_no_position_match"

        if entry_realized and position_match:
            if sl_visible and tp_visible and sl_coverage_ok:
                protection_class = "IN_POSITION_WITH_PROTECTIVE_EXITS"
                target_state = "EXIT_WORKING"
                reason = "parent_filled_position_open_tp_sl_visible"
            elif sl_visible and sl_coverage_ok:
                protection_class = "IN_POSITION_WITH_PRIMARY_SL_PROTECTION"
                target_state = "EXIT_WORKING"
                reason = "parent_filled_position_open_sl_visible_tp_missing"
            elif sl_visible:
                protection_class = "URGENT_RISK_STATE_SL_MISSING"
                target_state = "BROKER_ACK_PENDING"
                reason = quantity_coverage.get("quantity_mismatch_reason") or quantity_coverage.get("quantity_state")
            elif tp_visible:
                protection_class = "URGENT_RISK_STATE_SL_MISSING"
                target_state = "BROKER_ACK_PENDING"
                reason = "position_open_sl_missing_tp_visible"
            else:
                protection_class = "URGENT_RISK_STATE_UNPROTECTED_POSITION"
                target_state = "BROKER_ACK_PENDING"
                reason = "position_open_no_protective_exits_visible"

        return {
            "has_position_protection_context": entry_realized and position_match,
            "entry_realized": entry_realized,
            "position_match": position_match,
            "parent_visible": parent_visible,
            "tp_visible": tp_visible,
            "sl_visible": sl_visible,
            "sl_coverage_ok": sl_coverage_ok,
            "sl_coverage_quantity": quantity_coverage.get("protective_sl_coverage"),
            "required_sl_coverage": quantity_coverage.get("open_position_estimate"),
            "quantity_state": quantity_coverage.get("quantity_state"),
            "quantity_mismatch_reason": quantity_coverage.get("quantity_mismatch_reason"),
            "protection_class": protection_class,
            "target_state": target_state,
            "reason": reason,
        }

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

    def get_time_exit_effective_exit_reason(self, record):
        if (
            record
            and (
                record.get("time_exit_reason") == TIME_EXIT_REASON
                or record.get("emergency_flatten_reason") == TIME_EXIT_REASON
            )
        ):
            return TIME_EXIT_REASON
        return PROTECTIVE_EMERGENCY_EXIT_REASON

    def arm_time_exit_on_entry_exposure(self, trade_id, record, fill_time=None):
        if not TIME_EXIT_ENABLED or record.get("entry_exposure_started_at") is not None:
            return

        started_at = fill_time if isinstance(fill_time, datetime) else datetime.now(timezone.utc)
        deadline_epoch = started_at.timestamp() + TIME_EXIT_AFTER_SECONDS
        record["entry_exposure_started_at"] = started_at
        record["time_exit_deadline_at"] = deadline_epoch
        record["time_exit_status"] = "TIME_EXIT_ARMED"
        record["time_exit_reason"] = TIME_EXIT_REASON

        self.append_trade_event(
            trade_id,
            f"TIME EXIT ARMED timeout_sec={TIME_EXIT_AFTER_SECONDS} "
            f"deadline_epoch={round(deadline_epoch, 3)}"
        )
        logger.warning(
            "TIME_EXIT_ARMED | "
            f"trade_id={trade_id} "
            f"symbol={record.get('symbol')} "
            f"side={record.get('side')} "
            f"signal_id={record.get('signal_id')} "
            f"entry_exposure_started_at={self.to_iso(started_at)} "
            f"deadline_epoch={round(deadline_epoch, 3)} "
            f"reason={TIME_EXIT_REASON}"
        )

    def process_time_exit_deadlines(self):
        if not TIME_EXIT_ENABLED:
            return

        now_epoch = time.time()
        now_dt = datetime.now(timezone.utc)
        with self.trade_analysis_lock:
            candidates = [
                dict(record)
                for record in self.trade_analysis.values()
                if (
                    not record.get("summary_logged")
                    and not record.get("closed")
                    and record.get("entry_exposure_started_at") is not None
                    and record.get("time_exit_deadline_at") is not None
                    and record.get("time_exit_status") in {
                        "TIME_EXIT_ARMED",
                        "TIME_EXIT_DUE",
                        "TIME_EXIT_SUBMITTED",
                        "TIME_EXIT_PENDING_CONFIRMATION",
                    }
                    and float(record.get("cumulative_entry_quantity") or 0.0) > float(record.get("cumulative_exit_quantity") or 0.0)
                    and float(record.get("time_exit_deadline_at") or 0.0) <= now_epoch
                )
            ]

        for record_snapshot in candidates:
            trade_id = record_snapshot["trade_id"]
            symbol = record_snapshot["symbol"]
            side = record_snapshot.get("side")
            signal_id = record_snapshot.get("signal_id")

            with self.trade_analysis_lock:
                live_record = self.trade_analysis.get(trade_id)
                if live_record is None or live_record.get("summary_logged") or live_record.get("closed"):
                    continue
                if live_record.get("time_exit_status") == "TIME_EXIT_ARMED":
                    live_record["time_exit_status"] = "TIME_EXIT_DUE"
                    self.append_trade_event(
                        trade_id,
                        f"TIME EXIT DEADLINE REACHED deadline_epoch={live_record.get('time_exit_deadline_at')}"
                    )
            logger.warning(
                "TIME_EXIT_DEADLINE_REACHED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"side={side} "
                f"signal_id={signal_id} "
                f"deadline_epoch={record_snapshot.get('time_exit_deadline_at')} "
                f"now_epoch={round(now_epoch, 3)} "
                f"reason={TIME_EXIT_REASON}"
            )

            try:
                positions = self.broker_read_positions(
                    caller="process_time_exit_deadlines",
                    reason_label=TIME_EXIT_REASON,
                    trade_id=trade_id,
                    symbol=symbol,
                    trade_analysis_lock_context="not_locked",
                    failure_log_level="error",
                )
                broker_position_qty = self.get_position_quantity_from_positions(positions, symbol)
            except Exception as exc:
                with self.trade_analysis_lock:
                    live_record = self.trade_analysis.get(trade_id)
                    if live_record is not None:
                        live_record["time_exit_status"] = "TIME_EXIT_FAILED_UNCERTAIN"
                        live_record["time_exit_last_attempt_at"] = now_dt
                        live_record["state"] = "BROKER_ACK_PENDING"
                logger.critical(
                    "TIME_EXIT_FAILED_UNCERTAIN | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"signal_id={signal_id} "
                    f"failure={exc} "
                    "decision=preserve_active_lock_no_blind_flatten"
                )
                continue

            logger.warning(
                "TIME_EXIT_BROKER_REALITY | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"side={side} "
                f"signal_id={signal_id} "
                f"broker_position_qty={broker_position_qty} "
                f"cumulative_entry_quantity={record_snapshot.get('cumulative_entry_quantity')} "
                f"cumulative_exit_quantity={record_snapshot.get('cumulative_exit_quantity')} "
                f"reason={TIME_EXIT_REASON}"
            )

            if broker_position_qty == 0.0:
                finalized = False
                with self.trade_analysis_lock:
                    live_record = self.trade_analysis.get(trade_id)
                    if live_record is not None:
                        live_record["time_exit_status"] = "TIME_EXIT_BROKER_FLAT_CONFIRMED"
                        live_record["time_exit_last_attempt_at"] = now_dt
                logger.warning(
                    "TIME_EXIT_FLATTEN_SUBMIT_SKIPPED | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"signal_id={signal_id} "
                    "broker_position_qty=0.0 "
                    "time_exit_status=TIME_EXIT_BROKER_FLAT_CONFIRMED "
                    "reason=broker_flat_before_time_exit_flatten"
                )
                self.finalize_trade_if_complete(trade_id)
                with self.trade_analysis_lock:
                    live_record = self.trade_analysis.get(trade_id)
                    finalized = bool(live_record and live_record.get("summary_logged"))
                    if live_record is not None and finalized:
                        live_record["time_exit_status"] = "TIME_EXIT_COMPLETED"
                        live_record["time_exit_completed_at"] = live_record.get("time_exit_completed_at") or now_dt
                logger.warning(
                    "TIME_EXIT_BROKER_FLAT_FINALIZE_CHECK | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"signal_id={signal_id} "
                    f"finalized={finalized} "
                    f"time_exit_status={'TIME_EXIT_COMPLETED' if finalized else 'TIME_EXIT_BROKER_FLAT_CONFIRMED'}"
                )
                continue

            with self.trade_analysis_lock:
                live_record = self.trade_analysis.get(trade_id)
                if live_record is None:
                    continue
                if (
                    live_record.get("time_exit_order_id") is not None
                    or live_record.get("emergency_flatten_order_id") is not None
                    or live_record.get("time_exit_status") in {"TIME_EXIT_SUBMITTED", "TIME_EXIT_PENDING_CONFIRMATION"}
                ):
                    live_record["time_exit_status"] = "TIME_EXIT_PENDING_CONFIRMATION"
                    live_record["state"] = "BROKER_ACK_PENDING"
                    existing_order_id = live_record.get("time_exit_order_id") or live_record.get("emergency_flatten_order_id")
                    logger.warning(
                        "TIME_EXIT_FLATTEN_SUBMIT_SKIPPED | "
                        f"trade_id={trade_id} "
                        f"symbol={symbol} "
                        f"side={side} "
                        f"signal_id={signal_id} "
                        f"existing_time_exit_order_id={existing_order_id} "
                        "reason=time_exit_flatten_already_inflight"
                    )
                    continue
                if int(live_record.get("time_exit_attempt_count") or 0) >= TIME_EXIT_MAX_ATTEMPTS:
                    live_record["time_exit_status"] = "TIME_EXIT_FAILED_UNCERTAIN"
                    live_record["state"] = "BROKER_ACK_PENDING"
                    logger.critical(
                        "TIME_EXIT_FAILED_UNCERTAIN | "
                        f"trade_id={trade_id} "
                        f"symbol={symbol} "
                        f"side={side} "
                        f"signal_id={signal_id} "
                        f"attempt_count={live_record.get('time_exit_attempt_count')} "
                        f"max_attempts={TIME_EXIT_MAX_ATTEMPTS} "
                        "decision=preserve_active_lock_max_attempts_reached"
                    )
                    continue
                live_record["time_exit_attempt_count"] = int(live_record.get("time_exit_attempt_count") or 0) + 1
                live_record["time_exit_last_attempt_at"] = now_dt
                live_record["time_exit_status"] = "TIME_EXIT_SUBMITTED"
                attempt_count = live_record["time_exit_attempt_count"]

            logger.warning(
                "TIME_EXIT_CHILD_CANCEL_REQUESTED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"side={side} "
                f"signal_id={signal_id} "
                f"attempt={attempt_count} "
                "policy=reuse_protective_emergency_flatten_cleanup "
                "sequence=cancel_known_bot_owned_orders_then_recheck_then_market_flatten "
                f"reason={TIME_EXIT_REASON}"
            )
            result = self.emergency_flatten_unprotected_position(
                trade_id,
                symbol,
                TIME_EXIT_REASON,
                protection_context={
                    "protection_class": "TIME_EXIT_MAX_DURATION",
                    "time_exit": True,
                },
            )

            with self.trade_analysis_lock:
                live_record = self.trade_analysis.get(trade_id)
                time_exit_order_id = None
                if live_record is not None:
                    live_record["time_exit_order_id"] = live_record.get("emergency_flatten_order_id")
                    time_exit_order_id = live_record.get("time_exit_order_id")
                    if result.get("flat_confirmed"):
                        live_record["time_exit_status"] = "TIME_EXIT_BROKER_FLAT_CONFIRMED"
                    elif result.get("pending") or result.get("ok"):
                        live_record["time_exit_status"] = "TIME_EXIT_PENDING_CONFIRMATION"
                    else:
                        live_record["time_exit_status"] = "TIME_EXIT_FAILED_UNCERTAIN"
                        live_record["state"] = "BROKER_ACK_PENDING"

            if result.get("flat_confirmed"):
                logger.warning(
                    "TIME_EXIT_BROKER_FLAT_CONFIRMED | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"signal_id={signal_id} "
                    f"broker_position_qty={broker_position_qty} "
                    f"reason={result.get('reason')}"
                )
                self.finalize_trade_if_complete(trade_id)
                with self.trade_analysis_lock:
                    live_record = self.trade_analysis.get(trade_id)
                    finalized = bool(live_record and live_record.get("summary_logged"))
                    if live_record is not None and finalized:
                        live_record["time_exit_status"] = "TIME_EXIT_COMPLETED"
                        live_record["time_exit_completed_at"] = live_record.get("time_exit_completed_at") or datetime.now(timezone.utc)
                logger.warning(
                    "TIME_EXIT_FINALIZE_CHECK | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"signal_id={signal_id} "
                    f"finalized={finalized} "
                    f"time_exit_status={'TIME_EXIT_COMPLETED' if finalized else 'TIME_EXIT_BROKER_FLAT_CONFIRMED'}"
                )
            elif result.get("pending") or result.get("ok"):
                logger.warning(
                    "TIME_EXIT_FLATTEN_SUBMITTED | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"signal_id={signal_id} "
                    f"time_exit_order_id={time_exit_order_id} "
                    f"broker_position_qty_before={broker_position_qty} "
                    f"reason={TIME_EXIT_REASON}"
                )
                logger.warning(
                    "TIME_EXIT_PENDING_CONFIRMATION | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"signal_id={signal_id} "
                    f"time_exit_order_id={time_exit_order_id} "
                    f"reason={result.get('reason')}"
                )
            else:
                logger.critical(
                    "TIME_EXIT_FAILED_UNCERTAIN | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"side={side} "
                    f"signal_id={signal_id} "
                    f"reason={result.get('reason')} "
                    "decision=preserve_active_lock"
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
            open_trades, open_orders = self.broker_read_open_trades_open_orders(
                caller="get_partial_timeout_parent_finality_snapshot",
                reason_label="partial_timeout_parent_finality_snapshot",
                trade_id=record.get("trade_id") if isinstance(record, dict) else None,
                symbol=record.get("symbol") if isinstance(record, dict) else None,
                trade_analysis_lock_context="not_locked",
            )
            for trade in open_trades:
                order = getattr(trade, "order", None)
                status = getattr(trade, "orderStatus", None)
                order_id = getattr(order, "orderId", None)
                perm_id = getattr(status, "permId", None)
                if order_id == parent_order_id or (parent_perm_id not in (None, 0) and perm_id == parent_perm_id):
                    open_trade_visible = True
                    parent_status = getattr(status, "status", None) or parent_status
                    parent_remaining_quantity = getattr(status, "remaining", None)
                    break

            for order in open_orders:
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

    # P187: parent finality broker snapshot is built outside trade_analysis_lock.
    # Commit only if the live lifecycle fields still match the candidate snapshot.
    def partial_timeout_parent_finality_snapshot_matches_live_record(self, record_snapshot, record):
        if record is None:
            return False
        if record.get("summary_logged"):
            return False
        if not self.is_parent_finality_pending(record):
            return False

        fields = (
            "state",
            "summary_logged",
            "entry_filled",
            "parent_order_id",
            "parent_perm_id",
            "parent_last_status",
            "cumulative_entry_quantity",
            "partial_entry_timeout_snapshot_quantity",
            "partial_entry_timeout_snapshot_remaining_quantity",
            "partial_entry_remainder_cancel_requested_at",
            "partial_entry_parent_finality_pending_since",
            "partial_entry_parent_finality_candidate_quantity",
            "partial_entry_parent_finality_candidate_status",
            "partial_entry_parent_finality_candidate_stable_pass_seen",
        )
        for field in fields:
            if record_snapshot.get(field) != record.get(field):
                return False

        return True

    def log_partial_timeout_parent_finality_snapshot_drift(self, record_snapshot, record, reason_label):
        try:
            logger.warning(
                "PARENT FINALITY SNAPSHOT DRIFT | "
                f"trade_id={record_snapshot.get('trade_id')} "
                f"symbol={record_snapshot.get('symbol')} "
                f"reason_label={reason_label} "
                f"snapshot_state={record_snapshot.get('state')} "
                f"live_state={record.get('state') if record else None} "
                f"snapshot_summary_logged={record_snapshot.get('summary_logged')} "
                f"live_summary_logged={record.get('summary_logged') if record else None} "
                f"snapshot_entry_filled={record_snapshot.get('entry_filled')} "
                f"live_entry_filled={record.get('entry_filled') if record else None} "
                f"snapshot_parent_status={record_snapshot.get('parent_last_status')} "
                f"live_parent_status={record.get('parent_last_status') if record else None} "
                f"snapshot_cumulative_entry_quantity={record_snapshot.get('cumulative_entry_quantity')} "
                f"live_cumulative_entry_quantity={record.get('cumulative_entry_quantity') if record else None} "
                "decision=preserve_current_lifecycle"
            )
        except Exception:
            try:
                logger.exception("PARENT FINALITY SNAPSHOT DRIFT LOG FAILED")
            except Exception:
                pass

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
        self.log_lifecycle_mutation_context(
            mutation_point="promote_partial_entry_to_live_quantity",
            reason_label=reason_label,
            trade_id=trade_id,
            symbol=record.get("symbol") if isinstance(record, dict) else None,
            previous_state=previous_state,
            next_state="ENTRY_FILLED" if record.get("state") in {"ENTRY_WORKING", "BROKER_ACK_PENDING", "SUBMITTING", "CANCELLED", "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL"} else record.get("state"),
        )
        if record.get("state") in {"ENTRY_WORKING", "BROKER_ACK_PENDING", "SUBMITTING", "CANCELLED", "PARTIAL_TIMEOUT_PENDING_PARENT_FINAL"}:
            record["state"] = "ENTRY_FILLED"
        self.clear_partial_entry_timeout(record)

        if not previously_entry_filled:
            self.aggregate_stats["filled_trades"] += 1
            self.get_symbol_stats_bucket(record["symbol"])["filled_trades"] += 1
            if self.is_shadow_test_trade(record):
                self.aggregate_stats["shadow_test_entry_filled_count"] += 1
            logger.info(
                "ENTRY FILL COMPLETED | "
                f"trade_id={trade_id} "
                f"total_quantity={record['realized_entry_quantity']} "
                f"reason={reason_label}"
            )

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

    def has_exit_complete_child_cleanup(self, record):
        if not record or record.get("summary_logged"):
            return False
        exit_reason = record.get("exit_reason")
        if exit_reason not in {"TP", "SL", "MIXED_EXIT"}:
            return False
        entry_quantity = self.get_lifecycle_entry_quantity(record)
        exit_quantity = float(record.get("realized_exit_quantity") or record.get("cumulative_exit_quantity") or 0.0)
        if entry_quantity <= 0 or exit_quantity <= 0:
            return False
        return abs(entry_quantity - exit_quantity) <= 1e-9 and record.get("exit_fill_price") is not None

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

    # P185: move high-risk broker reads out of trade_analysis_lock; no lifecycle semantics change.
    def build_finalize_trade_snapshot_if_ready(self, trade_id):
        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return None
            if record["summary_logged"]:
                logger.info(
                    "FINALIZE SKIPPED | "
                    f"trade_id={trade_id} reason=already_finalized "
                    f"state={record['state']} closed={record['closed']}"
                )
                return None

            ready = False

            if self.is_exit_fully_filled(record) and record["exit_fill_price"] is not None:
                if record["realized_entry_quantity"] is None:
                    record["realized_entry_quantity"] = self.get_lifecycle_entry_quantity(record)
                if record.get("exit_reason") not in {PROTECTIVE_EMERGENCY_EXIT_REASON, TIME_EXIT_REASON}:
                    record["exit_reason"] = self.derive_completed_exit_reason(record)
                ready = record["exit_reason"] in ("TP", "SL", "MIXED_EXIT", PROTECTIVE_EMERGENCY_EXIT_REASON, TIME_EXIT_REASON)
            elif record["entry_filled"] and record["exit_fill_price"] is not None and record["exit_reason"] in ("TP", "SL", "MIXED_EXIT", PROTECTIVE_EMERGENCY_EXIT_REASON, TIME_EXIT_REASON):
                ready = True
            elif (
                record.get("closed")
                and record.get("exit_reason") in {PROTECTIVE_EMERGENCY_EXIT_REASON, TIME_EXIT_REASON}
                and record.get("execution_validation_status") == "broker_flat_confirmed_exit_fill_details_incomplete"
            ):
                ready = True
            elif record["state"] == "INCOMPLETE":
                ready = True
            elif not record["entry_filled"] and record["state"] in ("CANCELLED", "REJECTED"):
                ready = True

            if not ready:
                return None

            record_snapshot = dict(record)
            record_snapshot["_p185_finalize_ready"] = ready
            return record_snapshot

    def finalize_snapshot_matches_live_record(self, record_snapshot, record):
        if record is None:
            return False
        if record.get("summary_logged"):
            return False

        fields = (
            "state",
            "entry_filled",
            "closed",
            "exit_fill_price",
            "exit_reason",
            "realized_entry_quantity",
            "realized_exit_quantity",
            "cumulative_entry_quantity",
            "cumulative_exit_quantity",
            "execution_validation_status",
        )
        for field in fields:
            if record_snapshot.get(field) != record.get(field):
                return False
        return True

    def log_finalize_snapshot_drift(self, record_snapshot, record, reason_label):
        try:
            logger.warning(
                "FINALIZE SNAPSHOT DRIFT | "
                f"trade_id={record_snapshot.get('trade_id')} "
                f"symbol={record_snapshot.get('symbol')} "
                f"snapshot_state={record_snapshot.get('state')} "
                f"live_state={record.get('state') if record else None} "
                f"snapshot_summary_logged={record_snapshot.get('summary_logged')} "
                f"live_summary_logged={record.get('summary_logged') if record else None} "
                f"reason_label={reason_label} "
                "decision=preserve_current_lifecycle"
            )
        except Exception:
            try:
                logger.exception("FINALIZE SNAPSHOT DRIFT LOG FAILED")
            except Exception:
                pass

    def finalize_trade_if_complete(self, trade_id):
        record_snapshot = self.build_finalize_trade_snapshot_if_ready(trade_id)
        if record_snapshot is None:
            return

        broker_reality = self.get_trade_broker_reality(
            record_snapshot,
            trade_analysis_lock_context="not_locked",
        )
        exit_cleanup_visibility_requests = []

        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is None:
                return
            if not self.finalize_snapshot_matches_live_record(record_snapshot, record):
                self.log_finalize_snapshot_drift(
                    record_snapshot,
                    record,
                    "finalize_trade_if_complete",
                )
                return

            ready = record_snapshot.get("_p185_finalize_ready", True)
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
                        "SYMBOL AMBIGUITY RELEASE POLICY | "
                        f"trade_id={trade_id} "
                        f"symbol={record['symbol']} "
                        f"state={record['state']} "
                        "release_policy=symbol_ambiguity_only "
                        "symbol_match=true "
                        "direct_open_trade_match=false "
                        "direct_broker_match=false "
                        "open_order_match=false "
                        "position_match=false "
                        "decision=release "
                        "risk_note=release_despite_symbol_level_ambiguity"
                    )
                    logger.warning(
                        "SYMBOL RELEASE DECISION | "
                        f"trade_id={trade_id} "
                        f"symbol={record['symbol']} "
                        f"state={record['state']} "
                        "release_policy=symbol_ambiguity_only "
                        "symbol_match=true "
                        "decision=release "
                        "risk_note=release_despite_symbol_level_ambiguity "
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
                    logger.info(
                        "FINALIZE DECISION | "
                        f"trade_id={trade_id} "
                        f"symbol={record.get('symbol')} "
                        f"state_before_finalize={record.get('state')} "
                        f"ready={ready} "
                        "exit_complete=None "
                        f"broker_real={broker_reality['broker_real']} "
                        f"release_override={release_override} "
                        f"entry_filled={record.get('entry_filled')} "
                        f"closed={record.get('closed')} "
                        f"exit_reason={record.get('exit_reason')} "
                        f"execution_validation_status={record.get('execution_validation_status')} "
                        f"active_execution_trade_id={getattr(self, 'active_execution_trade_id', None)} "
                        "decision=hold_broker_activity_remains"
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
                f"release_policy={'symbol_ambiguity_only' if release_override else 'none'} "
                f"risk_note={'release_despite_symbol_level_ambiguity' if release_override else 'none'} "
                f"open_trade_match={broker_reality['has_open_trade_match']} "
                f"direct_open_trade_match={broker_reality['has_direct_open_trade_match']} "
                f"symbol_open_trade_match={broker_reality['has_symbol_open_trade_match']} "
                f"open_order_match={broker_reality['has_open_order_match']} "
                f"direct_broker_match={broker_reality['has_direct_broker_match']} "
                f"position_match={broker_reality['has_position_match']}"
            )

            exit_complete = self.is_exit_fully_filled(record) and record["exit_fill_price"] is not None
            emergency_flat_incomplete_details = bool(
                record.get("closed")
                and record.get("exit_reason") in {PROTECTIVE_EMERGENCY_EXIT_REASON, TIME_EXIT_REASON}
                and record.get("execution_validation_status") == "broker_flat_confirmed_exit_fill_details_incomplete"
            )

            if exit_complete or emergency_flat_incomplete_details:
                if not record["entry_filled"]:
                    self.append_anomaly(trade_id, "ENTRY_PARTIAL_LIFECYCLE_CLOSED")
                if (
                    exit_complete
                    and record.get("execution_validation_status") != "broker_reconciled_flat"
                    and record.get("execution_validation_status") != "broker_flat_confirmed_exit_fill_details_incomplete"
                ):
                    self.set_execution_validation_status(
                        trade_id,
                        "exit_complete",
                        f"exit_reason={record.get('exit_reason')}"
                    )
                exit_cleanup_visibility_requests.append(
                    (trade_id, dict(record), "FINALIZE_EXIT_COMPLETE")
                )
                record["closed"] = True
                if record.get("exit_reason") in {PROTECTIVE_EMERGENCY_EXIT_REASON, TIME_EXIT_REASON}:
                    record["protective_emergency_active"] = False
                    record["protective_emergency_status"] = (
                        "flat_confirmed_exit_fill_details_incomplete"
                        if emergency_flat_incomplete_details
                        else "flat_confirmed_stale_orders_cleared"
                    )
                    if record.get("exit_reason") == TIME_EXIT_REASON:
                        record["time_exit_status"] = "TIME_EXIT_COMPLETED"
                        record["time_exit_completed_at"] = record.get("time_exit_completed_at") or datetime.now(timezone.utc)
                self.log_lifecycle_mutation_context(
                    mutation_point="finalize_trade_close",
                    reason_label="exit_complete",
                    trade_id=trade_id,
                    symbol=record.get("symbol") if isinstance(record, dict) else None,
                    previous_state=record.get("state") if isinstance(record, dict) else None,
                    next_state="CLOSED",
                )
                record["state"] = "CLOSED"
                record["gross_pnl"] = 0.0 if emergency_flat_incomplete_details else self.calculate_gross_pnl(record)
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
                    if record.get("bot_stage") == "PRD":
                        self.activate_live_daily_sl_stop(
                            record["trade_id"],
                            record["exit_fill_time"]
                        )
                elif record["exit_reason"] == "MIXED_EXIT":
                    self.aggregate_stats["mixed_exit_count"] += 1
                elif record["exit_reason"] == PROTECTIVE_EMERGENCY_EXIT_REASON:
                    self.aggregate_stats["emergency_flatten_count"] = (
                        self.aggregate_stats.get("emergency_flatten_count", 0) + 1
                    )
                    symbol_bucket["emergency_flatten_count"] = (
                        symbol_bucket.get("emergency_flatten_count", 0) + 1
                    )
                elif record["exit_reason"] == TIME_EXIT_REASON:
                    self.aggregate_stats["time_exit_count"] = (
                        self.aggregate_stats.get("time_exit_count", 0) + 1
                    )
                    symbol_bucket["time_exit_count"] = (
                        symbol_bucket.get("time_exit_count", 0) + 1
                    )

                paper_sl_component_present = (
                    record["exit_reason"] == "SL"
                    or (
                        record["exit_reason"] == "MIXED_EXIT"
                        and float(record.get("sl_exit_quantity") or 0.0) > 0
                    )
                )

                if record.get("bot_stage") == "ACC" and paper_sl_component_present:
                    self.activate_paper_symbol_daily_sl_stop(
                        record["symbol"],
                        record["trade_id"],
                        record["exit_fill_time"]
                    )
            else:
                if record["entry_filled"] and not exit_complete:
                    self.append_anomaly(trade_id, "INCOMPLETE_ENTRY_WITHOUT_FULL_EXIT")
                    self.set_execution_validation_status(
                        trade_id,
                        "validation_incomplete",
                        "entry_filled_without_full_exit"
                    )
                    self.log_lifecycle_mutation_context(
                        mutation_point="finalize_trade_incomplete",
                        reason_label="entry_filled_without_full_exit",
                        trade_id=trade_id,
                        symbol=record.get("symbol") if isinstance(record, dict) else None,
                        previous_state=record.get("state") if isinstance(record, dict) else None,
                        next_state="INCOMPLETE",
                    )
                    record["state"] = "INCOMPLETE"
                if record["state"] == "INCOMPLETE":
                    exit_cleanup_visibility_requests.append(
                        (trade_id, dict(record), "FINALIZE_INCOMPLETE")
                    )
                    self.aggregate_stats["incomplete_count"] += 1
                    if self.is_shadow_test_trade(record):
                        self.aggregate_stats["shadow_test_incomplete_count"] += 1

            if exit_complete:
                finalize_decision = "summarize_exit_complete"
            elif emergency_flat_incomplete_details:
                finalize_decision = "summarize_emergency_flat_fill_details_incomplete"
            elif record["state"] == "INCOMPLETE":
                finalize_decision = "summarize_incomplete"
            elif record["state"] in {"CANCELLED", "REJECTED"}:
                finalize_decision = "summarize_cancelled_or_rejected"
            else:
                finalize_decision = "summarize_other_ready_state"

            logger.info(
                "FINALIZE DECISION | "
                f"trade_id={trade_id} "
                f"symbol={record.get('symbol')} "
                f"state_before_finalize={record.get('state')} "
                f"ready={ready} "
                f"exit_complete={exit_complete} "
                f"emergency_flatten={record.get('exit_reason') == PROTECTIVE_EMERGENCY_EXIT_REASON} "
                f"fill_details_complete={record.get('exit_fill_price') is not None} "
                f"broker_flat_confirmed={record.get('closed') and record.get('exit_reason') == PROTECTIVE_EMERGENCY_EXIT_REASON} "
                f"stale_bot_orders_cleared={record.get('protective_emergency_status') in {'flat_confirmed_stale_orders_cleared', 'flat_confirmed_exit_fill_details_incomplete'}} "
                f"broker_real={broker_reality['broker_real']} "
                f"release_override={release_override} "
                f"entry_filled={record.get('entry_filled')} "
                f"closed={record.get('closed')} "
                f"exit_reason={record.get('exit_reason')} "
                f"execution_validation_status={record.get('execution_validation_status')} "
                f"active_execution_trade_id={getattr(self, 'active_execution_trade_id', None)} "
                f"decision={finalize_decision}"
            )
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
                f"risk_intent_profile={record.get('risk_intent_profile')} "
                f"risk_intent_percent={record.get('risk_intent_percent')} "
                f"risk_intent_status={record.get('risk_intent_status')} "
                f"risk_intent_reason={record.get('risk_intent_reason')} "
                f"theoretical_size_before_containment={record.get('theoretical_size_before_containment')} "
                f"containment_status={record.get('containment_status')} "
                f"containment_stage={record.get('containment_stage')} "
                f"containment_profile={record.get('containment_profile')} "
                f"containment_max_size={record.get('containment_max_size')} "
                f"containment_max_notional={record.get('containment_max_notional')} "
                f"containment_hierarchy={record.get('containment_hierarchy')} "
                f"containment_decision_source={record.get('containment_decision_source')} "
                f"containment_denial_source={record.get('containment_denial_source')} "
                f"triggering_containment_rule={record.get('triggering_containment_rule')} "
                f"notional_model={record.get('notional_model')} "
                f"notional_currency={record.get('notional_currency')} "
                f"capped_size_after_size_containment={record.get('capped_size_after_size_containment')} "
                f"estimated_notional_before_containment={record.get('estimated_notional_before_containment')} "
                f"estimated_notional_after_size_containment={record.get('estimated_notional_after_size_containment')} "
                f"estimated_notional_after_containment={record.get('estimated_notional_after_containment')} "
                f"containment_action={record.get('containment_action')} "
                f"containment_reason={record.get('containment_reason')} "
                f"approved_size_after_containment={record.get('approved_size_after_containment')} "
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
                f"emergency_flatten={record.get('exit_reason') == PROTECTIVE_EMERGENCY_EXIT_REASON} "
                f"fill_details_complete={record.get('exit_fill_price') is not None} "
                f"broker_flat_confirmed={record.get('closed') and record.get('exit_reason') == PROTECTIVE_EMERGENCY_EXIT_REASON} "
                f"stale_bot_orders_cleared={record.get('protective_emergency_status') in {'flat_confirmed_stale_orders_cleared', 'flat_confirmed_exit_fill_details_incomplete'}} "
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
                f"emergency_flatten_count={self.aggregate_stats.get('emergency_flatten_count', 0)} "
                f"gross_pnl={self.aggregate_stats['gross_pnl']} "
                f"commission={self.aggregate_stats['commission']} "
                f"net_pnl={self.aggregate_stats['net_pnl']} "
                f"shadow_test_net_pnl={self.aggregate_stats['shadow_test_net_pnl']} "
                f"symbol_snapshot={json.dumps(symbol_snapshot, sort_keys=True)}"
            )

        for cleanup_trade_id, cleanup_record, cleanup_trigger in exit_cleanup_visibility_requests:
            self.log_exit_cleanup_visibility(
                cleanup_trade_id,
                cleanup_record,
                cleanup_trigger,
                trade_analysis_lock_context="not_locked",
            )

    def update_trade_from_fill(self, fill):
        try:
            execution = fill.execution
            order_id = getattr(execution, "orderId", None)
            price = getattr(execution, "price", None)
            fill_time, execution_time, callback_fill_time = self.get_effective_execution_time(fill, execution)
            realized_quantity = self.normalize_fill_quantity(getattr(execution, "shares", None))

            trade_id, record = self.get_trade_by_order_id(order_id)
            if record is None:
                trade_id, record = self.get_trade_by_emergency_flatten_perm_id(getattr(execution, "permId", None))
            if record is None:
                return

            identity_ok, identity_reason, parsed_order_ref = self.execution_order_ref_matches_record(
                record,
                execution,
                contract=getattr(fill, "contract", None),
            )
            if not identity_ok:
                self.log_broker_fill_without_lifecycle_event(
                    trade_id,
                    identity_reason,
                    "fill_callback_rejected_by_order_ref_identity_gate",
                    fill=fill,
                    execution=execution,
                    contract=getattr(fill, "contract", None),
                )
                if identity_reason == "foreign_run_id":
                    self.log_order_ref_match_decision(
                        "BROKER_FILL_ORPHANED_BY_RUN_ID_MISMATCH",
                        record,
                        parsed_order_ref,
                        identity_reason,
                        execution=execution,
                        decision="reject_current_lifecycle_match",
                    )
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

            exec_id = getattr(execution, "execId", None) or execution_identity
            leg = self.map_order_leg(record, order_id)
            perm_id = getattr(execution, "permId", None) or self.get_record_order_perm_id(record, order_id)
            emergency_flatten_fill_match = (
                order_id == record.get("emergency_flatten_order_id")
                or (
                    perm_id not in (None, 0)
                    and perm_id == record.get("emergency_flatten_perm_id")
                )
            )
            if emergency_flatten_fill_match:
                leg = "EMERGENCY_FLATTEN_EXIT"

            self.append_trade_event(
                trade_id,
                f"FILL execution_identity={execution_identity} orderId={order_id} "
                f"side={getattr(execution, 'side', None)} shares={getattr(execution, 'shares', None)} "
                f"normalized_quantity={realized_quantity} price={price} "
                f"execution_time={self.to_iso(execution_time)} "
                f"callback_fill_time={self.to_iso(callback_fill_time)}"
            )

            exit_cleanup_trigger = None
            exit_cleanup_record_snapshot = None
            mixed_exit_detected = False
            mixed_tp_exit_quantity = None
            mixed_sl_exit_quantity = None

            if order_id == record["parent_order_id"]:
                late_fill_pending = False
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is None:
                        return

                    record["entry_fill_time"] = fill_time
                    previously_entry_filled = record["entry_filled"]
                    previous_cumulative_entry_quantity = float(record.get("cumulative_entry_quantity") or 0.0)
                    previous_finalized_quantity = float(record.get("realized_entry_quantity") or 0.0)
                    self.accumulate_quantity_and_notional(
                        record,
                        "cumulative_entry_quantity",
                        "cumulative_entry_notional",
                        "entry_fill_price",
                        realized_quantity,
                        price
                    )
                    self.log_fill_event(
                        record,
                        order_id,
                        perm_id,
                        exec_id,
                        leg,
                        getattr(execution, "side", None),
                        price,
                        realized_quantity,
                        fill_time,
                        callback_fill_time,
                    )
                    self.log_fill_timing(record, fill_time)
                    if previous_cumulative_entry_quantity <= 0 and float(record.get("cumulative_entry_quantity") or 0.0) > 0:
                        self.arm_time_exit_on_entry_exposure(trade_id, record, fill_time)
                    if previous_cumulative_entry_quantity <= 0:
                        logger.info(
                            "ENTRY FILL STARTED | "
                            f"trade_id={trade_id} "
                            f"order_id={order_id} "
                            f"exec_id={exec_id} "
                            f"quantity={realized_quantity} "
                            f"timestamp={self.to_iso(fill_time)}"
                        )
                    if previous_finalized_quantity > 0:
                        late_fill_pending = True
                        logger.warning(
                            "LATE ENTRY FILL OBSERVED | "
                            f"trade_id={trade_id} "
                            f"order_id={order_id} "
                            f"exec_id={exec_id} "
                            f"previous_finalized_quantity={previous_finalized_quantity} "
                            f"cumulative_entry_quantity={record.get('cumulative_entry_quantity')} "
                            f"timestamp={self.to_iso(fill_time)}"
                        )
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
                        logger.info(
                            "ENTRY PARTIAL | "
                            f"trade_id={trade_id} "
                            f"cumulative={record['cumulative_entry_quantity']} "
                            f"remaining={self.get_parent_remaining_quantity(record)} "
                            f"order_id={order_id} "
                            f"exec_id={exec_id} "
                            f"timestamp={self.to_iso(fill_time)}"
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
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is None:
                        return

                    previous_cumulative_exit_quantity = float(record.get("cumulative_exit_quantity") or 0.0)
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
                    self.log_fill_event(
                        record,
                        order_id,
                        perm_id,
                        exec_id,
                        leg,
                        getattr(execution, "side", None),
                        price,
                        realized_quantity,
                        fill_time,
                        callback_fill_time,
                    )
                    self.log_fill_timing(record, fill_time)
                    if previous_cumulative_exit_quantity <= 0:
                        logger.info(
                            "EXIT STARTED | "
                            f"trade_id={trade_id} "
                            f"order_id={order_id} "
                            f"exec_id={exec_id} "
                            f"leg={leg} "
                            f"timestamp={self.to_iso(fill_time)}"
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
                        exit_cleanup_trigger = "TP_EXIT_FILLED"
                    else:
                        record["state"] = "EXIT_WORKING"
                        self.append_trade_event(
                            trade_id,
                            f"EXIT PARTIAL FILL cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                            f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                            f"tp_exit_quantity={record['tp_exit_quantity']} sl_exit_quantity={record['sl_exit_quantity']} "
                            f"exit_fill={record['exit_fill_price']}"
                        )
                    if record.get("exit_reason") == "MIXED_EXIT":
                        mixed_exit_detected = True
                        mixed_tp_exit_quantity = record["tp_exit_quantity"]
                        mixed_sl_exit_quantity = record["sl_exit_quantity"]
                        self.append_trade_event(
                            trade_id,
                            f"MIXED EXIT DETECTED tp_exit_quantity={mixed_tp_exit_quantity} "
                            f"sl_exit_quantity={mixed_sl_exit_quantity}"
                        )
                    exit_log_snapshot = {
                        "exit_reason": record.get("exit_reason"),
                        "cumulative_exit_quantity": record.get("cumulative_exit_quantity"),
                        "realized_entry_quantity": record.get("realized_entry_quantity"),
                        "realized_exit_quantity": record.get("realized_exit_quantity"),
                        "tp_exit_quantity": record.get("tp_exit_quantity"),
                        "sl_exit_quantity": record.get("sl_exit_quantity"),
                        "exit_fill_price": record.get("exit_fill_price"),
                        "is_exit_complete": self.is_exit_fully_filled(record),
                    }
                    if exit_cleanup_trigger:
                        exit_cleanup_record_snapshot = dict(record)

                if exit_cleanup_trigger and exit_cleanup_record_snapshot is not None:
                    self.log_exit_cleanup_visibility(
                        trade_id,
                        exit_cleanup_record_snapshot,
                        exit_cleanup_trigger
                    )
                if exit_log_snapshot["is_exit_complete"]:
                    logger.info(
                        "EXIT FULLY FILLED | "
                        f"trade_id={trade_id} exit_reason={exit_log_snapshot['exit_reason']} "
                        f"cumulative_exit_quantity={exit_log_snapshot['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={exit_log_snapshot['realized_entry_quantity']} "
                        f"exit_fill_price={exit_log_snapshot['exit_fill_price']}"
                    )
                    logger.info(
                        "EXIT COMPLETED | "
                        f"trade_id={trade_id} "
                        f"reason={exit_log_snapshot['exit_reason']} "
                        f"total_quantity={exit_log_snapshot['realized_exit_quantity']} "
                        f"order_id={order_id} "
                        f"exec_id={exec_id} "
                        f"timestamp={self.to_iso(fill_time)}"
                    )
                else:
                    logger.info(
                        "EXIT PARTIAL FILL | "
                        f"trade_id={trade_id} cumulative_exit_quantity={exit_log_snapshot['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={exit_log_snapshot['realized_entry_quantity']} "
                        f"tp_exit_quantity={exit_log_snapshot['tp_exit_quantity']} sl_exit_quantity={exit_log_snapshot['sl_exit_quantity']} "
                        f"exit_fill_price={exit_log_snapshot['exit_fill_price']}"
                    )
            elif order_id == record["sl_order_id"]:
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is None:
                        return

                    previous_cumulative_exit_quantity = float(record.get("cumulative_exit_quantity") or 0.0)
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
                    self.log_fill_event(
                        record,
                        order_id,
                        perm_id,
                        exec_id,
                        leg,
                        getattr(execution, "side", None),
                        price,
                        realized_quantity,
                        fill_time,
                        callback_fill_time,
                    )
                    self.log_fill_timing(record, fill_time)
                    if previous_cumulative_exit_quantity <= 0:
                        logger.info(
                            "EXIT STARTED | "
                            f"trade_id={trade_id} "
                            f"order_id={order_id} "
                            f"exec_id={exec_id} "
                            f"leg={leg} "
                            f"timestamp={self.to_iso(fill_time)}"
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
                        exit_cleanup_trigger = "SL_EXIT_FILLED"
                    else:
                        record["state"] = "EXIT_WORKING"
                        self.append_trade_event(
                            trade_id,
                            f"EXIT PARTIAL FILL cumulative_exit_quantity={record['cumulative_exit_quantity']} "
                            f"realized_entry_quantity={record.get('realized_entry_quantity')} "
                            f"tp_exit_quantity={record['tp_exit_quantity']} sl_exit_quantity={record['sl_exit_quantity']} "
                            f"exit_fill={record['exit_fill_price']}"
                        )
                    if record.get("exit_reason") == "MIXED_EXIT":
                        mixed_exit_detected = True
                        mixed_tp_exit_quantity = record["tp_exit_quantity"]
                        mixed_sl_exit_quantity = record["sl_exit_quantity"]
                        self.append_trade_event(
                            trade_id,
                            f"MIXED EXIT DETECTED tp_exit_quantity={mixed_tp_exit_quantity} "
                            f"sl_exit_quantity={mixed_sl_exit_quantity}"
                        )
                    exit_log_snapshot = {
                        "exit_reason": record.get("exit_reason"),
                        "cumulative_exit_quantity": record.get("cumulative_exit_quantity"),
                        "realized_entry_quantity": record.get("realized_entry_quantity"),
                        "realized_exit_quantity": record.get("realized_exit_quantity"),
                        "tp_exit_quantity": record.get("tp_exit_quantity"),
                        "sl_exit_quantity": record.get("sl_exit_quantity"),
                        "exit_fill_price": record.get("exit_fill_price"),
                        "is_exit_complete": self.is_exit_fully_filled(record),
                    }
                    if exit_cleanup_trigger:
                        exit_cleanup_record_snapshot = dict(record)

                if exit_cleanup_trigger and exit_cleanup_record_snapshot is not None:
                    self.log_exit_cleanup_visibility(
                        trade_id,
                        exit_cleanup_record_snapshot,
                        exit_cleanup_trigger
                    )
                if exit_log_snapshot["is_exit_complete"]:
                    logger.info(
                        "EXIT FULLY FILLED | "
                        f"trade_id={trade_id} exit_reason={exit_log_snapshot['exit_reason']} "
                        f"cumulative_exit_quantity={exit_log_snapshot['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={exit_log_snapshot['realized_entry_quantity']} "
                        f"exit_fill_price={exit_log_snapshot['exit_fill_price']}"
                    )
                    logger.info(
                        "EXIT COMPLETED | "
                        f"trade_id={trade_id} "
                        f"reason={exit_log_snapshot['exit_reason']} "
                        f"total_quantity={exit_log_snapshot['realized_exit_quantity']} "
                        f"order_id={order_id} "
                        f"exec_id={exec_id} "
                        f"timestamp={self.to_iso(fill_time)}"
                    )
                else:
                    logger.info(
                        "EXIT PARTIAL FILL | "
                        f"trade_id={trade_id} cumulative_exit_quantity={exit_log_snapshot['cumulative_exit_quantity']} "
                        f"realized_entry_quantity={exit_log_snapshot['realized_entry_quantity']} "
                        f"tp_exit_quantity={exit_log_snapshot['tp_exit_quantity']} sl_exit_quantity={exit_log_snapshot['sl_exit_quantity']} "
                        f"exit_fill_price={exit_log_snapshot['exit_fill_price']}"
                    )
            elif emergency_flatten_fill_match:
                with self.trade_analysis_lock:
                    record = self.trade_analysis.get(trade_id)
                    if record is None:
                        return

                    record["exit_fill_time"] = fill_time
                    self.accumulate_quantity_and_notional(
                        record,
                        "cumulative_exit_quantity",
                        "cumulative_exit_notional",
                        "exit_fill_price",
                        realized_quantity,
                        price,
                    )
                    self.accumulate_quantity_and_notional(
                        record,
                        "emergency_flatten_exit_quantity",
                        "emergency_flatten_exit_notional",
                        "emergency_flatten_exit_fill_price",
                        realized_quantity,
                        price,
                    )
                    record["realized_exit_quantity"] = float(record.get("cumulative_exit_quantity") or 0.0)
                    record["exit_reason"] = self.get_time_exit_effective_exit_reason(record)
                    record["protective_emergency_status"] = "flatten_fill_observed"
                    if record["exit_reason"] == TIME_EXIT_REASON:
                        record["time_exit_status"] = "TIME_EXIT_PENDING_CONFIRMATION"
                        record["time_exit_order_id"] = record.get("emergency_flatten_order_id")
                    if not record.get("closed"):
                        record["state"] = "BROKER_ACK_PENDING"
                    self.log_fill_event(
                        record,
                        order_id,
                        perm_id,
                        exec_id,
                        leg,
                        getattr(execution, "side", None),
                        price,
                        realized_quantity,
                        fill_time,
                        callback_fill_time,
                    )
                    self.log_fill_timing(record, fill_time)
                    self.append_trade_event(
                        trade_id,
                        f"PROTECTIVE/TIME EXIT FLATTEN FILL quantity={realized_quantity} "
                        f"price={price} cumulative_exit_quantity={record.get('cumulative_exit_quantity')}"
                    )
                    emergency_fill_snapshot = {
                        "cumulative_exit_quantity": record.get("cumulative_exit_quantity"),
                        "realized_exit_quantity": record.get("realized_exit_quantity"),
                        "exit_fill_price": record.get("exit_fill_price"),
                        "protective_emergency_status": record.get("protective_emergency_status"),
                    }

                logger.critical(
                    "PROTECTIVE_EMERGENCY_FLATTEN_FILL | "
                    f"trade_id={trade_id} "
                    f"symbol={record.get('symbol')} "
                    f"order_id={order_id} "
                    f"perm_id={perm_id} "
                    f"exec_id={exec_id} "
                    f"leg=emergency_flatten_exit "
                    f"quantity={realized_quantity} "
                    f"price={price} "
                    f"cumulative_exit_quantity={emergency_fill_snapshot['cumulative_exit_quantity']} "
                    f"realized_exit_quantity={emergency_fill_snapshot['realized_exit_quantity']} "
                    f"exit_fill_price={emergency_fill_snapshot['exit_fill_price']} "
                    f"protective_emergency_status={emergency_fill_snapshot['protective_emergency_status']} "
                        f"exit_reason={self.get_time_exit_effective_exit_reason(record)} "
                        "decision=map_emergency_flatten_fill_as_exit"
                )

            if mixed_exit_detected:
                logger.warning(
                    "MIXED EXIT DETECTED | "
                    f"trade_id={trade_id} tp_exit_quantity={mixed_tp_exit_quantity} "
                    f"sl_exit_quantity={mixed_sl_exit_quantity}"
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
            execution = getattr(fill, "execution", None)
            exec_id = getattr(report, "execId", None) or getattr(execution, "execId", None) or commission_identity
            leg = self.map_order_leg(record, order_id)
            trade_status = getattr(trade, "orderStatus", None)
            perm_id = getattr(trade_status, "permId", None) or self.get_record_order_perm_id(record, order_id)
            self.log_commission_correlation(
                record,
                order_id,
                perm_id,
                exec_id,
                leg,
                getattr(execution, "side", None),
                getattr(execution, "price", None),
                self.normalize_fill_quantity(getattr(execution, "shares", None)),
                commission,
                getattr(report, "currency", None),
                getattr(report, "realizedPNL", None),
                getattr(fill, "time", None),
            )

            should_finalize = False
            late_commission_applied_log = None
            late_commission_recorded_log = None

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
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
                        net_pnl_delta = record["net_pnl"] - previous_net_pnl
                        self.aggregate_stats["commission"] = round(self.aggregate_stats["commission"] + commission, 2)
                        self.aggregate_stats["net_pnl"] = round(
                            self.aggregate_stats["net_pnl"] + net_pnl_delta,
                            2
                        )
                        if self.is_shadow_test_trade(record):
                            self.adjust_shadow_test_net_pnl(net_pnl_delta)
                        symbol_bucket = self.get_symbol_stats_bucket(record["symbol"])
                        symbol_bucket["commission"] = round(symbol_bucket["commission"] + commission, 2)
                        symbol_bucket["net_pnl"] = round(
                            symbol_bucket["net_pnl"] + net_pnl_delta,
                            2
                        )
                        self.append_trade_event(
                            trade_id,
                            f"LATE COMMISSION APPLIED commission={commission} "
                            f"net_pnl={record['net_pnl']}"
                        )
                        late_commission_applied_log = {
                            "net_pnl": record["net_pnl"],
                        }
                    else:
                        self.append_trade_event(
                            trade_id,
                            f"LATE COMMISSION RECORDED commission={commission}"
                        )
                        late_commission_recorded_log = {
                            "closed": record["closed"],
                        }
                else:
                    should_finalize = True

            if late_commission_applied_log is not None:
                logger.info(
                    "LATE COMMISSION APPLIED | "
                    f"trade_id={trade_id} commission={commission} "
                    f"commission_identity={commission_identity} "
                    f"net_pnl={late_commission_applied_log['net_pnl']}"
                )
                return

            if late_commission_recorded_log is not None:
                logger.info(
                    "LATE COMMISSION RECORDED | "
                    f"trade_id={trade_id} commission={commission} "
                    f"commission_identity={commission_identity} "
                    f"closed={late_commission_recorded_log['closed']}"
                )
                return

            if should_finalize:
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

            open_trades, open_orders = self.broker_read_open_trades_open_orders(
                caller="cancel_partial_entry_remainder",
                reason_label="partial_entry_remainder_cancel",
                trade_id=trade_id,
                symbol=record_snapshot.get("symbol") if isinstance(record_snapshot, dict) else None,
                trade_analysis_lock_context="not_locked",
            )
            for trade in open_trades:
                order = getattr(trade, "order", None)
                if getattr(order, "orderId", None) == parent_order_id:
                    parent_trade = trade
                    break

            if parent_trade is not None:
                self.broker_write_cancel_order(
                    parent_trade.order,
                    caller="cancel_partial_entry_remainder",
                    reason_label="partial_entry_remainder_cancel",
                    trade_id=trade_id,
                    symbol=record_snapshot.get("symbol") if isinstance(record_snapshot, dict) else None,
                    real_broker_exposure=True,
                )
                return True, "cancelled_via_open_trade"

            for order in open_orders:
                if getattr(order, "orderId", None) == parent_order_id:
                    parent_order = order
                    break

            if parent_order is not None:
                self.broker_write_cancel_order(
                    parent_order,
                    caller="cancel_partial_entry_remainder",
                    reason_label="partial_entry_remainder_cancel",
                    trade_id=trade_id,
                    symbol=record_snapshot.get("symbol") if isinstance(record_snapshot, dict) else None,
                    real_broker_exposure=True,
                )
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

            broker_reality = self.get_trade_broker_reality(
                candidate,
                trade_analysis_lock_context="not_locked",
            )
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

            if self.reconcile_trade_from_in_session_broker_fills(
                trade_id,
                "partial_timeout_parent_finality_check"
            ):
                continue

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is None or record["summary_logged"] or not self.is_parent_finality_pending(record):
                    continue
                record_snapshot = dict(record)

            finality_snapshot = self.get_partial_timeout_parent_finality_snapshot(record_snapshot)

            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if not self.partial_timeout_parent_finality_snapshot_matches_live_record(record_snapshot, record):
                    self.log_partial_timeout_parent_finality_snapshot_drift(
                        record_snapshot,
                        record,
                        "partial_timeout_parent_finality_snapshot_commit",
                    )
                    if record is not None and not record.get("summary_logged"):
                        self.append_trade_event(
                            trade_id,
                            "PARENT FINALITY SNAPSHOT DRIFT decision=preserve_current_lifecycle"
                        )
                    continue
                record["partial_entry_parent_finality_last_check"] = now_dt
                record["partial_entry_parent_final_status"] = finality_snapshot.get("parent_status")
                record["partial_entry_parent_final_quantity"] = finality_snapshot.get("cumulative_entry_quantity")
                record["partial_entry_parent_final_remaining_quantity"] = finality_snapshot.get("parent_remaining_quantity")
                record["partial_entry_parent_final_authority_quantity"] = finality_snapshot.get("authority_quantity")
                record["partial_entry_parent_final_authority_source"] = finality_snapshot.get("authority_source")
                record_snapshot = dict(record)

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
                        if not self.partial_timeout_parent_finality_snapshot_matches_live_record(record_snapshot, record):
                            self.log_partial_timeout_parent_finality_snapshot_drift(
                                record_snapshot,
                                record,
                                "partial_timeout_parent_finality_drift_preserve",
                            )
                            if record is not None and not record.get("summary_logged"):
                                self.append_trade_event(
                                    trade_id,
                                    "PARENT FINALITY SNAPSHOT DRIFT decision=preserve_current_lifecycle"
                                )
                        elif record is not None and not record["summary_logged"] and self.is_parent_finality_pending(record):
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
                    if not self.partial_timeout_parent_finality_snapshot_matches_live_record(record_snapshot, record):
                        self.log_partial_timeout_parent_finality_snapshot_drift(
                            record_snapshot,
                            record,
                            "partial_timeout_parent_finality_authority_fail_closed",
                        )
                        if record is not None and not record.get("summary_logged"):
                            self.append_trade_event(
                                trade_id,
                                "PARENT FINALITY SNAPSHOT DRIFT decision=preserve_current_lifecycle"
                            )
                        continue
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
                    if not self.partial_timeout_parent_finality_snapshot_matches_live_record(record_snapshot, record):
                        self.log_partial_timeout_parent_finality_snapshot_drift(
                            record_snapshot,
                            record,
                            "partial_timeout_parent_finality_candidate_reject",
                        )
                        if record is not None and not record.get("summary_logged"):
                            self.append_trade_event(
                                trade_id,
                                "PARENT FINALITY SNAPSHOT DRIFT decision=preserve_current_lifecycle"
                            )
                    elif record is not None:
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
                if not self.partial_timeout_parent_finality_snapshot_matches_live_record(record_snapshot, record):
                    self.log_partial_timeout_parent_finality_snapshot_drift(
                        record_snapshot,
                        record,
                        "partial_timeout_parent_finality_candidate_observed",
                    )
                    if record is not None and not record.get("summary_logged"):
                        self.append_trade_event(
                            trade_id,
                            "PARENT FINALITY SNAPSHOT DRIFT decision=preserve_current_lifecycle"
                        )
                    continue
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
                record_snapshot = dict(record)

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
                if not self.partial_timeout_parent_finality_snapshot_matches_live_record(record_snapshot, record):
                    self.log_partial_timeout_parent_finality_snapshot_drift(
                        record_snapshot,
                        record,
                        "partial_timeout_parent_finality_acceptance",
                    )
                    if record is not None and not record.get("summary_logged"):
                        self.append_trade_event(
                            trade_id,
                            "PARENT FINALITY SNAPSHOT DRIFT decision=preserve_current_lifecycle"
                        )
                    continue
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
                record_snapshot = dict(record)

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
                if not self.partial_timeout_parent_finality_snapshot_matches_live_record(record_snapshot, record):
                    self.log_partial_timeout_parent_finality_snapshot_drift(
                        record_snapshot,
                        record,
                        "partial_timeout_parent_finality_promotion",
                    )
                    if record is not None and not record.get("summary_logged"):
                        self.append_trade_event(
                            trade_id,
                            "PARENT FINALITY SNAPSHOT DRIFT decision=preserve_current_lifecycle"
                        )
                    continue
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
                if self.is_current_broker_io_owner_context("update_trade_from_error"):
                    self.get_active_trade_candidates("ERROR_202_CANCEL_CLEANUP")
                else:
                    self.enqueue_broker_system_job(
                        "BROKER_REALITY_RECONCILIATION",
                        "ERROR_202_CANCEL_CLEANUP",
                        symbol=record.get("symbol"),
                        force=False,
                        include_active_candidate_check=True,
                    )
        except Exception:
            logger.exception("TRADE ERROR ANALYSIS FAILED")

    def handle_connectivity_error(self, error_code, error_string):
        now_dt = datetime.now(timezone.utc)

        if error_code == 1100:
            self.connectivity_uncertain = True
            self.connectivity_uncertain_since = now_dt
            self.connectivity_reconstruction_anchor_since = now_dt
            self.connectivity_last_error_code = error_code
            self.connectivity_last_error_message = error_string
            self.connectivity_reconciliation_required = True
            self.session_socket_connected = False
            self.session_initialized = False
            self.session_healthy = False
            logger.warning("CONNECTIVITY UNCERTAIN ENTERED")
            return True

        if error_code == 1102:
            self.connectivity_last_error_code = error_code
            self.connectivity_last_error_message = error_string
            logger.warning("CONNECTIVITY RESTORED")
            if self.connectivity_reconciliation_required:
                logger.warning("CONNECTIVITY RESTORED RECONCILIATION QUEUED")
                self.enqueue_broker_system_job(
                    "BROKER_REALITY_RECONCILIATION",
                    "connectivity_restore_reconciliation",
                    force=False,
                    include_active_candidate_check=True,
                    connectivity_restore_complete=True,
                )
            return True

        return False

    def is_connectivity_execution_blocked(self):
        if self.connectivity_uncertain or self.connectivity_reconciliation_required:
            logger.warning(
                "EXECUTION BLOCKED CONNECTIVITY UNCERTAIN | "
                f"stage={BOT_STAGE} "
                f"connectivity_uncertain={self.connectivity_uncertain} "
                f"connectivity_reconciliation_required={self.connectivity_reconciliation_required} "
                f"connectivity_uncertain_since={self.to_iso(self.connectivity_uncertain_since)} "
                f"connectivity_last_error_code={self.connectivity_last_error_code} "
                f"connectivity_last_error_message={self.connectivity_last_error_message} "
                f"connectivity_reconciliation_completed_at={self.to_iso(self.connectivity_reconciliation_completed_at)} "
                f"active_execution_trade_id={self.active_execution_trade_id}"
            )
            return True
        return False

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

    def is_runtime_symbol_disabled(self, symbol):
        return self._normalize_symbol(symbol) in RUNTIME_DISABLED_SYMBOLS

    def is_eurusd_execution_disabled(self, symbol):
        return self._normalize_symbol(symbol) == "EURUSD"

    def log_eurusd_execution_disabled(self, symbol, payload_format=None, signal_id=None, location=None):
        logger.warning(
            "EURUSD_EXECUTION_DISABLED | "
            f"symbol={self._normalize_symbol(symbol)} "
            f"payload_format={payload_format} "
            f"signal_id={signal_id} "
            f"location={location} "
            "queued=false "
            "order_submitted=false "
            f"reason={EURUSD_EXECUTION_DISABLED_REASON}"
        )

    def get_vwap_atr_distance_magnitude(self, observations, context_label=None):
        raw_distance = None
        raw_distance_value = None
        if isinstance(observations, dict):
            raw_distance = observations.get("distance_from_vwap_atr")
            raw_distance_value = observations.get("distance_from_vwap_atr_value")

        selected_source = None
        selected_value = None
        if raw_distance_value is not None:
            selected_source = "distance_from_vwap_atr_value"
            selected_value = raw_distance_value
        elif raw_distance is not None:
            selected_source = "distance_from_vwap_atr"
            selected_value = raw_distance

        magnitude = None
        if selected_value is not None:
            try:
                magnitude = abs(float(selected_value))
            except Exception:
                magnitude = None

        logger.info(
            "VWAP_ATR_DISTANCE_MAGNITUDE | "
            f"context={context_label} "
            f"raw_distance_from_vwap_atr={raw_distance} "
            f"raw_distance_from_vwap_atr_value={raw_distance_value} "
            f"selected_source={selected_source} "
            f"bot_used_magnitude={magnitude}"
        )
        return magnitude

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

        logger.info(
            f"RAW PAYLOAD RECEIVED | "
            f"{json.dumps(redact_sensitive_payload(normalized['raw_payload']), sort_keys=True)}"
        )
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
            if stage in {"TST", "ACC", "PRD"}:
                policy_branch = "det_gated_execution_enabled"
                allow_queue = True
                policy_reason = (
                    "DET-gated execution is enabled for TST, ACC, and PRD; "
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
        distance_from_vwap_atr_magnitude = self.get_vwap_atr_distance_magnitude(
            obs,
            "assess_det_context",
        )

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

        if distance_from_vwap_atr_magnitude is not None and distance_from_vwap_atr_magnitude > 1.75:
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
        late_distance = distance_from_vwap_atr_magnitude
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

        distance_from_vwap_atr_magnitude = self.get_vwap_atr_distance_magnitude(
            obs,
            "assess_truth_extension_blockers",
        )
        if distance_from_vwap_atr_magnitude is not None:
            if distance_from_vwap_atr_magnitude >= 1.5:
                soft_blockers.append("extended_from_vwap")
            if distance_from_vwap_atr_magnitude >= 2.0 and "late" not in hard_blockers:
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

    def resolve_risk_intent(self, classification):
        risk_profile_result = self.resolve_risk_profile(classification)
        return {
            "classification": classification,
            "risk_intent_profile": risk_profile_result["risk_profile"],
            "risk_intent_percent": risk_profile_result["risk_percent"],
            "allowed_money_risk": risk_profile_result["allowed_money_risk"],
            "risk_intent_status": "RISK_INTENT_DEFINED",
            "risk_intent_reason": "classification_mapped_to_risk_intent",
            "risk_profile": risk_profile_result["risk_profile"],
            "risk_percent": risk_profile_result["risk_percent"],
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
        risk_intent_result = self.resolve_risk_intent(classification)

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
            "risk_intent_profile": risk_intent_result["risk_intent_profile"],
            "risk_intent_percent": risk_intent_result["risk_intent_percent"],
            "risk_intent_status": risk_intent_result["risk_intent_status"],
            "risk_intent_reason": risk_intent_result["risk_intent_reason"],
            "risk_profile": risk_intent_result["risk_profile"],
            "risk_percent": risk_intent_result["risk_percent"],
            "allowed_money_risk": risk_intent_result["allowed_money_risk"],
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
            f"truth_classification={formal_contract['truth_classification']} "
            f"det_classification={formal_contract['det_classification']} "
            f"risk_intent_profile={formal_contract['risk_intent_profile']} "
            f"risk_intent_percent={formal_contract['risk_intent_percent']} "
            f"risk_intent_status={formal_contract['risk_intent_status']} "
            f"risk_intent_reason={formal_contract['risk_intent_reason']} "
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

        if stage != "TST":
            return False, "shadow_test_override_denied_stage_not_tst"
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
        gate_enabled = stage in {"TST", "ACC", "PRD"}
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
            socket_connected = self.broker_read_is_connected(
                caller="update_session_health",
                reason_label="session_health_check",
                failure_log_level="info",
            )
            initialization_complete = (
                socket_connected and
                self.next_order_id is not None and
                self.is_baseline_contract_cache_ready() and
                self.events_attached
            )

            self.session_socket_connected = socket_connected
            self.session_initialized = (
                initialization_complete
                and not self.session_initialization_failed
            )
            self.session_healthy = (
                socket_connected
                and initialization_complete
                and not self.session_initialization_failed
            )
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
            f"initialization_failed={self.session_initialization_failed} "
            f"initialization_failure_reason={self.session_initialization_failure_reason} "
            f"reconnect_count={self.session_reconnect_count} "
            f"order_id={self.next_order_id} "
            f"contract_cache_size={len(self.contract_cache)} "
            f"events_attached={self.events_attached}"
        )

    def mark_session_initialization_failed(self, reason):
        reason_text = str(reason or "session_initialization_failed")
        self.session_initialization_failed = True
        self.session_initialization_failure_reason = reason_text
        self.session_initialized = False
        self.session_healthy = False
        self.startup_reconciliation_completed = False
        self.startup_reconciliation_status = "blocked"
        self.startup_reconciliation_block_reason = reason_text
        self.external_entries_blocked_reason = reason_text
        logger.critical(
            "SESSION_INITIALIZATION_FAILED | "
            f"reason={reason_text} "
            "decision=fail_closed_external_entries_blocked"
        )

    def clear_session_initialization_failure(self, reason_label):
        if not self.session_initialization_failed:
            return
        logger.warning(
            "SESSION_INITIALIZATION_FAILURE_CLEARED | "
            f"previous_reason={self.session_initialization_failure_reason} "
            f"reason={reason_label}"
        )
        self.session_initialization_failed = False
        self.session_initialization_failure_reason = None

    def is_unresolved_contract_qualification_failure(self):
        return (
            self.session_initialization_failed
            and str(self.session_initialization_failure_reason or "").startswith(
                "contract_qualification_failed"
            )
        )

    def ensure_order_id_initialized(self):
        with self.order_id_lock:
            if self.next_order_id is None:
                self.next_order_id = self.broker_get_req_id(
                    caller="connect_ib",
                    reason_label="ensure_order_id_initialized",
                )
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
            self.log_exec_details_event_observability(trade, fill)
            logger.info(f"FILL: {fill}")

            self.update_trade_from_fill(fill)
            self.enqueue_broker_system_job(
                "BROKER_REALITY_RECONCILIATION",
                "post_exec_position_flat_check",
                force=False,
                check_flat_positions=True,
            )

        def on_open_order(trade):
            self.log_open_order_event_observability(trade)
            order = trade.order
            status = trade.orderStatus
            logger.info(
                f"OPEN ORDER EVENT → "
                f"orderId={getattr(order, 'orderId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"orderRef={getattr(order, 'orderRef', None)} "
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
            self.log_order_status_event_observability(trade)
            order = trade.order
            status = trade.orderStatus
            logger.info(
                f"ORDER STATUS EVENT → "
                f"orderId={getattr(order, 'orderId', None)} "
                f"parentId={getattr(order, 'parentId', None)} "
                f"orderRef={getattr(order, 'orderRef', None)} "
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
            if self.handle_connectivity_error(errorCode, errorString):
                return
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
        failed_symbols = []

        for sym in QUALIFIED_FUTURE_SYMBOLS:
            if sym in RUNTIME_DISABLED_SYMBOLS:
                logger.info(
                    "CONTRACT QUALIFICATION SKIPPED | "
                    f"symbol={sym} "
                    "reason=runtime_disabled"
                )
                continue
            try:
                base = self.build_base_contract(sym)
                spec = self.get_instrument_spec(sym)
                details = self.broker_write_req_contract_details(
                    base,
                    caller="qualify_contracts",
                    reason_label="contract_qualification",
                    symbol=sym,
                )

                detail = self.select_front_month_detail(
                    details,
                    requested_symbol=sym,
                    requested_spec=spec,
                    requested_contract=base,
                )
                contract = detail.contract

                self.contract_cache[sym] = contract
                self.contract_min_ticks[sym] = float(getattr(detail, "minTick", spec["tick_size"]))

                logger.info(f"{sym} → {contract.lastTradeDateOrContractMonth}")
            except Exception as exc:
                failed_symbols.append(sym)
                logger.exception(
                    "CONTRACT QUALIFICATION FAILED | "
                    f"symbol={sym} "
                    f"reason={exc}"
                )

        if failed_symbols:
            raise RuntimeError(
                "contract_qualification_failed:" + ",".join(failed_symbols)
            )

    def ensure_contract_cache_initialized(self):
        if self.is_baseline_contract_cache_ready():
            logger.info(f"CONTRACT CACHE BASELINE READY | size={len(self.contract_cache)}")
            return

        logger.info("CONTRACT CACHE MISSING BASELINE | qualifying contracts")
        self.qualify_contracts()

    def is_baseline_contract_cache_ready(self):
        required = {
            symbol for symbol in QUALIFIED_FUTURE_SYMBOLS
            if symbol not in RUNTIME_DISABLED_SYMBOLS
        }
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
                    f"reason={reason_label} | socket_connected={self.session_socket_connected} | "
                    f"session_healthy={self.session_healthy} session_initialized={self.session_initialized}"
                )
                return

            self.session_recovery_in_progress = True
            try:
                if self.broker_read_is_connected(
                    caller="reconnect_recovery",
                    reason_label=f"{reason_label}:forced_recovery_socket_check",
                    failure_log_level="info",
                ):
                    self.attach_ib_events(force_reset=True)
                    self.ensure_order_id_initialized()
                    self.ensure_contract_cache_initialized()
                    self.update_session_health()

                    postfailure = []
                    if not self.broker_read_is_connected(
                        caller="reconnect_recovery",
                        reason_label=f"{reason_label}:forced_recovery_postcheck",
                        failure_log_level="info",
                    ):
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
                    self.post_reconnect_fill_reconstruction_sweep(
                        f"forced_session_recovery_complete_{reason_label}",
                        since_time=self.get_reconnect_fill_reconstruction_since(),
                    )
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

            socket_connected = self.broker_read_is_connected(
                caller="connect_ib",
                reason_label="connect_socket_precheck",
                failure_log_level="info",
            )

            if not socket_connected:
                logger.info("CONNECTING TO IBKR")
                self.log_session_health("CONNECT START (SOCKET_CONNECT_REQUIRED)")

                try:
                    self.broker_write_connect(
                        self.IB_HOST,
                        self.IB_PORT,
                        self.IB_CLIENT_ID,
                        caller="connect_ib",
                        reason_label="socket_connect_required",
                    )
                except Exception:
                    logger.exception("CONNECTION FAILED")
                    self.update_session_health()
                    self.log_session_health("CONNECT FAILED")
                    raise

                if not self.broker_read_is_connected(
                    caller="connect_ib",
                    reason_label="connect_socket_postcheck",
                    failure_log_level="info",
                ):
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
                try:
                    self.qualify_contracts()
                except Exception as exc:
                    self.mark_session_initialization_failed(str(exc))
                    self.update_session_health()
                    self.log_session_health("CONNECT FAILED CONTRACT QUALIFICATION")
                    raise
            else:
                logger.info(f"CONTRACT CACHE BASELINE READY | size={len(self.contract_cache)}")

            self.clear_session_initialization_failure("connect_ib_contracts_ready")
            self.update_session_health()
            self.log_session_health("CONNECT COMPLETE")

            if not self.session_healthy:
                reason = (
                    self.session_initialization_failure_reason
                    or "IBKR session initialization incomplete after connect_ib"
                )
                self.mark_session_initialization_failed(reason)
                raise RuntimeError(reason)

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
            details = self.broker_write_req_contract_details(
                base,
                caller="ensure_symbol_contract_ready",
                reason_label="contract_cache_recovery",
                symbol=symbol,
            )
            detail = self.select_front_month_detail(
                details,
                requested_symbol=symbol,
                requested_spec=spec,
                requested_contract=base,
            )
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
                self.next_order_id = self.broker_get_req_id(
                    caller="place_bracket_order",
                    reason_label="allocate_bracket_order_ids",
                )

            parent_id = self.next_order_id
            tp_id = parent_id + 1
            sl_id = parent_id + 2
            self.next_order_id += 3

        return parent_id, tp_id, sl_id

    def allocate_exit_order_ids(self):
        with self.order_id_lock:
            if self.next_order_id is None:
                self.next_order_id = self.broker_get_req_id(
                    caller="place_timeout_retained_replacement_protection",
                    reason_label="allocate_exit_order_ids",
                )

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
            "broker_reconciled_flat",
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

    def log_exit_cleanup_visibility(self, trade_id, record, trigger_label, trade_analysis_lock_context="maybe_locked"):
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
            open_trades, open_orders = self.broker_read_open_trades_open_orders(
                caller="log_exit_cleanup_visibility",
                reason_label=trigger_label,
                trade_id=trade_id,
                symbol=record.get("symbol") if isinstance(record, dict) else None,
                trade_analysis_lock_context=trade_analysis_lock_context,
            )
            for trade in open_trades:
                order = getattr(trade, "order", None)
                status = getattr(trade, "orderStatus", None)
                order_id = getattr(order, "orderId", None)
                for leg, expected_id in order_ids.items():
                    if expected_id is not None and order_id == expected_id:
                        visibility[f"{leg}_in_open_trades"] = True
                        status_by_leg[leg] = getattr(status, "status", None)
                        quantity_by_leg[leg] = getattr(order, "totalQuantity", None)

            for order in open_orders:
                order_id = getattr(order, "orderId", None)
                for leg, expected_id in order_ids.items():
                    if expected_id is not None and order_id == expected_id:
                        visibility[f"{leg}_in_open_orders"] = True
                        if quantity_by_leg[leg] is None:
                            quantity_by_leg[leg] = getattr(order, "totalQuantity", None)
        except Exception as exc:
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
            open_trades, open_orders = self.broker_read_open_trades_open_orders(
                caller="get_timeout_retained_child_snapshot",
                reason_label="timeout_retained_child_snapshot",
                trade_id=record.get("trade_id") if isinstance(record, dict) else None,
                symbol=record.get("symbol") if isinstance(record, dict) else None,
                trade_analysis_lock_context="not_locked",
            )
            for trade in open_trades:
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

            for order in open_orders:
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
                    open_trades, open_orders = self.broker_read_open_trades_open_orders(
                        caller="cancel_timeout_retained_child_orders",
                        reason_label=f"timeout_retained_child_cancel:{leg_name}",
                        trade_id=trade_id,
                        symbol=record.get("symbol") if isinstance(record, dict) else None,
                        trade_analysis_lock_context="not_locked",
                    )
                    for trade in open_trades:
                        order = getattr(trade, "order", None)
                        if getattr(order, "orderId", None) == order_id:
                            cancel_target = order
                            cancel_reason = "cancelled_via_open_trade"
                            break

                    if cancel_target is None:
                        for order in open_orders:
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
                        self.broker_write_cancel_order(
                            cancel_target,
                            caller="cancel_timeout_retained_child_orders",
                            reason_label="timeout_retained_child_cancel",
                            trade_id=trade_id,
                            symbol=record.get("symbol") if isinstance(record, dict) else None,
                            real_broker_exposure=True,
                        )
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
        order_tif = self.get_order_tif(symbol, "timeout_retained_replacement")

        if not order_tif:
            logger.error(
                "ORDER_TIF_RESOLUTION_FAILED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                "order_role=timeout_retained_replacement "
                "decision=skip_replacement_submit"
            )
            self.append_trade_event(
                trade_id,
                "TIMEOUT RETAINED REPLACEMENT SKIPPED reason=order_tif_resolution_failed"
            )
            return {
                "ok": False,
                "reason": "order_tif_resolution_failed",
            }

        tp = LimitOrder(child_action, retained_quantity, target_price)
        tp.orderId = tp_id
        tp.transmit = False
        tp.tif = order_tif
        tp.ocaGroup = oca_group
        tp.ocaType = 1
        tp.orderRef = self.build_order_ref(trade_id, symbol, "TP")

        sl = StopOrder(child_action, retained_quantity, stop_price)
        sl.orderId = sl_id
        sl.transmit = True
        sl.tif = order_tif
        sl.ocaGroup = oca_group
        sl.ocaType = 1
        sl.orderRef = self.build_order_ref(trade_id, symbol, "SL")

        with self.trade_analysis_lock:
            live_record = self.trade_analysis.get(trade_id)
            if live_record is not None:
                live_record["order_ref_tp"] = tp.orderRef
                live_record["order_ref_sl"] = sl.orderRef

        for role, order in (("TP", tp), ("SL", sl)):
            logger.info(
                "ORDER_REF_ASSIGNED | "
                f"run_id={self.run_id} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"role={role} "
                f"order_id={getattr(order, 'orderId', None)} "
                f"orderRef={getattr(order, 'orderRef', None)} "
                "context=timeout_retained_replacement"
            )

        self.append_trade_event(
            trade_id,
            f"TIMEOUT RETAINED REPLACEMENT TP SUBMIT REQUESTED order_id={tp_id} quantity={retained_quantity} "
            f"price={target_price} tif={tp.tif} ocaGroup={tp.ocaGroup} ocaType={tp.ocaType}"
        )
        self.append_trade_event(
            trade_id,
            f"TIMEOUT RETAINED REPLACEMENT SL SUBMIT REQUESTED order_id={sl_id} quantity={retained_quantity} "
            f"price={stop_price} tif={sl.tif} ocaGroup={sl.ocaGroup} ocaType={sl.ocaType}"
        )
        logger.warning(
            "TIMEOUT RETAINED REPLACEMENT TP SUBMIT REQUESTED | "
            f"trade_id={trade_id} symbol={symbol} order_id={tp_id} "
            f"quantity={retained_quantity} target_price={target_price} "
            f"tif={getattr(tp, 'tif', None)} "
            f"orderRef={getattr(tp, 'orderRef', None)} "
            f"ocaGroup={getattr(tp, 'ocaGroup', None)} "
            f"ocaType={getattr(tp, 'ocaType', None)}"
        )
        logger.warning(
            "TIMEOUT RETAINED REPLACEMENT SL SUBMIT REQUESTED | "
            f"trade_id={trade_id} symbol={symbol} order_id={sl_id} "
            f"quantity={retained_quantity} stop_price={stop_price} "
            f"tif={getattr(sl, 'tif', None)} "
            f"orderRef={getattr(sl, 'orderRef', None)} "
            f"ocaGroup={getattr(sl, 'ocaGroup', None)} "
            f"ocaType={getattr(sl, 'ocaType', None)}"
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
            tp_trade = self.broker_write_place_order(
                contract,
                tp,
                caller="place_timeout_retained_replacement_protection",
                reason_label="timeout_retained_replacement_tp_submit",
                trade_id=trade_id,
                symbol=symbol,
            )
            sl_trade = self.broker_write_place_order(
                contract,
                sl,
                caller="place_timeout_retained_replacement_protection",
                reason_label="timeout_retained_replacement_sl_submit",
                trade_id=trade_id,
                symbol=symbol,
            )
            self.log_trade_snapshot("TIMEOUT RETAINED REPLACEMENT TP", tp_trade)
            self.log_trade_snapshot("TIMEOUT RETAINED REPLACEMENT SL", sl_trade)
            self.broker_write_sleep(
                0.20,
                caller="place_timeout_retained_replacement_protection",
                reason_label="timeout_retained_replacement_post_submit_wait",
                trade_id=trade_id,
                symbol=symbol,
            )
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
        self.broker_write_sleep(
            0.20,
            caller="reconcile_timeout_retained_child_protection",
            reason_label="timeout_retained_child_post_cancel_wait",
            trade_id=trade_id,
            symbol=record_snapshot.get("symbol"),
        )
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

    def assess_broker_bracket_confirmation(self, parent_id, tp_id, sl_id, trade_analysis_lock_context="maybe_locked"):
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
                "totalQuantity": None,
                "filled": None,
                "remaining": None,
                "action": None,
                "orderType": None,
                "whyHeld": None,
                "orderRef": None,
                "visible_source": None,
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
            open_trades, open_orders = self.broker_read_open_trades_open_orders(
                caller="assess_broker_bracket_confirmation",
                reason_label="bracket_confirmation",
                trade_id=None,
                symbol=None,
                trade_analysis_lock_context=trade_analysis_lock_context,
            )
            for trade in open_trades:
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
                        leg_data["totalQuantity"] = self.normalize_broker_quantity_or_none(
                            getattr(order, "totalQuantity", None)
                        )
                        leg_data["filled"] = self.normalize_broker_quantity_or_none(
                            getattr(status, "filled", None)
                        )
                        leg_data["remaining"] = self.normalize_broker_quantity_or_none(
                            getattr(status, "remaining", None)
                        )
                        leg_data["action"] = getattr(order, "action", None)
                        leg_data["orderType"] = getattr(order, "orderType", None)
                        leg_data["whyHeld"] = getattr(status, "whyHeld", None)
                        leg_data["orderRef"] = self.get_order_ref(order=order)
                        leg_data["visible_source"] = "open_trades"
                        break

            for order in open_orders:
                order_id = getattr(order, "orderId", None)
                if order_id not in expected_ids:
                    continue
                for leg_name, leg_data in broker_orders.items():
                    if leg_data["orderId"] == order_id:
                        leg_data["visible_in_open_orders"] = True
                        leg_data["visible_source"] = (
                            "open_trades+open_orders"
                            if leg_data.get("visible_source") == "open_trades"
                            else "open_orders"
                        )
                        if leg_data["parentId"] is None:
                            leg_data["parentId"] = getattr(order, "parentId", None)
                        if leg_data["permId"] in (None, 0):
                            leg_data["permId"] = getattr(order, "permId", None)
                        if leg_data["totalQuantity"] is None:
                            leg_data["totalQuantity"] = self.normalize_broker_quantity_or_none(
                                getattr(order, "totalQuantity", None)
                            )
                        if leg_data["action"] is None:
                            leg_data["action"] = getattr(order, "action", None)
                        if leg_data["orderType"] is None:
                            leg_data["orderType"] = getattr(order, "orderType", None)
                        if leg_data["orderRef"] is None:
                            leg_data["orderRef"] = self.get_order_ref(order=order)
                        break
        except Exception as exc:
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
                "order_id": leg_data["orderId"],
                "parentId": leg_data["parentId"],
                "parent_id": leg_data["parentId"],
                "expected_parent_id": leg_data["expected_parent_id"],
                "status": leg_data["status"],
                "permId": leg_data["permId"],
                "perm_id": leg_data["permId"],
                "totalQuantity": leg_data["totalQuantity"],
                "filled": leg_data["filled"],
                "remaining": leg_data["remaining"],
                "action": leg_data["action"],
                "orderType": leg_data["orderType"],
                "whyHeld": leg_data["whyHeld"],
                "orderRef": leg_data["orderRef"],
                "visible_source": leg_data["visible_source"],
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
            f"BRACKET VALIDATION SUMMARY | {json.dumps({'outcome': outcome, 'reason': reason, 'broker_state_category': broker_state_category, 'any_visible': any_visible, 'all_visible': all_visible, 'all_links_ok': all_links_ok, 'all_broker_acknowledged': all_broker_acknowledged, 'all_broker_live': all_broker_live, 'any_terminal_status': any_terminal_status, 'pending_broker_ack': pending_broker_ack, 'ambiguous_broker_state': ambiguous_broker_state, 'broken_or_terminal_state': broken_or_terminal_state, 'quantity_state': None, 'quantity_coverage_ok': None, 'open_position_estimate': None, 'protective_sl_coverage': None, 'planned_parent_quantity': None, 'planned_tp_quantity': None, 'planned_sl_quantity': None, 'visible_order_ids': sorted(leg_data['orderId'] for leg_data in broker_orders.values() if leg_data['submitted_to_ib']), 'leg_validation': leg_validation}, sort_keys=True)}"
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

    def parse_contract_expiry_key(self, expiry):
        expiry_text = str(expiry or "").strip()
        if not expiry_text.isdigit():
            return None, "expiry_missing_or_non_numeric"

        if len(expiry_text) == 6:
            try:
                year = int(expiry_text[:4])
                month = int(expiry_text[4:6])
                if year < 2000 or month < 1 or month > 12:
                    return None, "expiry_yyyymm_out_of_range"
                return (year, month, 0), None
            except Exception:
                return None, "expiry_yyyymm_parse_failed"

        if len(expiry_text) == 8:
            try:
                expiry_date = datetime.strptime(expiry_text, "%Y%m%d").date()
            except Exception:
                return None, "expiry_yyyymmdd_parse_failed"

            if expiry_date < datetime.now(timezone.utc).date():
                return None, "expiry_yyyymmdd_expired"
            return (expiry_date.year, expiry_date.month, expiry_date.day), None

        return None, "expiry_length_unsupported"

    def normalize_contract_metadata_text(self, value):
        if value is None:
            return None
        text = str(value).strip().upper()
        return text or None

    def get_contract_detail_valid_exchanges(self, detail):
        valid_exchanges = getattr(detail, "validExchanges", None)
        if not valid_exchanges:
            return []
        if isinstance(valid_exchanges, str):
            return [
                exchange.strip()
                for exchange in valid_exchanges.split(",")
                if exchange.strip()
            ]
        try:
            return [
                str(exchange).strip()
                for exchange in valid_exchanges
                if str(exchange).strip()
            ]
        except Exception:
            return [str(valid_exchanges).strip()]

    def resolve_contract_detail_exchange_match(self, detail, contract, requested_exchange):
        requested_exchange_text = self.normalize_contract_metadata_text(requested_exchange)
        if not requested_exchange_text:
            return False, None, []

        exchange_sources = [
            ("exchange", getattr(contract, "exchange", None)),
            ("primaryExchange", getattr(contract, "primaryExchange", None)),
        ]
        for valid_exchange in self.get_contract_detail_valid_exchanges(detail):
            exchange_sources.append(("validExchanges", valid_exchange))

        observed_values = []
        for source, value in exchange_sources:
            normalized_value = self.normalize_contract_metadata_text(value)
            if not normalized_value:
                continue
            observed_values.append(f"{source}:{normalized_value}")
            if normalized_value == requested_exchange_text:
                return True, source, observed_values

        return False, None, observed_values

    def build_contract_selection_evidence(
        self,
        detail,
        requested_symbol,
        requested_spec=None,
        matched_exchange_source=None,
        selection_reason=None,
        skipped_reason=None,
    ):
        contract = getattr(detail, "contract", None) if detail is not None else None
        requested_exchange = (requested_spec or {}).get("exchange")
        exchange_match, resolved_exchange_source, exchange_values = (
            self.resolve_contract_detail_exchange_match(
                detail,
                contract,
                requested_exchange,
            )
            if contract is not None
            else (False, None, [])
        )
        return {
            "requested_symbol": requested_symbol,
            "metadata_symbol": getattr(contract, "symbol", None),
            "localSymbol": getattr(contract, "localSymbol", None),
            "tradingClass": getattr(contract, "tradingClass", None),
            "secType": getattr(contract, "secType", None),
            "exchange": getattr(contract, "exchange", None),
            "primaryExchange": getattr(contract, "primaryExchange", None),
            "validExchanges": self.get_contract_detail_valid_exchanges(detail),
            "matched_exchange_source": matched_exchange_source or resolved_exchange_source,
            "exchange_match": exchange_match,
            "exchange_values": exchange_values,
            "currency": getattr(contract, "currency", None),
            "conId": getattr(contract, "conId", None),
            "expiry": getattr(contract, "lastTradeDateOrContractMonth", None),
            "selection_reason": selection_reason,
            "skipped_reason": skipped_reason,
        }

    def validate_fdxm_contract_detail_identity(self, detail, contract, requested_spec):
        requested_exchange = (requested_spec or {}).get("exchange")
        exchange_match, matched_exchange_source, exchange_values = (
            self.resolve_contract_detail_exchange_match(
                detail,
                contract,
                requested_exchange,
            )
        )
        failures = []

        if self.normalize_contract_metadata_text(getattr(contract, "secType", None)) != "FUT":
            failures.append("sec_type_mismatch")
        if self.normalize_contract_metadata_text(getattr(contract, "tradingClass", None)) != "FDXM":
            failures.append("trading_class_mismatch")
        if self.normalize_contract_metadata_text(getattr(contract, "currency", None)) != "EUR":
            failures.append("currency_mismatch")
        if getattr(contract, "conId", None) in (None, 0):
            failures.append("con_id_missing")
        if not getattr(contract, "localSymbol", None):
            failures.append("local_symbol_missing")
        if not exchange_match:
            failures.append("exchange_mismatch")

        return {
            "ok": not failures,
            "failures": failures,
            "matched_exchange_source": matched_exchange_source,
            "exchange_values": exchange_values,
        }

    def score_contract_detail_match(self, contract, requested_symbol=None, requested_spec=None, requested_contract=None):
        score = 0
        mismatch_reasons = []

        requested_symbol_value = requested_symbol
        requested_exchange = None
        requested_currency = None
        requested_trading_class = None
        if requested_spec:
            requested_symbol_value = requested_symbol_value or requested_spec.get("symbol")
            requested_exchange = requested_spec.get("exchange")
            requested_currency = requested_spec.get("currency")
            requested_trading_class = requested_spec.get("trading_class")
        if requested_contract is not None:
            requested_symbol_value = requested_symbol_value or getattr(requested_contract, "symbol", None)
            requested_exchange = requested_exchange or getattr(requested_contract, "exchange", None)
            requested_currency = requested_currency or getattr(requested_contract, "currency", None)
            requested_trading_class = requested_trading_class or getattr(requested_contract, "tradingClass", None)

        contract_symbol = getattr(contract, "symbol", None)
        contract_exchange = getattr(contract, "exchange", None)
        contract_primary_exchange = getattr(contract, "primaryExchange", None)
        contract_currency = getattr(contract, "currency", None)
        contract_trading_class = getattr(contract, "tradingClass", None)

        if requested_symbol_value and contract_symbol:
            if str(contract_symbol).upper() == str(requested_symbol_value).upper():
                score += 4
            else:
                mismatch_reasons.append("symbol_mismatch")

        if requested_currency and contract_currency:
            if str(contract_currency).upper() == str(requested_currency).upper():
                score += 3
            else:
                mismatch_reasons.append("currency_mismatch")

        if requested_trading_class and contract_trading_class:
            if str(contract_trading_class).upper() == str(requested_trading_class).upper():
                score += 3
            else:
                mismatch_reasons.append("trading_class_mismatch")

        if requested_exchange:
            requested_exchange_text = str(requested_exchange).upper()
            exchange_values = {
                str(value).upper()
                for value in (contract_exchange, contract_primary_exchange)
                if value
            }
            if requested_exchange_text in exchange_values:
                score += 2
            elif exchange_values:
                score -= 1

        return score, mismatch_reasons

    def select_front_month_detail(self, details, requested_symbol=None, requested_spec=None, requested_contract=None):
        raw_count = len(details or [])
        candidates = []
        skipped_reasons = {}
        requested_symbol_text = self.normalize_contract_metadata_text(requested_symbol)
        fdxm_identity_mode = requested_symbol_text == "FDXM"

        for detail in details or []:
            contract = getattr(detail, "contract", None)
            if contract is None:
                skipped_reasons["missing_contract"] = skipped_reasons.get("missing_contract", 0) + 1
                continue

            expiry = getattr(contract, "lastTradeDateOrContractMonth", None)
            expiry_key, expiry_skip_reason = self.parse_contract_expiry_key(expiry)
            if expiry_skip_reason is not None:
                skipped_reasons[expiry_skip_reason] = skipped_reasons.get(expiry_skip_reason, 0) + 1
                continue

            fdxm_identity = None
            if fdxm_identity_mode:
                fdxm_identity = self.validate_fdxm_contract_detail_identity(
                    detail,
                    contract,
                    requested_spec,
                )
                if not fdxm_identity["ok"]:
                    reason_key = "metadata_" + "_and_".join(fdxm_identity["failures"])
                    skipped_reasons[reason_key] = skipped_reasons.get(reason_key, 0) + 1
                    logger.info(
                        "CONTRACT_SELECTION_DETAIL_SKIPPED | "
                        f"{json.dumps(self.build_contract_selection_evidence(detail, requested_symbol, requested_spec, skipped_reason=reason_key), sort_keys=True)}"
                    )
                    continue

            match_score, mismatch_reasons = self.score_contract_detail_match(
                contract,
                requested_symbol=requested_symbol,
                requested_spec=requested_spec,
                requested_contract=requested_contract,
            )
            if fdxm_identity_mode and "symbol_mismatch" in mismatch_reasons:
                mismatch_reasons = [
                    reason for reason in mismatch_reasons
                    if reason != "symbol_mismatch"
                ]
                match_score += 1
            if mismatch_reasons:
                reason_key = "metadata_" + "_and_".join(mismatch_reasons)
                skipped_reasons[reason_key] = skipped_reasons.get(reason_key, 0) + 1
                logger.info(
                    "CONTRACT_SELECTION_DETAIL_SKIPPED | "
                    f"{json.dumps(self.build_contract_selection_evidence(detail, requested_symbol, requested_spec, skipped_reason=reason_key), sort_keys=True)}"
                )
                continue

            candidates.append({
                "detail": detail,
                "expiry_key": expiry_key,
                "match_score": match_score,
                "matched_exchange_source": (
                    fdxm_identity.get("matched_exchange_source")
                    if fdxm_identity is not None
                    else None
                ),
            })

        logger.info(
            "CONTRACT_SELECTION_CANDIDATES | "
            f"requested_symbol={requested_symbol} "
            f"raw_detail_count={raw_count} "
            f"valid_candidate_count={len(candidates)} "
            f"skipped_count={raw_count - len(candidates)} "
            f"skipped_reasons={json.dumps(skipped_reasons, sort_keys=True)}"
        )

        if not candidates:
            raise RuntimeError(
                f"No valid front-month contract detail found for {requested_symbol}: "
                f"{json.dumps(skipped_reasons, sort_keys=True)}"
            )

        selected = sorted(
            candidates,
            key=lambda item: (item["expiry_key"], -item["match_score"])
        )[0]
        selected_detail = selected["detail"]
        selected_contract = selected_detail.contract

        logger.info(
            "CONTRACT_SELECTION_SELECTED | "
            f"requested_symbol={requested_symbol} "
            f"selected_symbol={getattr(selected_contract, 'symbol', None)} "
            f"selected_localSymbol={getattr(selected_contract, 'localSymbol', None)} "
            f"selected_tradingClass={getattr(selected_contract, 'tradingClass', None)} "
            f"selected_exchange={getattr(selected_contract, 'exchange', None)} "
            f"selected_primaryExchange={getattr(selected_contract, 'primaryExchange', None)} "
            f"selected_validExchanges={self.get_contract_detail_valid_exchanges(selected_detail)} "
            f"matched_exchange_source={selected.get('matched_exchange_source')} "
            f"selected_currency={getattr(selected_contract, 'currency', None)} "
            f"selected_expiry={getattr(selected_contract, 'lastTradeDateOrContractMonth', None)} "
            f"selected_conId={getattr(selected_contract, 'conId', None)} "
            f"match_score={selected['match_score']} "
            "selection_reason=front_month_lowest_expiry_highest_score"
        )

        return selected_detail

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
            "risk_intent_profile": formal_contract["risk_intent_profile"],
            "risk_intent_percent": formal_contract["risk_intent_percent"],
            "risk_intent_status": formal_contract["risk_intent_status"],
            "risk_intent_reason": formal_contract["risk_intent_reason"],
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
            "theoretical_size_before_containment": None,
            "approved_size_after_containment": None,
            "containment_status": "prepared_not_active",
            "containment_action": "approve_as_is",
            "containment_reason": "containment_preparation_only",
            "containment_hierarchy": ">".join(CONTAINMENT_ACTION_HIERARCHY),
            "containment_decision_source": "none",
            "containment_denial_source": "none",
            "triggering_containment_rule": "none",
            "containment_max_size": None,
            "containment_max_notional": None,
            "notional_model": None,
            "notional_currency": None,
            "capped_size_after_size_containment": None,
            "estimated_notional_before_containment": None,
            "estimated_notional_after_size_containment": None,
            "estimated_notional_after_containment": None,
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
        webhook_timing_start_ms = self.current_monotonic_ms()
        normalized = None
        webhook_received_time = datetime.now(timezone.utc)
        logger.info(
            "WEBHOOK RECEIVED | "
            f"{json.dumps(build_sanitized_payload_summary(data), sort_keys=True)}"
        )

        if not isinstance(data, dict):
            logger.error(f"INVALID PAYLOAD TYPE: {type(data)}")
            return self.log_webhook_processing_timing(
                "invalid_payload_type",
                webhook_timing_start_ms,
                normalized,
                queued=False,
            )

        normalized = self.normalize_signal(data)
        self.log_normalized_signal(normalized)

        if self.is_runtime_symbol_disabled(normalized["symbol"]):
            logger.warning(
                "SYMBOL RUNTIME DISABLED | "
                f"symbol={normalized['symbol']} "
                f"payload_format={normalized['payload_format']} "
                f"signal_id={normalized.get('signal_id')} "
                "reason=fdxm_disabled_due_to_margin_exposure"
            )
            return self.log_webhook_processing_timing(
                "symbol_runtime_disabled",
                webhook_timing_start_ms,
                normalized,
                queued=False,
            )

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
            return self.log_webhook_processing_timing(
                "diagnostic_classified_no_execution",
                webhook_timing_start_ms,
                normalized,
                queued=False,
            )

        if not normalized["symbol"] or not normalized["side"]:
            logger.error(
                "INVALID SIGNAL PAYLOAD | "
                f"missing_canonical_fields symbol={normalized['symbol']} side={normalized['side']} "
                f"payload_format={normalized['payload_format']}"
            )
            return self.log_webhook_processing_timing(
                "invalid_signal_payload",
                webhook_timing_start_ms,
                normalized,
                queued=False,
            )

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
                return self.log_webhook_processing_timing(
                    routing_block_status,
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

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
                return self.log_webhook_processing_timing(
                    "enriched_candidate_classified_no_execution",
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

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
                return self.log_webhook_processing_timing(
                    "enriched_candidate_classified_no_execution",
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

            if self.is_eurusd_execution_disabled(normalized["symbol"]):
                self.log_eurusd_execution_disabled(
                    normalized["symbol"],
                    payload_format=normalized["payload_format"],
                    signal_id=normalized["signal_id"],
                    location="handle_webhook_signal_after_classification_before_queue",
                )
                logger.info(
                    "QUEUE DECISION | "
                    f"path=det_gated_execution "
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
                    "queued=false "
                    "blocked_by=unsupported_spot_fx_sizing "
                    f"reason={EURUSD_EXECUTION_DISABLED_REASON}"
                )
                return self.log_webhook_processing_timing(
                    EURUSD_EXECUTION_DISABLED_REASON,
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

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
                return self.log_webhook_processing_timing(
                    "enriched_candidate_missing_price",
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

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
                return self.log_webhook_processing_timing(
                    "enriched_candidate_entry_derivation_failed",
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

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
                return self.log_webhook_processing_timing(
                    "enriched_candidate_entry_band_rejected",
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

            now = time.time()
            key = f"{symbol}-{side}-{round(entry, 8)}"

            if key == self.last_signal and now - self.last_signal_time < 5:
                logger.info("Duplicate ignored")
                return self.log_webhook_processing_timing(
                    "duplicate_ignored",
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

            self.last_signal = key
            self.last_signal_time = now

            job = self.build_det_execution_job(normalized, formal_contract, gate_result)
            job["webhook_received_time"] = webhook_received_time
            job["classification_completed_time"] = classification_completed_time

            external_entry_blocked, block_source, block_reason = self.should_block_external_entry()
            if external_entry_blocked:
                self.log_external_entry_blocked(
                    job=job,
                    normalized=normalized,
                    location="handle_webhook_signal_before_queue",
                    block_source=block_source,
                    reason=block_reason,
                )
                logger.info(
                    "QUEUE DECISION | "
                    f"path=external_entry_gate "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"queued=false "
                    f"blocked_by={block_source} "
                    f"reason={block_reason}"
                )
                return self.log_webhook_processing_timing(
                    block_reason or "external_entry_blocked",
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

            session_entry_blocked, session_close_state = self.should_block_new_entry_for_session_close(symbol)
            if session_entry_blocked:
                self.log_session_entry_blocked(
                    symbol,
                    side=side,
                    job=job,
                    normalized=normalized,
                    location="handle_webhook_signal_before_queue",
                    state=session_close_state,
                )
                logger.info(
                    "QUEUE DECISION | "
                    f"path=session_close_entry_cutoff "
                    f"payload_format={normalized['payload_format']} "
                    f"signal_id={normalized['signal_id']} "
                    f"symbol={normalized['symbol']} "
                    f"side={normalized['side']} "
                    f"truth_classification={formal_contract['truth_classification']} "
                    f"det_classification={formal_contract['det_classification']} "
                    f"execution_lane={job['execution_lane']} "
                    f"promoted_from_shadow={job['promoted_from_shadow']} "
                    f"queued=false "
                    "blocked_by=session_close_policy "
                    "reason=session_close_entry_cutoff"
                )
                self.enqueue_session_close_system_job(
                    "webhook_entry_cutoff",
                    symbol=symbol,
                )
                return self.log_webhook_processing_timing(
                    "session_close_entry_cutoff",
                    webhook_timing_start_ms,
                    normalized,
                    queued=False,
                )

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
            return self.log_webhook_processing_timing(
                "queued",
                webhook_timing_start_ms,
                normalized,
                queued=True,
            )

        if normalized["payload_format"] == "legacy_execution":
            logger.info(
                "LEGACY EXECUTION IGNORED | "
                f"payload_format={normalized['payload_format']} "
                f"signal_id={normalized['signal_id']} "
                f"symbol={normalized['symbol']} "
                f"side={normalized['side']} "
                "legacy_execution payloads are deprecated and denied for queueing"
            )
            return self.log_webhook_processing_timing(
                "legacy_execution_denied",
                webhook_timing_start_ms,
                normalized,
                queued=False,
            )

        if not normalized["execution_ready"]:
            logger.info(
                "CANDIDATE PAYLOAD LOGGED NO EXECUTION | "
                f"payload_format={normalized['payload_format']} "
                f"signal_id={normalized['signal_id']} "
                f"symbol={normalized['symbol']} "
                f"side={normalized['side']} "
                f"missing_execution_field=entry_price"
            )
            return self.log_webhook_processing_timing(
                "candidate_logged_no_execution",
                webhook_timing_start_ms,
                normalized,
                queued=False,
            )

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
            return self.log_webhook_processing_timing(
                "execution_denied_by_stage_policy",
                webhook_timing_start_ms,
                normalized,
                queued=False,
            )

        # Legacy execution queueing removed - legacy payloads are denied earlier
        return self.log_webhook_processing_timing(
            None,
            webhook_timing_start_ms,
            normalized,
            queued=False,
        )

    # ==========================================================
    # WORKER & EXECUTION PREFLIGHT
    # ==========================================================

    def execution_preflight_check(self, job):
        symbol = job["symbol"]
        reasons = []

        if self.is_connectivity_execution_blocked():
            return False

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
                if self.broker_read_is_connected(
                    caller="execution_worker",
                    reason_label="execution_preflight_recovery_socket_check",
                    failure_log_level="info",
                ):
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
        self.register_broker_io_owner_candidate("execution_worker")

        logger.info("EXECUTION WORKER STARTED")
        logger.info(f"BOT STAGE: {BOT_STAGE}")

        try:
            self.connect_ib()
            self.run_startup_reconciliation()
        except Exception as exc:
            if not self.session_initialization_failed:
                self.mark_session_initialization_failed(str(exc))
            logger.critical(
                "EXECUTION_WORKER_STARTUP_FAILED_FAIL_CLOSED | "
                f"reason={self.session_initialization_failure_reason} "
                "worker_alive=true "
                "execution_jobs_allowed=false"
            )

        while True:
            job = None
            try:
                if self.session_initialization_failed:
                    job = self.execution_queue.get(timeout=1.0)
                    job["worker_pickup_time"] = datetime.now(timezone.utc)
                    logger.critical(
                        "EXECUTION_WORKER_FAIL_CLOSED_JOB_SKIPPED | "
                        f"job_type={job.get('job_type')} "
                        f"symbol={job.get('symbol')} "
                        f"reason={self.session_initialization_failure_reason} "
                        "decision=no_execution_or_broker_recovery_while_startup_failure_unresolved"
                    )
                    continue

                with self.execution_lock:
                    try:
                        self.broker_write_sleep(
                            0.01,
                            caller="execution_worker",
                            reason_label="execution_worker_idle_event_pump",
                        )
                    except Exception as exc:
                        logger.warning(
                            "EXECUTION_WORKER_EVENT_PUMP_FAILED | "
                            f"reason=execution_worker_idle_event_pump failure={exc}"
                        )
                    self.reconcile_active_trade_lifecycle_from_broker_fills(
                        "execution_worker_periodic_sweep"
                    )
                    self.process_time_exit_deadlines()
                    self.process_partial_entry_timeouts()
                    self.process_partial_timeout_parent_finality()

                job = self.execution_queue.get(timeout=1.0)
                job["worker_pickup_time"] = datetime.now(timezone.utc)
                logger.info(
                    "EXECUTION WORKER QUEUE ITEM ACQUIRED | "
                    f"job_type={job.get('job_type')} "
                    f"symbol={job.get('symbol')} "
                    f"side={job.get('side')} "
                    f"grade={job.get('grade')} "
                    f"queue_size_after_get={self.execution_queue.qsize()}"
                )

                with self.execution_lock:
                    if self.is_broker_system_job(job):
                        self.process_broker_system_job(job)
                        continue

                    external_entry_blocked, block_source, block_reason = self.should_block_external_entry()
                    if external_entry_blocked:
                        self.log_external_entry_blocked(
                            job=job,
                            location="execution_worker_external_entry_gate",
                            block_source=block_source,
                            reason=block_reason,
                        )
                        logger.warning(
                            "EXECUTION WORKER EXECUTION SKIPPED | "
                            f"symbol={job.get('symbol')} "
                            f"reason={block_reason}"
                        )
                        continue

                    if self.is_runtime_symbol_disabled(job.get("symbol")):
                        logger.warning(
                            "EXECUTION BLOCKED SYMBOL RUNTIME DISABLED | "
                            f"symbol={job.get('symbol')} "
                            "reason=fdxm_disabled_due_to_margin_exposure"
                        )
                        logger.warning(
                            "EXECUTION WORKER EXECUTION SKIPPED | "
                            f"symbol={job.get('symbol')} "
                            "reason=symbol_runtime_disabled"
                        )
                        continue

                    if self.is_eurusd_execution_disabled(job.get("symbol")):
                        self.log_eurusd_execution_disabled(
                            job.get("symbol"),
                            payload_format=job.get("payload_format"),
                            signal_id=job.get("signal_id"),
                            location="execution_worker",
                        )
                        logger.warning(
                            "EXECUTION WORKER EXECUTION SKIPPED | "
                            f"symbol={job.get('symbol')} "
                            f"reason={EURUSD_EXECUTION_DISABLED_REASON}"
                        )
                        continue

                    session_entry_blocked, session_close_state = self.should_block_new_entry_for_session_close(job.get("symbol"))
                    if session_entry_blocked:
                        self.log_session_entry_blocked(
                            job.get("symbol"),
                            job=job,
                            location="execution_worker_before_preflight",
                            state=session_close_state,
                        )
                        self.run_session_close_sweep(
                            "execution_worker_entry_hard_gate",
                            target_symbol=job.get("symbol"),
                        )
                        logger.warning(
                            "EXECUTION WORKER EXECUTION SKIPPED | "
                            f"symbol={job.get('symbol')} "
                            "reason=session_close_entry_cutoff"
                        )
                        continue

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

                    paper_risk_regime = self.evaluate_paper_execution_risk_regime(job)
                    if not paper_risk_regime["execution_allowed"]:
                        logger.warning(
                            "EXECUTION RISK REGIME BLOCKED | "
                            f"stage={paper_risk_regime['stage']} "
                            f"risk_branch={paper_risk_regime['risk_branch']} "
                            f"incoming_symbol={job['symbol']} "
                            f"day_key={paper_risk_regime.get('day_key')} "
                            f"trigger_trade_id={paper_risk_regime.get('trigger_trade_id')} "
                            f"reason={paper_risk_regime['reason']}"
                        )
                        logger.warning(
                            "EXECUTION WORKER EXECUTION SKIPPED | "
                            f"symbol={job.get('symbol')} "
                            "reason=acc_symbol_daily_sl_stop"
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

                    logger.info(
                        "IB CONNECTED: "
                        f"{self.broker_read_is_connected(caller='execution_worker', reason_label='pre_order_connected_log', failure_log_level='info')}"
                    )
                    logger.info("STARTING ORDER EXECUTION")

                    self.place_bracket_order(job)

                    logger.info(
                        "EXECUTION WORKER EXECUTION COMPLETED | "
                        f"symbol={job.get('symbol')} "
                        f"side={job.get('side')}"
                    )

            except queue.Empty:
                logger.debug("EXECUTION WORKER QUEUE POLL | acquired_job=false")
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

        if self.is_runtime_symbol_disabled(symbol):
            logger.warning(
                "EXECUTION BLOCKED SYMBOL RUNTIME DISABLED | "
                f"symbol={symbol} "
                "reason=fdxm_disabled_due_to_margin_exposure "
                "location=place_bracket_order"
            )
            return

        if self.is_eurusd_execution_disabled(symbol):
            self.log_eurusd_execution_disabled(
                symbol,
                payload_format=job.get("payload_format"),
                signal_id=job.get("signal_id"),
                location="place_bracket_order",
            )
            return

        size_constraints = self.get_size_constraints(symbol)
        configured_max_size = size_constraints["max_size"]
        configured_containment_max_size = self.get_containment_max_size(symbol)
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
        job["contract_con_id"] = getattr(contract, "conId", None)
        job["contract_local_symbol"] = getattr(contract, "localSymbol", None)
        job["contract_trading_class"] = getattr(contract, "tradingClass", None)
        job["contract_exchange"] = getattr(contract, "exchange", None)
        job["contract_currency"] = getattr(contract, "currency", None)
        job["contract_sec_type"] = getattr(contract, "secType", None)

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
            elif configured_containment_max_size is None or configured_containment_max_size <= 0:
                containment_reason = "missing_or_invalid_configured_containment_max_size"
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
                f"configured_containment_max_size={configured_containment_max_size} "
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
                    f"configured_containment_max_size={configured_containment_max_size} "
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
            f"stop_distance_comparison_basis={stop_validation['stop_distance_comparison_basis']} "
            f"final_stop_distance_ticks={stop_validation['final_stop_distance_ticks']} "
            f"min_stop_distance_ticks={stop_validation['min_stop_distance_ticks']} "
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
                f"stop_distance_comparison_basis={stop_validation['stop_distance_comparison_basis']} "
                f"final_stop_distance_ticks={stop_validation['final_stop_distance_ticks']} "
                f"min_stop_distance_ticks={stop_validation['min_stop_distance_ticks']} "
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
            configured_containment_max_size is not None and
            normalized_size is not None and
            normalized_size > configured_containment_max_size
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
                f"configured_containment_max_size={configured_containment_max_size} "
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
                    f"configured_containment_max_size={configured_containment_max_size} "
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
        job["risk_intent_profile"] = job.get("risk_intent_profile", job.get("risk_profile"))
        job["risk_intent_percent"] = job.get("risk_intent_percent", sizing_result["intended_risk_percent"])
        job["risk_intent_status"] = job.get("risk_intent_status", "RISK_INTENT_DEFINED")
        job["risk_intent_reason"] = job.get("risk_intent_reason", "classification_mapped_to_risk_intent")
        containment_result = self.resolve_execution_containment(symbol, normalized_size, entry)
        job["theoretical_size_before_containment"] = containment_result["theoretical_size_before_containment"]
        job["containment_status"] = containment_result["containment_status"]
        job["containment_stage"] = containment_result["containment_stage"]
        job["containment_profile"] = containment_result["containment_profile"]
        job["containment_max_size"] = containment_result["containment_max_size"]
        job["containment_max_notional"] = containment_result["containment_max_notional"]
        job["containment_hierarchy"] = containment_result["containment_hierarchy"]
        job["containment_decision_source"] = containment_result["containment_decision_source"]
        job["containment_denial_source"] = containment_result["containment_denial_source"]
        job["triggering_containment_rule"] = containment_result["triggering_containment_rule"]
        job["notional_model"] = containment_result["notional_model"]
        job["notional_currency"] = containment_result["notional_currency"]
        job["capped_size_after_size_containment"] = containment_result["capped_size_after_size_containment"]
        job["estimated_notional_before_containment"] = containment_result["estimated_notional_before_containment"]
        job["estimated_notional_after_size_containment"] = containment_result["estimated_notional_after_size_containment"]
        job["estimated_notional_after_containment"] = containment_result["estimated_notional_after_containment"]
        job["containment_action"] = containment_result["containment_action"]
        job["containment_reason"] = containment_result["containment_reason"]
        job["approved_size_after_containment"] = containment_result["approved_size_after_containment"]
        approved_size_after_containment = job["approved_size_after_containment"]

        logger.info(
            "EXECUTION CANDIDATE DET OUTCOME | "
            f"symbol={symbol} "
            f"grade={job.get('grade', '')} "
            f"truth_classification={job.get('truth_classification', '')} "
            f"det_classification={job.get('det_classification', '')} "
            f"execution_lane={job.get('execution_lane', 'standard')} "
            f"promoted_from_shadow={job.get('promoted_from_shadow', False)} "
            f"shadow_override_reason={job.get('shadow_override_reason', '')}"
        )
        logger.info(
            "EXECUTION CANDIDATE RISK INTENT | "
            f"symbol={symbol} "
            f"risk_intent_profile={job.get('risk_intent_profile')} "
            f"risk_intent_percent={job.get('risk_intent_percent')} "
            f"risk_intent_status={job.get('risk_intent_status')} "
            f"risk_intent_reason={job.get('risk_intent_reason')} "
            f"allowed_money_risk={job.get('allowed_money_risk')} "
            f"stop_distance_points={job.get('stop_distance_points')} "
            f"raw_position_size={job.get('raw_position_size')} "
            f"normalized_position_size={job.get('normalized_position_size')}"
        )
        logger.info(
            "EXECUTION CANDIDATE CONTAINMENT | "
            f"symbol={symbol} "
            f"containment_status={job.get('containment_status')} "
            f"containment_stage={job.get('containment_stage')} "
            f"containment_profile={job.get('containment_profile')} "
            f"containment_action={job.get('containment_action')} "
            f"containment_reason={job.get('containment_reason')} "
            f"containment_hierarchy={job.get('containment_hierarchy')} "
            f"containment_decision_source={job.get('containment_decision_source')} "
            f"containment_denial_source={job.get('containment_denial_source')} "
            f"triggering_containment_rule={job.get('triggering_containment_rule')} "
            f"notional_model={job.get('notional_model')} "
            f"notional_currency={job.get('notional_currency')} "
            f"containment_max_size={job.get('containment_max_size')} "
            f"containment_max_notional={job.get('containment_max_notional')} "
            f"theoretical_size_before_containment={job.get('theoretical_size_before_containment')} "
            f"capped_size_after_size_containment={job.get('capped_size_after_size_containment')} "
            f"estimated_notional_before_containment={job.get('estimated_notional_before_containment')} "
            f"estimated_notional_after_size_containment={job.get('estimated_notional_after_size_containment')} "
            f"estimated_notional_after_containment={job.get('estimated_notional_after_containment')} "
            f"approved_size_after_containment={job.get('approved_size_after_containment')}"
        )

        if job.get("containment_action") == "denied":
            if job.get("containment_denial_source") not in CONTAINMENT_DECISION_SOURCES or job.get("containment_denial_source") == "none":
                job["containment_reason"] = (
                    f"final_containment_coherence_failed:invalid_denial_source:"
                    f"{job.get('containment_reason')}"
                )
                job["containment_decision_source"] = "stage_containment"
                job["containment_denial_source"] = "stage_containment"
                job["approved_size_after_containment"] = None
            logger.error(
                "EXECUTION CONTAINMENT DENIED | "
                f"symbol={symbol} "
                f"side={side} "
                f"containment_status={job.get('containment_status')} "
                f"containment_stage={job.get('containment_stage')} "
                f"containment_profile={job.get('containment_profile')} "
                f"containment_action={job.get('containment_action')} "
                f"containment_reason={job.get('containment_reason')} "
                f"containment_hierarchy={job.get('containment_hierarchy')} "
                f"containment_decision_source={job.get('containment_decision_source')} "
                f"containment_denial_source={job.get('containment_denial_source')} "
                f"triggering_containment_rule={job.get('triggering_containment_rule')} "
                f"notional_model={job.get('notional_model')} "
                f"notional_currency={job.get('notional_currency')} "
                f"containment_max_size={job.get('containment_max_size')} "
                f"containment_max_notional={job.get('containment_max_notional')} "
                f"theoretical_size_before_containment={job.get('theoretical_size_before_containment')} "
                f"capped_size_after_size_containment={job.get('capped_size_after_size_containment')} "
                f"estimated_notional_before_containment={job.get('estimated_notional_before_containment')} "
                f"estimated_notional_after_size_containment={job.get('estimated_notional_after_size_containment')} "
                f"estimated_notional_after_containment={job.get('estimated_notional_after_containment')} "
                f"approved_size_after_containment={job.get('approved_size_after_containment')}"
            )
            return

        if job.get("containment_action") == "capped":
            logger.warning(
                "EXECUTION CONTAINMENT CAPPED | "
                f"symbol={symbol} "
                f"side={side} "
                f"containment_stage={job.get('containment_stage')} "
                f"containment_profile={job.get('containment_profile')} "
                f"theoretical_size_before_containment={job.get('theoretical_size_before_containment')} "
                f"capped_size_after_size_containment={job.get('capped_size_after_size_containment')} "
                f"approved_size_after_containment={job.get('approved_size_after_containment')} "
                f"containment_max_size={job.get('containment_max_size')} "
                f"containment_max_notional={job.get('containment_max_notional')} "
                f"notional_model={job.get('notional_model')} "
                f"notional_currency={job.get('notional_currency')} "
                f"estimated_notional_before_containment={job.get('estimated_notional_before_containment')} "
                f"estimated_notional_after_size_containment={job.get('estimated_notional_after_size_containment')} "
                f"estimated_notional_after_containment={job.get('estimated_notional_after_containment')} "
                f"triggering_containment_rule={job.get('triggering_containment_rule')} "
                f"containment_decision_source={job.get('containment_decision_source')} "
                f"containment_denial_source={job.get('containment_denial_source')} "
                f"reason={job.get('containment_reason')}"
            )

        containment_fail_closed_reason = None
        containment_action = job.get("containment_action")
        decision_source = job.get("containment_decision_source")
        theoretical_size = job.get("theoretical_size_before_containment")
        estimated_notional_after_containment = job.get("estimated_notional_after_containment")
        containment_max_notional = job.get("containment_max_notional")

        if containment_action not in CONTAINMENT_ACTION_HIERARCHY:
            containment_fail_closed_reason = "invalid_containment_action"
            decision_source = "stage_containment"
        elif job.get("containment_status") != "active":
            containment_fail_closed_reason = "containment_status_not_active"
            decision_source = "stage_containment"
        elif not job.get("containment_stage") or not job.get("containment_profile"):
            containment_fail_closed_reason = "missing_stage_containment_context"
            decision_source = "stage_containment"
        elif decision_source not in CONTAINMENT_DECISION_SOURCES:
            containment_fail_closed_reason = "invalid_containment_decision_source"
            decision_source = "stage_containment"
        elif containment_action == "approve_as_is" and decision_source != "none":
            containment_fail_closed_reason = "approve_as_is_decision_source_not_none"
            decision_source = "stage_containment"
        elif containment_action == "capped" and decision_source not in {"size_containment", "exposure_containment"}:
            containment_fail_closed_reason = "capped_decision_source_invalid"
            decision_source = "stage_containment"
        elif approved_size_after_containment is None:
            containment_fail_closed_reason = "missing_containment_approved_size"
            decision_source = "size_containment"
        else:
            try:
                approved_size_value = float(approved_size_after_containment)
                theoretical_size_value = float(theoretical_size)
            except Exception:
                containment_fail_closed_reason = "containment_size_values_not_numeric"
                decision_source = "size_containment"
            else:
                if approved_size_value <= 0 or theoretical_size_value <= 0:
                    containment_fail_closed_reason = "containment_size_values_non_positive"
                    decision_source = "size_containment"
                elif approved_size_value > theoretical_size_value + 1e-9:
                    containment_fail_closed_reason = "approved_size_exceeds_theoretical_size"
                    decision_source = "size_containment"
                elif containment_action == "approve_as_is" and abs(approved_size_value - theoretical_size_value) > 1e-9:
                    containment_fail_closed_reason = "approve_as_is_size_mismatch"
                    decision_source = "size_containment"
                elif containment_action == "capped" and approved_size_value >= theoretical_size_value - 1e-9:
                    containment_fail_closed_reason = "capped_size_not_reduced"
                    decision_source = "size_containment"
                elif size_constraints["broker_type"] == "future":
                    size_valid, validation_reason = self.validate_position_size(
                        symbol,
                        approved_size_after_containment,
                    )
                    if not size_valid:
                        containment_fail_closed_reason = (
                            f"approved_size_invalid:{validation_reason or 'size_invalid'}"
                        )
                        decision_source = "size_containment"

        if containment_fail_closed_reason is None:
            if containment_max_notional is None:
                containment_fail_closed_reason = "missing_or_invalid_containment_max_notional"
                decision_source = "stage_containment"
            elif estimated_notional_after_containment is None:
                containment_fail_closed_reason = "approved_notional_unavailable"
                decision_source = "exposure_containment"
            else:
                try:
                    approved_notional_value = float(estimated_notional_after_containment)
                    containment_max_notional_value = float(containment_max_notional)
                except Exception:
                    containment_fail_closed_reason = "containment_notional_values_not_numeric"
                    decision_source = "exposure_containment"
                else:
                    if approved_notional_value <= 0:
                        containment_fail_closed_reason = "approved_notional_non_positive"
                        decision_source = "exposure_containment"
                    elif containment_max_notional_value <= 0:
                        containment_fail_closed_reason = "containment_max_notional_non_positive"
                        decision_source = "stage_containment"
                    elif approved_notional_value > containment_max_notional_value:
                        containment_fail_closed_reason = "approved_notional_exceeds_containment_cap"
                        decision_source = "exposure_containment"

        if containment_fail_closed_reason is not None:
            fail_closed_triggering_rule = {
                "size_containment": "instrument_size_cap",
                "exposure_containment": "instrument_notional_cap",
                "stage_containment": "stage_profile",
            }.get(decision_source, "stage_profile")
            job["containment_action"] = "denied"
            job["containment_reason"] = f"final_containment_coherence_failed:{containment_fail_closed_reason}"
            job["containment_decision_source"] = decision_source
            job["containment_denial_source"] = decision_source
            job["triggering_containment_rule"] = fail_closed_triggering_rule
            job["approved_size_after_containment"] = None
            approved_size_after_containment = None
            logger.error(
                "EXECUTION CONTAINMENT FAIL-CLOSED | "
                f"symbol={symbol} "
                f"side={side} "
                f"containment_stage={job.get('containment_stage')} "
                f"containment_profile={job.get('containment_profile')} "
                f"containment_action={job.get('containment_action')} "
                f"containment_reason={job.get('containment_reason')} "
                f"containment_decision_source={job.get('containment_decision_source')} "
                f"containment_denial_source={job.get('containment_denial_source')} "
                f"triggering_containment_rule={job.get('triggering_containment_rule')} "
                f"theoretical_size_before_containment={job.get('theoretical_size_before_containment')} "
                f"capped_size_after_size_containment={job.get('capped_size_after_size_containment')} "
                f"approved_size_after_containment={job.get('approved_size_after_containment')} "
                f"estimated_notional_after_containment={job.get('estimated_notional_after_containment')} "
                f"containment_max_notional={job.get('containment_max_notional')} "
                "decision=no_order_submitted"
            )
            return

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
            f"containment_stage={job.get('containment_stage')} "
            f"containment_profile={job.get('containment_profile')} "
            f"containment_max_size={job.get('containment_max_size')} "
            f"containment_max_notional={job.get('containment_max_notional')} "
            f"containment_hierarchy={job.get('containment_hierarchy')} "
            f"containment_decision_source={job.get('containment_decision_source')} "
            f"containment_denial_source={job.get('containment_denial_source')} "
            f"triggering_containment_rule={job.get('triggering_containment_rule')} "
            f"notional_model={job.get('notional_model')} "
            f"notional_currency={job.get('notional_currency')} "
            f"theoretical_size_before_containment={job.get('theoretical_size_before_containment')} "
            f"capped_size_after_size_containment={job.get('capped_size_after_size_containment')} "
            f"estimated_notional_before_containment={job.get('estimated_notional_before_containment')} "
            f"estimated_notional_after_size_containment={job.get('estimated_notional_after_size_containment')} "
            f"estimated_notional_after_containment={job.get('estimated_notional_after_containment')} "
            f"approved_size_after_containment={approved_size_after_containment} "
            f"containment_action={job.get('containment_action')} "
            f"containment_reason={job.get('containment_reason')} "
            f"planned_position_size={approved_size_after_containment} "
            f"risk_r={risk_distance_r} "
            f"intended_risk_pct={job.get('intended_risk_percent', 0)*100:.1f}%"
        )

        if self.is_connectivity_execution_blocked():
            return

        session_entry_blocked, session_close_state = self.should_block_new_entry_for_session_close(symbol)
        if session_entry_blocked:
            self.log_session_entry_blocked(
                symbol,
                side=side,
                job=job,
                location="place_bracket_order_before_order_id_allocation",
                state=session_close_state,
            )
            self.run_session_close_sweep(
                "place_bracket_order_entry_hard_gate",
                target_symbol=symbol,
            )
            return

        order_tif = self.get_order_tif(symbol, "bracket")
        if not order_tif:
            logger.error(
                "ORDER_TIF_RESOLUTION_FAILED | "
                f"symbol={symbol} "
                "order_role=bracket "
                "decision=skip_bracket_submit"
            )
            return

        if self.is_prd_dry_run_enabled():
            self.finalize_prd_dry_run_order_plan(
                job=job,
                symbol=symbol,
                side=side,
                entry=entry,
                stop=stop,
                target=target,
                spread_adjusted_entry=spread_adjusted_entry,
                approved_size_after_containment=approved_size_after_containment,
            )
            return

        try:
            self.assert_prd_live_release_allowed(
                symbol=symbol,
                job=job,
            )
        except Exception as exc:
            logger.critical(
                "PRD_LIVE_RELEASE_GATE_BLOCKED_BEFORE_TRADE_REGISTRATION | "
                f"symbol={symbol} "
                f"side={side} "
                f"reason={exc} "
                "decision=no_trade_record_no_order_submitted"
            )
            return

        self.assert_order_transmission_allowed(
            symbol=symbol,
            trade_id=None,
            reason_label="bracket_order_plan_pre_submit",
        )

        parent_id, tp_id, sl_id = self.allocate_bracket_order_ids()
        job["planned_parent_quantity"] = approved_size_after_containment
        job["planned_tp_quantity"] = approved_size_after_containment
        job["planned_sl_quantity"] = approved_size_after_containment

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

        parent = LimitOrder(parent_action, approved_size_after_containment, entry)
        parent.orderId = parent_id
        parent.transmit = False
        parent.tif = order_tif
        parent.orderRef = self.build_order_ref(trade_id, symbol, "ENTRY")

        tp = LimitOrder(child_action, approved_size_after_containment, target)
        tp.orderId = tp_id
        tp.parentId = parent_id
        tp.transmit = False
        tp.tif = order_tif
        tp.orderRef = self.build_order_ref(trade_id, symbol, "TP")

        sl = StopOrder(child_action, approved_size_after_containment, stop)
        sl.orderId = sl_id
        sl.parentId = parent_id
        sl.transmit = True
        sl.tif = order_tif
        sl.orderRef = self.build_order_ref(trade_id, symbol, "SL")

        with self.trade_analysis_lock:
            record = self.trade_analysis.get(trade_id)
            if record is not None:
                record["order_ref_entry"] = parent.orderRef
                record["order_ref_tp"] = tp.orderRef
                record["order_ref_sl"] = sl.orderRef

        for role, order in (("ENTRY", parent), ("TP", tp), ("SL", sl)):
            logger.info(
                "ORDER_REF_ASSIGNED | "
                f"run_id={self.run_id} "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"role={role} "
                f"order_id={getattr(order, 'orderId', None)} "
                f"orderRef={getattr(order, 'orderRef', None)}"
            )

        logger.info(
            f"ORDER DEF PARENT → orderId={parent.orderId} parentId={parent.parentId} "
            f"action={parent.action} orderType={parent.orderType} "
            f"totalQuantity={getattr(parent, 'totalQuantity', None)} "
            f"lmtPrice={getattr(parent, 'lmtPrice', None)} auxPrice={getattr(parent, 'auxPrice', None)} "
            f"transmit={parent.transmit} "
            f"tif={getattr(parent, 'tif', None)} "
            f"orderRef={getattr(parent, 'orderRef', None)} "
            f"outsideRth={getattr(parent, 'outsideRth', None)} "
            f"ocaGroup={getattr(parent, 'ocaGroup', None)} "
            f"ocaType={getattr(parent, 'ocaType', None)}"
        )
        logger.info(
            f"ORDER DEF TP → orderId={tp.orderId} parentId={tp.parentId} "
            f"action={tp.action} orderType={tp.orderType} "
            f"totalQuantity={getattr(tp, 'totalQuantity', None)} "
            f"lmtPrice={getattr(tp, 'lmtPrice', None)} auxPrice={getattr(tp, 'auxPrice', None)} "
            f"transmit={tp.transmit} "
            f"tif={getattr(tp, 'tif', None)} "
            f"orderRef={getattr(tp, 'orderRef', None)} "
            f"outsideRth={getattr(tp, 'outsideRth', None)} "
            f"ocaGroup={getattr(tp, 'ocaGroup', None)} "
            f"ocaType={getattr(tp, 'ocaType', None)}"
        )
        logger.info(
            f"ORDER DEF SL → orderId={sl.orderId} parentId={sl.parentId} "
            f"action={sl.action} orderType={sl.orderType} "
            f"totalQuantity={getattr(sl, 'totalQuantity', None)} "
            f"lmtPrice={getattr(sl, 'lmtPrice', None)} auxPrice={getattr(sl, 'auxPrice', None)} "
            f"transmit={sl.transmit} "
            f"tif={getattr(sl, 'tif', None)} "
            f"orderRef={getattr(sl, 'orderRef', None)} "
            f"outsideRth={getattr(sl, 'outsideRth', None)} "
            f"ocaGroup={getattr(sl, 'ocaGroup', None)} "
            f"ocaType={getattr(sl, 'ocaType', None)}"
        )

        self.set_trade_timestamp(trade_id, "bracket_submit_start_time")
        self.append_trade_event(trade_id, "PLACE ORDER START")
        self.update_bracket_submit_transaction(
            trade_id,
            bracket_submit_transaction_status="in_progress",
            bracket_submit_exception=None,
            bracket_submit_uncertain=False,
        )

        parent_trade = None
        tp_trade = None
        sl_trade = None
        try:
            self.mark_bracket_submit_leg_attempted(trade_id, "parent")
            parent_trade = self.broker_write_place_order(
                contract,
                parent,
                caller="place_bracket_order",
                reason_label="bracket_parent_submit",
                trade_id=trade_id,
                symbol=symbol,
            )
            self.mark_bracket_submit_leg_completed(trade_id, "parent", parent_trade)

            self.mark_bracket_submit_leg_attempted(trade_id, "tp")
            tp_trade = self.broker_write_place_order(
                contract,
                tp,
                caller="place_bracket_order",
                reason_label="bracket_tp_submit",
                trade_id=trade_id,
                symbol=symbol,
            )
            self.mark_bracket_submit_leg_completed(trade_id, "tp", tp_trade)

            self.mark_bracket_submit_leg_attempted(trade_id, "sl")
            sl_trade = self.broker_write_place_order(
                contract,
                sl,
                caller="place_bracket_order",
                reason_label="bracket_sl_submit",
                trade_id=trade_id,
                symbol=symbol,
            )
            self.mark_bracket_submit_leg_completed(trade_id, "sl", sl_trade)
        except Exception as exc:
            with self.trade_analysis_lock:
                record_snapshot = dict(self.trade_analysis.get(trade_id) or {})
            submit_context = self.get_bracket_submit_context_from_record(record_snapshot)
            self.handle_bracket_submission_failure(
                trade_id,
                record_snapshot,
                submit_context,
                exc,
            )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return

        self.update_bracket_submit_transaction(
            trade_id,
            bracket_submit_transaction_status="submitted",
            bracket_submit_uncertain=False,
        )
        self.append_trade_event(trade_id, "BRACKET SUBMIT TRANSACTION SUBMITTED")

        self.log_trade_snapshot("POST PLACE PARENT", parent_trade)
        self.log_trade_snapshot("POST PLACE TP", tp_trade)
        self.log_trade_snapshot("POST PLACE SL", sl_trade)

        try:
            self.broker_write_sleep(
                0.20,
                caller="place_bracket_order",
                reason_label="bracket_post_submit_wait",
                trade_id=trade_id,
                symbol=symbol,
            )
        except Exception as exc:
            self.handle_post_submit_uncertainty(
                trade_id,
                "bracket_post_submit_wait_failed",
                exc,
            )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return

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

            broker_confirmation = self.assess_broker_bracket_confirmation(
                parent_id,
                tp_id,
                sl_id,
                trade_analysis_lock_context="not_locked",
            )
            if broker_confirmation.get("reason") == "broker_state_check_failed":
                self.handle_post_submit_uncertainty(
                    trade_id,
                    "bracket_confirmation_read_failed_after_submit",
                    RuntimeError(broker_confirmation.get("reason")),
                )
                self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
                return
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

            try:
                self.broker_write_sleep(
                    0.10,
                    caller="place_bracket_order",
                    reason_label="bracket_confirmation_poll_wait",
                    trade_id=trade_id,
                    symbol=symbol,
                )
            except Exception as exc:
                self.handle_post_submit_uncertainty(
                    trade_id,
                    "bracket_confirmation_poll_wait_failed",
                    exc,
                )
                self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
                return

        self.log_trade_snapshot("POST CONFIRM PARENT", parent_trade)
        self.log_trade_snapshot("POST CONFIRM TP", tp_trade)
        self.log_trade_snapshot("POST CONFIRM SL", sl_trade)

        if broker_confirmation is None:
            broker_confirmation = self.assess_broker_bracket_confirmation(
                parent_id,
                tp_id,
                sl_id,
                trade_analysis_lock_context="not_locked",
            )
        if broker_confirmation.get("reason") == "broker_state_check_failed":
            self.handle_post_submit_uncertainty(
                trade_id,
                "bracket_final_confirmation_read_failed_after_submit",
                RuntimeError(broker_confirmation.get("reason")),
            )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return
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

        try:
            open_trades = self.broker_read_open_trades(
                caller="place_bracket_order",
                reason_label="post_submit_open_trades_inspection",
                trade_id=trade_id,
                symbol=symbol,
                trade_analysis_lock_context="not_locked",
            )
        except Exception as exc:
            self.handle_post_submit_uncertainty(
                trade_id,
                "post_submit_open_trades_inspection_failed",
                exc,
            )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return

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

        with self.trade_analysis_lock:
            protection_record_snapshot = dict(self.trade_analysis.get(trade_id) or {})

        broker_reality = None
        protection_context = self.classify_position_protection_context(
            protection_record_snapshot,
            broker_reality,
            broker_confirmation,
        )
        if protection_record_snapshot:
            broker_reality = self.get_trade_broker_reality(
                protection_record_snapshot,
                trade_analysis_lock_context="not_locked",
            )
            if broker_reality.get("check_failed"):
                self.handle_post_submit_uncertainty(
                    trade_id,
                    "post_submit_broker_reality_check_failed",
                    RuntimeError(broker_reality.get("failure")),
                )
                self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
                return
            protection_context = self.classify_position_protection_context(
                protection_record_snapshot,
                broker_reality,
                broker_confirmation,
            )

        if (
            broker_reality
            and broker_reality.get("check_failed")
            and (
                protection_record_snapshot.get("entry_filled")
                or self.has_realized_parent_entry(protection_record_snapshot)
            )
        ):
            self.emergency_flatten_unprotected_position(
                trade_id,
                symbol,
                "protective_emergency_broker_read_failed_after_submit",
                protection_context=protection_context,
                broker_reality=broker_reality,
            )
            logger.critical(
                "PROTECTIVE_EMERGENCY_FLATTEN_BLOCKED_BROKER_READ_FAILED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"parent_order_id={parent_id} "
                f"tp_order_id={tp_id} "
                f"sl_order_id={sl_id} "
                "operator_action_required=True "
                "decision=preserve_active_lock_no_blind_flatten"
            )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return

        quantity_coverage = self.assess_bracket_quantity_coverage(
            protection_record_snapshot,
            broker_confirmation=broker_confirmation,
            broker_reality=broker_reality,
        )
        self.log_bracket_quantity_coverage(protection_record_snapshot, quantity_coverage)
        logger.info(
            f"BRACKET VALIDATION SUMMARY | {json.dumps({'outcome': broker_confirmation.get('outcome'), 'reason': broker_confirmation.get('reason'), 'broker_state_category': broker_state_category, 'quantity_state': quantity_coverage.get('quantity_state'), 'quantity_coverage_ok': quantity_coverage.get('quantity_coverage_ok'), 'open_position_estimate': quantity_coverage.get('open_position_estimate'), 'protective_sl_coverage': quantity_coverage.get('protective_sl_coverage'), 'planned_parent_quantity': quantity_coverage.get('planned_parent_quantity'), 'planned_tp_quantity': quantity_coverage.get('planned_tp_quantity'), 'planned_sl_quantity': quantity_coverage.get('planned_sl_quantity'), 'parent_total_quantity': quantity_coverage.get('parent_total_quantity'), 'parent_filled_quantity': quantity_coverage.get('parent_filled_quantity'), 'parent_remaining_quantity': quantity_coverage.get('parent_remaining_quantity'), 'tp_total_quantity': quantity_coverage.get('tp_total_quantity'), 'tp_filled_quantity': quantity_coverage.get('tp_filled_quantity'), 'tp_remaining_quantity': quantity_coverage.get('tp_remaining_quantity'), 'sl_total_quantity': quantity_coverage.get('sl_total_quantity'), 'sl_filled_quantity': quantity_coverage.get('sl_filled_quantity'), 'sl_remaining_quantity': quantity_coverage.get('sl_remaining_quantity')}, sort_keys=True)}"
        )
        if quantity_coverage.get("quantity_mismatch_reason") and quantity_coverage.get("quantity_coverage_ok"):
            logger.warning(
                "BRACKET_QUANTITY_MISMATCH | "
                f"{json.dumps(self.build_quantity_coverage_log_fields(protection_record_snapshot, quantity_coverage), sort_keys=True)}"
            )

        if confirmed and not self.is_bracket_quantity_safe_for_ack(quantity_coverage):
            quantity_state = quantity_coverage.get("quantity_state")
            open_position_estimate = quantity_coverage.get("open_position_estimate")
            has_open_exposure = open_position_estimate is not None and open_position_estimate > 0
            now_dt = datetime.now(timezone.utc)
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    previous_state = record.get("state")
                    record["state"] = "BROKER_ACK_PENDING"
                    record["broker_ack_pending_since"] = record.get("broker_ack_pending_since") or now_dt
                    record["broker_ack_pending_last_check"] = now_dt
                    record["broker_ack_pending_reason"] = quantity_state
                    record["broker_ack_pending_category"] = broker_state_category
                    record["broker_ack_pending_visible_order_ids"] = broker_confirmation["visible_order_ids"]
                    self.append_trade_event(
                        trade_id,
                        f"BRACKET QUANTITY GATE BLOCKED previous_state={previous_state} "
                        f"quantity_state={quantity_state} "
                        f"open_position_estimate={open_position_estimate} "
                        f"protective_sl_coverage={quantity_coverage.get('protective_sl_coverage')} "
                        f"reason={quantity_coverage.get('quantity_mismatch_reason')}"
                    )
            self.set_execution_validation_status(
                trade_id,
                "validation_incomplete",
                quantity_state,
            )
            if has_open_exposure:
                self.emergency_flatten_unprotected_position(
                    trade_id,
                    symbol,
                    quantity_state,
                    protection_context=protection_context,
                    broker_reality=broker_reality,
                )
                self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
                return
            logger.warning(
                "BRACKET_QUANTITY_UNKNOWN_WAITING | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"quantity_state={quantity_state} "
                "decision=wait_recheck_no_open_exposure_no_emergency"
            )
            confirmed = False
            pending_broker_ack = True
            broker_state_category = "WAITABLE_ACK"

        if protection_context["protection_class"] in {
            "IN_POSITION_WITH_PROTECTIVE_EXITS",
            "IN_POSITION_WITH_PRIMARY_SL_PROTECTION",
        }:
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    previous_state = record["state"]
                    record["state"] = "EXIT_WORKING"
                    record["broker_ack_pending_reason"] = protection_context["reason"]
                    record["broker_ack_pending_category"] = broker_state_category
                    record["broker_ack_pending_visible_order_ids"] = broker_confirmation["visible_order_ids"]
                    self.append_trade_event(
                        trade_id,
                        "PARENT_FILLED_POSITION_OPEN_PROTECTIVE_EXITS_VISIBLE "
                        f"previous_state={previous_state} new_state={record['state']} "
                        f"broker_state_category={broker_state_category} "
                        f"protection_class={protection_context['protection_class']} "
                        f"reason={protection_context['reason']} "
                        f"visible_order_ids={broker_confirmation['visible_order_ids']}"
                    )
            self.set_execution_validation_status(
                trade_id,
                "broker_live" if broker_confirmation.get("all_broker_live") else "broker_acknowledged",
                protection_context["reason"],
            )
            logger.warning(
                "BRACKET_PARENT_FILLED_PROTECTIVE_EXITS_CONFIRMED | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"parent_order_id={parent_id} "
                f"tp_order_id={tp_id} "
                f"sl_order_id={sl_id} "
                f"parent_visible={protection_context['parent_visible']} "
                f"tp_visible={protection_context['tp_visible']} "
                f"sl_visible={protection_context['sl_visible']} "
                f"entry_filled={protection_record_snapshot.get('entry_filled')} "
                f"cumulative_entry_quantity={protection_record_snapshot.get('cumulative_entry_quantity')} "
                f"realized_entry_quantity={protection_record_snapshot.get('realized_entry_quantity')} "
                f"position_match={protection_context['position_match']} "
                f"position_sizes={(broker_reality or {}).get('matching_position_sizes')} "
                f"visible_order_ids={broker_confirmation['visible_order_ids']} "
                f"broker_state_category={broker_state_category} "
                f"protection_class={protection_context['protection_class']} "
                "decision=exit_working_preserve_protective_exits "
                f"reason={protection_context['reason']}"
            )
            logger.warning(
                f"{protection_context['protection_class']} | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"parent_order_id={parent_id} "
                f"tp_order_id={tp_id} "
                f"sl_order_id={sl_id} "
                f"parent_visible={protection_context['parent_visible']} "
                f"tp_visible={protection_context['tp_visible']} "
                f"sl_visible={protection_context['sl_visible']} "
                f"position_match={protection_context['position_match']} "
                f"position_sizes={(broker_reality or {}).get('matching_position_sizes')} "
                f"visible_order_ids={broker_confirmation['visible_order_ids']} "
                f"broker_state_category={broker_state_category} "
                "decision=protective_context_interpreted_before_ack_pending "
                f"reason={protection_context['reason']}"
            )
            if not protection_context["parent_visible"]:
                logger.info(
                    "PARENT_MISSING_AFTER_FULL_FILL_NORMAL | "
                    f"trade_id={trade_id} "
                    f"symbol={symbol} "
                    f"parent_order_id={parent_id} "
                    f"tp_order_id={tp_id} "
                    f"sl_order_id={sl_id} "
                    f"tp_visible={protection_context['tp_visible']} "
                    f"sl_visible={protection_context['sl_visible']} "
                    f"position_match={protection_context['position_match']} "
                    f"position_sizes={(broker_reality or {}).get('matching_position_sizes')} "
                    f"visible_order_ids={broker_confirmation['visible_order_ids']} "
                    f"broker_state_category={broker_state_category} "
                    "decision=parent_absence_accepted_after_full_fill "
                    f"reason={protection_context['reason']}"
                )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return

        if protection_context["protection_class"] in {
            "URGENT_RISK_STATE_SL_MISSING",
            "URGENT_RISK_STATE_UNPROTECTED_POSITION",
        }:
            now_dt = datetime.now(timezone.utc)
            with self.trade_analysis_lock:
                record = self.trade_analysis.get(trade_id)
                if record is not None:
                    previous_state = record["state"]
                    record["state"] = protection_context["target_state"] or record["state"]
                    record["broker_ack_pending_since"] = record.get("broker_ack_pending_since") or now_dt
                    record["broker_ack_pending_last_check"] = now_dt
                    record["broker_ack_pending_reason"] = protection_context["reason"]
                    record["broker_ack_pending_category"] = broker_state_category
                    record["broker_ack_pending_visible_order_ids"] = broker_confirmation["visible_order_ids"]
                    self.append_trade_event(
                        trade_id,
                        f"{protection_context['protection_class']} previous_state={previous_state} "
                        f"new_state={record['state']} broker_state_category={broker_state_category} "
                        f"visible_order_ids={broker_confirmation['visible_order_ids']} "
                        f"reason={protection_context['reason']}"
                    )
            self.set_execution_validation_status(
                trade_id,
                "validation_incomplete",
                protection_context["reason"],
            )
            log_label = (
                "PROTECTIVE_SL_MISSING_WITH_POSITION"
                if protection_context["protection_class"] == "URGENT_RISK_STATE_SL_MISSING"
                else "URGENT_RISK_STATE_UNPROTECTED_POSITION"
            )
            logger.critical(
                f"{log_label} | "
                f"trade_id={trade_id} "
                f"symbol={symbol} "
                f"parent_order_id={parent_id} "
                f"tp_order_id={tp_id} "
                f"sl_order_id={sl_id} "
                f"parent_visible={protection_context['parent_visible']} "
                f"tp_visible={protection_context['tp_visible']} "
                f"sl_visible={protection_context['sl_visible']} "
                f"entry_filled={protection_record_snapshot.get('entry_filled')} "
                f"cumulative_entry_quantity={protection_record_snapshot.get('cumulative_entry_quantity')} "
                f"realized_entry_quantity={protection_record_snapshot.get('realized_entry_quantity')} "
                f"position_match={protection_context['position_match']} "
                f"position_sizes={(broker_reality or {}).get('matching_position_sizes')} "
                f"visible_order_ids={broker_confirmation['visible_order_ids']} "
                f"broker_state_category={broker_state_category} "
                f"protection_class={protection_context['protection_class']} "
                "decision=preserve_active_unresolved_no_generic_cleanup "
                "operator_action_required=True "
                f"reason={protection_context['reason']}"
            )
            self.emergency_flatten_unprotected_position(
                trade_id,
                symbol,
                protection_context["reason"],
                protection_context=protection_context,
                broker_reality=broker_reality,
            )
            self.set_trade_timestamp(trade_id, "bracket_submit_end_time")
            return

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
                broker_reality = self.get_trade_broker_reality(
                    broker_reality_record,
                    trade_analysis_lock_context="not_locked",
                )

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
                if not self.session_socket_connected:
                    self.session_reconnect_count += 1
                    self.last_reconnect_time = datetime.now(timezone.utc)
                    logger.warning(
                        f"RECONNECT ATTEMPT #{self.session_reconnect_count} | "
                        f"socket_connected=false"
                    )
                    self.log_session_health(f"RECONNECT_ATTEMPT_{self.session_reconnect_count}")
                    self.enqueue_broker_system_job(
                        "RECONNECT_RECOVERY",
                        "watchdog_reconnect_required",
                        force=False,
                        reconnect_count=self.session_reconnect_count,
                        since_time=self.get_reconnect_fill_reconstruction_since(self.last_reconnect_time),
                    )
                elif not self.session_healthy:
                    logger.warning("FORCED SESSION RECOVERY (SOCKET ALIVE BUT SESSION UNHEALTHY)")
                    self.log_session_health("WATCHDOG_UNHEALTHY_BEFORE_RECOVERY")
                    self.enqueue_broker_system_job(
                        "RECONNECT_RECOVERY",
                        "WATCHDOG_SOCKET_ALIVE_SESSION_UNHEALTHY",
                        force=False,
                    )
            except Exception:
                logger.exception("WATCHDOG ERROR")

            time.sleep(10)

# ==========================================================
# API
# ==========================================================

app = FastAPI()

def initialize_bot_once():
    global bot
    with bot_init_lock:
        if bot is not None:
            logger.warning(
                "BOT_INITIALIZE_SKIPPED | "
                "reason=bot_already_initialized"
            )
            return bot

        validate_webhook_secret_config()
        acquire_runtime_process_lock()
        try:
            bot = ScalpingBot()
            logger.warning(
                "BOT_INITIALIZED | "
                f"bot_stage={BOT_STAGE} "
                f"bot_patch={BOT_PATCH} "
                f"lock_key={RUNTIME_LOCK_KEY}"
            )
            return bot
        except Exception:
            release_runtime_process_lock()
            raise

def get_bot(required=True):
    if bot is None and required:
        raise HTTPException(status_code=503, detail="Bot runtime not initialized")
    return bot

@app.on_event("startup")
def startup_event():
    initialize_bot_once()

@app.on_event("shutdown")
def shutdown_event():
    global bot
    runtime_bot = bot
    if runtime_bot is not None:
        logger.warning(
            "BOT_SHUTDOWN_BROKER_IO_SKIPPED | "
            "reason=shutdown_context_is_not_broker_io_owner "
            f"session_socket_connected={getattr(runtime_bot, 'session_socket_connected', None)} "
            "decision=release_process_lock_without_direct_ibkr_call"
        )
    release_runtime_process_lock()
    logger.warning("BOT_SHUTDOWN_COMPLETE")

@app.get("/health")
def health():
    runtime_bot = get_bot(required=False)
    prd_live_gate_result = (
        runtime_bot.evaluate_prd_live_release_gates() if runtime_bot else {
            "allowed": False,
            "failed_gates": [],
            "unknown_gates": ["bot_not_initialized"],
            "policy": {
                "prd_live_approval": parse_env_bool(os.getenv(PRD_LIVE_APPROVAL_ENV_VAR)),
                "dry_run_enabled": False,
            },
        }
    )
    return {
        "status": "ok",
        "bot_name": BOT_NAME,
        "bot_version": BOT_VERSION,
        "bot_patch": BOT_PATCH,
        "bot_stage": BOT_STAGE,
        "bot_initialized": runtime_bot is not None,
        "runtime_lock_acquired": RUNTIME_LOCK_SOCKET is not None,
        "runtime_lock_key": RUNTIME_LOCK_KEY,
        "webhook_secret_configured": bool(get_configured_webhook_secret()),
        "prd_live_gates_allowed": prd_live_gate_result.get("allowed"),
        "prd_live_failed_gates": prd_live_gate_result.get("failed_gates", []),
        "prd_live_unknown_gates": prd_live_gate_result.get("unknown_gates", []),
        "prd_live_approval_configured": (
            prd_live_gate_result.get("policy", {}).get("prd_live_approval") is True
        ),
        "prd_dry_run_enabled": (
            prd_live_gate_result.get("policy", {}).get("dry_run_enabled") is True
        ),
        "broker_io_owner_mode": (
            getattr(runtime_bot, "broker_io_owner_mode", None) if runtime_bot else None
        ),
        "singleton_lock_active": RUNTIME_LOCK_SOCKET is not None,
        "startup_reconciliation_required": (
            runtime_bot.startup_reconciliation_required if runtime_bot else None
        ),
        "startup_reconciliation_completed": (
            runtime_bot.startup_reconciliation_completed if runtime_bot else None
        ),
        "startup_reconciliation_status": (
            runtime_bot.startup_reconciliation_status if runtime_bot else "not_initialized"
        ),
        "startup_reconciliation_block_reason": (
            runtime_bot.startup_reconciliation_block_reason if runtime_bot else "bot_not_initialized"
        ),
        "startup_reconciliation_ambiguous_symbols": (
            sorted(runtime_bot.startup_reconciliation_ambiguous_symbols) if runtime_bot else []
        ),
    }

@app.post("/webhook/tradingview")
async def webhook_handler(request: Request):
    runtime_bot = get_bot()
    configured_secret = get_configured_webhook_secret()
    if webhook_secret_required() and not configured_secret:
        logger.critical(
            "WEBHOOK SECRET CONFIG MISSING | "
            f"stage={BOT_STAGE} "
            "decision=reject_webhook_fail_closed"
        )
        raise HTTPException(status_code=503, detail="Webhook secret not configured")

    raw_body = await request.body()
    raw_text = raw_body.decode("utf-8", errors="replace")

    try:
        data = json.loads(raw_text)
    except Exception:
        logger.exception("WEBHOOK JSON PARSE FAILED")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not isinstance(data, dict):
        logger.error(f"WEBHOOK JSON ROOT MUST BE OBJECT, GOT: {type(data)}")
        raise HTTPException(status_code=400, detail="JSON payload must be an object")

    received_secret = (
        request.headers.get("x-webhook-secret")
        or request.headers.get("authorization")
        or data.get("secret")
        or data.get("webhook_secret")
    )
    if configured_secret and not hmac.compare_digest(str(received_secret or ""), configured_secret):
        logger.warning(
            "WEBHOOK AUTH FAILED | "
            f"symbol={data.get('symbol')} "
            f"signal_id={data.get('signal_id')} "
            "reason=invalid_secret"
        )
        raise HTTPException(status_code=403)
    if not configured_secret and webhook_secret_required():
        logger.critical(
            "WEBHOOK AUTH FAILED | "
            "reason=missing_secret_config "
            f"stage={BOT_STAGE}"
        )
        raise HTTPException(status_code=403)

    logger.info(
        "WEBHOOK RECEIVED SANITIZED | "
        f"{json.dumps(build_sanitized_payload_summary(data), sort_keys=True)}"
    )

    try:
        status = runtime_bot.handle_webhook_signal(data)
    except Exception:
        logger.exception(
            "WEBHOOK PROCESSING FAILED | "
            f"payload_summary={json.dumps(build_sanitized_payload_summary(data), sort_keys=True)}"
        )
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

    return {"status": status}
