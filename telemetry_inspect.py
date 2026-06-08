import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path


DEFAULT_DB_PATH = Path("runtime_state") / "telemetry_events.sqlite3"
EVENT_COLUMNS = (
    "id",
    "timestamp_utc",
    "event_type",
    "event_group",
    "severity",
    "bot_stage",
    "symbol",
    "trade_id",
    "order_id",
    "signal_id",
    "state",
    "reason",
    "latency_ms",
)
DEFAULT_PERFORMANCE_BUCKET_FIELDS = ("symbol", "grade", "strategy_family", "session", "side")
VALID_PERFORMANCE_BUCKET_FIELDS = {
    "symbol",
    "grade",
    "strategy_family",
    "session",
    "session_name",
    "session_profile",
    "side",
    "exit_reason",
}
TIME_EXIT_REASON = "TIME_EXIT_5_MIN_MAX_DURATION"


class InspectError(Exception):
    pass


def resolve_db_path(path_text):
    return Path(path_text).expanduser()


def format_bytes(size):
    try:
        size = float(size)
    except Exception:
        return "unknown"
    units = ("B", "KB", "MB", "GB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} GB"


def connect_read_only(db_path):
    if not db_path.exists():
        raise InspectError(f"database_not_found: {db_path}")
    if not db_path.is_file():
        raise InspectError(f"database_path_not_file: {db_path}")

    uri = db_path.resolve().as_uri() + "?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise InspectError(f"database_open_failed: {exc}") from exc
    connection.row_factory = sqlite3.Row
    return connection


def ensure_events_table(connection):
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = ? AND name = ?",
        ("table", "telemetry_events"),
    ).fetchone()
    if row is None:
        raise InspectError("telemetry_events_table_not_found")


def open_events_db(db_path):
    connection = connect_read_only(db_path)
    try:
        ensure_events_table(connection)
    except Exception:
        connection.close()
        raise
    return connection


def row_value(row, key):
    value = row[key]
    return "" if value is None else str(value)


def parse_payload(row):
    raw = row["payload_json"] if "payload_json" in row.keys() else None
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {"payload": parsed}
    except Exception:
        return {"payload_json": raw, "payload_parse_error": True}


def format_payload_value(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def print_event(row, include_payload=True):
    line = (
        f"#{row_value(row, 'id')} "
        f"{row_value(row, 'timestamp_utc')} "
        f"{row_value(row, 'severity')} "
        f"{row_value(row, 'event_type')} "
        f"group={row_value(row, 'event_group')} "
        f"stage={row_value(row, 'bot_stage')} "
        f"symbol={row_value(row, 'symbol')} "
        f"trade_id={row_value(row, 'trade_id')} "
        f"signal_id={row_value(row, 'signal_id')} "
        f"state={row_value(row, 'state')} "
        f"reason={row_value(row, 'reason')} "
        f"latency_ms={row_value(row, 'latency_ms')}"
    )
    print(line)
    if include_payload:
        payload = parse_payload(row)
        if payload:
            print("  payload=" + json.dumps(payload, sort_keys=True))


def fetch_events(connection, where_sql="", params=(), order_sql="timestamp_utc ASC, id ASC", limit=None):
    columns = ", ".join(EVENT_COLUMNS + ("payload_json",))
    sql = f"SELECT {columns} FROM telemetry_events"
    if where_sql:
        sql += f" WHERE {where_sql}"
    sql += f" ORDER BY {order_sql}"
    query_params = list(params)
    if limit is not None:
        sql += " LIMIT ?"
        query_params.append(int(limit))
    return connection.execute(sql, query_params).fetchall()


def command_summary(connection, _args):
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS event_count,
            MIN(timestamp_utc) AS oldest_event,
            MAX(timestamp_utc) AS newest_event,
            COUNT(DISTINCT event_type) AS event_type_count,
            COUNT(DISTINCT trade_id) AS trade_count,
            COUNT(DISTINCT signal_id) AS signal_count
        FROM telemetry_events
        """
    ).fetchone()
    print("Telemetry Summary")
    print(f"events={row['event_count']}")
    print(f"oldest_event={row['oldest_event']}")
    print(f"newest_event={row['newest_event']}")
    print(f"event_types={row['event_type_count']}")
    print(f"trade_ids={row['trade_count']}")
    print(f"signal_ids={row['signal_count']}")
    print()
    print("Top Event Types")
    for item in connection.execute(
        """
        SELECT event_type, COUNT(*) AS count
        FROM telemetry_events
        GROUP BY event_type
        ORDER BY count DESC, event_type ASC
        LIMIT 20
        """
    ):
        print(f"{item['event_type']}: {item['count']}")


def command_recent(connection, args):
    rows = fetch_events(
        connection,
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Recent Events limit={args.limit}")
    for row in rows:
        print_event(row, include_payload=args.payload)


def command_trade(connection, args):
    rows = fetch_events(connection, "trade_id = ?", (args.trade_id,))
    print(f"Trade Timeline trade_id={args.trade_id} count={len(rows)}")
    for row in rows:
        print_event(row, include_payload=args.payload)


def command_signal(connection, args):
    rows = fetch_events(connection, "signal_id = ?", (args.signal_id,))
    print(f"Signal Timeline signal_id={args.signal_id} count={len(rows)}")
    for row in rows:
        print_event(row, include_payload=args.payload)


def command_event_type(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        (args.event_type,),
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Events event_type={args.event_type} limit={args.limit} count={len(rows)}")
    for row in rows:
        print_event(row, include_payload=args.payload)


def command_risk_blocks(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ? AND state = ?",
        ("RISK_GATE_DECISION", "blocked"),
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Risk Blocks limit={args.limit} count={len(rows)}")
    for row in rows:
        print_event(row, include_payload=args.payload)


def command_queue_decisions(connection, args):
    print("Queue Decision Counts")
    for row in connection.execute(
        """
        SELECT state, reason, COUNT(*) AS count
        FROM telemetry_events
        WHERE event_type = ?
        GROUP BY state, reason
        ORDER BY count DESC, state ASC, reason ASC
        """,
        ("QUEUE_DECISION",),
    ):
        print(f"state={row['state']} reason={row['reason']} count={row['count']}")
    print()
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("QUEUE_DECISION",),
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Recent Queue Decisions limit={args.limit} count={len(rows)}")
    for row in rows:
        print_event(row, include_payload=args.payload)


def command_closed_trades(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("TRADE_CLOSED",),
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Closed Trades limit={args.limit} count={len(rows)}")
    for row in rows:
        payload = parse_payload(row)
        fields = {
            "id": row["id"],
            "timestamp_utc": row["timestamp_utc"],
            "trade_id": row["trade_id"] or payload.get("trade_id"),
            "symbol": row["symbol"] or payload.get("symbol"),
            "grade": payload.get("grade"),
            "exit_reason": row["reason"] or payload.get("exit_reason"),
            "net_pnl": payload.get("net_pnl"),
            "net_r": payload.get("net_r"),
            "commission": payload.get("commission"),
            "entry_slippage": payload.get("entry_slippage"),
            "exit_slippage": payload.get("exit_slippage"),
            "duration_in_trade_sec": payload.get("duration_in_trade_sec"),
            "analysis_ready": payload.get("analysis_ready"),
            "missing_fields": payload.get("missing_fields"),
        }
        print(" ".join(f"{key}={format_payload_value(value)}" for key, value in fields.items()))
        if args.payload:
            print("  payload=" + json.dumps(payload, sort_keys=True))


def command_edge_decisions(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("EDGE_GATE_DECISION",),
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Edge Decisions limit={args.limit} count={len(rows)}")
    for row in rows:
        payload = parse_payload(row)
        candidate = payload.get("candidate")
        if not isinstance(candidate, dict):
            candidate = {}
        fields = {
            "id": row["id"],
            "timestamp_utc": row["timestamp_utc"],
            "symbol": row["symbol"] or candidate.get("symbol"),
            "signal_id": row["signal_id"],
            "would_allow": "null" if payload.get("would_allow") is None else payload.get("would_allow"),
            "reason": row["reason"] or payload.get("reason"),
            "bucket": payload.get("bucket_used"),
            "fallback_level": payload.get("bucket_fallback_level"),
            "sample_size": payload.get("sample_size"),
            "expectancy_r": payload.get("expectancy_r"),
            "net_pnl_total": payload.get("net_pnl_total"),
            "avg_slip_r": payload.get("avg_slippage_impact_r"),
            "cache_hit": payload.get("cache_hit"),
        }
        print(" ".join(f"{key}={format_payload_value(value)}" for key, value in fields.items()))
        if args.payload:
            print("  payload=" + json.dumps(payload, sort_keys=True))


def command_edge_summary(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("EDGE_GATE_DECISION",),
        order_sql="timestamp_utc ASC, id ASC",
    )
    summary = {}
    for row in rows:
        payload = parse_payload(row)
        key = (
            str(payload.get("would_allow")),
            row["reason"] or payload.get("reason") or "unknown",
            payload.get("bucket_used") or "unknown",
        )
        summary[key] = summary.get(key, 0) + 1

    print(f"Edge Summary count={len(rows)}")
    for (would_allow, reason, bucket), count in sorted(summary.items(), key=lambda item: (-item[1], item[0])):
        print(
            f"would_allow={would_allow} "
            f"reason={reason} "
            f"bucket={bucket} "
            f"count={count}"
        )


def command_slippage_decisions(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("SLIPPAGE_DEFENSE_DECISION",),
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Slippage Defense Decisions limit={args.limit} count={len(rows)}")
    for row in rows:
        payload = parse_payload(row)
        fields = {
            "id": row["id"],
            "timestamp_utc": row["timestamp_utc"],
            "phase": payload.get("phase") or row["state"],
            "symbol": row["symbol"] or payload.get("symbol"),
            "grade": payload.get("grade"),
            "side": payload.get("side"),
            "signal_id": row["signal_id"] or payload.get("signal_id"),
            "would_allow": "null" if payload.get("would_allow") is None else payload.get("would_allow"),
            "reason": row["reason"] or payload.get("reason"),
            "signal_age_sec": payload.get("signal_age_sec"),
            "queue_age_sec": payload.get("queue_age_sec"),
            "entry_drift_r": payload.get("entry_drift_r"),
            "historical_avg_slip_r": payload.get("historical_avg_slippage_impact_r"),
            "historical_sample_size": payload.get("historical_sample_size"),
            "historical_bucket": payload.get("historical_bucket_used"),
            "current_spread_status": payload.get("current_spread_status"),
        }
        print(" ".join(f"{key}={format_payload_value(value)}" for key, value in fields.items()))
        if args.payload:
            print("  payload=" + json.dumps(payload, sort_keys=True))


def command_slippage_summary(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("SLIPPAGE_DEFENSE_DECISION",),
        order_sql="timestamp_utc ASC, id ASC",
    )
    summary = {}
    for row in rows:
        payload = parse_payload(row)
        would_allow = "null" if payload.get("would_allow") is None else str(payload.get("would_allow"))
        key = (
            payload.get("phase") or row["state"] or "unknown",
            would_allow,
            row["reason"] or payload.get("reason") or "unknown",
            payload.get("historical_bucket_used") or "n/a",
        )
        summary[key] = summary.get(key, 0) + 1

    print(f"Slippage Defense Summary count={len(rows)}")
    for (phase, would_allow, reason, bucket), count in sorted(summary.items(), key=lambda item: (-item[1], item[0])):
        print(
            f"phase={phase} "
            f"would_allow={would_allow} "
            f"reason={reason} "
            f"historical_bucket={bucket} "
            f"count={count}"
        )


def command_learning_advice(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("TRADE_LEARNING_ADVICE",),
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Trade Learning Advice limit={args.limit} count={len(rows)}")
    for row in rows:
        payload = parse_payload(row)
        fields = {
            "id": row["id"],
            "timestamp_utc": row["timestamp_utc"],
            "symbol": row["symbol"] or payload.get("symbol"),
            "grade": payload.get("grade"),
            "side": payload.get("side"),
            "signal_id": row["signal_id"] or payload.get("signal_id"),
            "would_adjust": "null" if payload.get("would_adjust") is None else payload.get("would_adjust"),
            "reason": row["reason"] or payload.get("reason"),
            "confidence": payload.get("confidence"),
            "bucket": payload.get("bucket_used"),
            "sample_size": payload.get("sample_size"),
            "expectancy_r": payload.get("expectancy_r"),
            "recent_expectancy_r": payload.get("recent_expectancy_r"),
            "avg_slip_r": payload.get("avg_slippage_impact_r"),
            "recommendations": payload.get("recommendations"),
            "data_warnings": payload.get("data_warnings"),
        }
        print(" ".join(f"{key}={format_payload_value(value)}" for key, value in fields.items()))
        if args.payload:
            print("  payload=" + json.dumps(payload, sort_keys=True))


def command_learning_summary(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("TRADE_LEARNING_ADVICE",),
        order_sql="timestamp_utc ASC, id ASC",
    )
    summary = {}
    for row in rows:
        payload = parse_payload(row)
        would_adjust = "null" if payload.get("would_adjust") is None else str(payload.get("would_adjust"))
        key = (
            would_adjust,
            row["reason"] or payload.get("reason") or "unknown",
            payload.get("confidence") or "unknown",
            payload.get("bucket_used") or "unknown",
        )
        summary[key] = summary.get(key, 0) + 1

    print(f"Trade Learning Summary count={len(rows)}")
    for (would_adjust, reason, confidence, bucket), count in sorted(
        summary.items(),
        key=lambda item: (-item[1], item[0]),
    ):
        print(
            f"would_adjust={would_adjust} "
            f"reason={reason} "
            f"confidence={confidence} "
            f"bucket={bucket} "
            f"count={count}"
        )


def command_quality_gate_decisions(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("EXECUTION_QUALITY_GATE_DECISION",),
        order_sql="timestamp_utc DESC, id DESC",
        limit=args.limit,
    )
    print(f"Execution Quality Gate Decisions limit={args.limit} count={len(rows)}")
    for row in rows:
        payload = parse_payload(row)
        fields = {
            "id": row["id"],
            "timestamp_utc": row["timestamp_utc"],
            "mode": payload.get("mode"),
            "enforcement_active": payload.get("enforcement_active"),
            "would_block": payload.get("would_block"),
            "blocked": payload.get("blocked"),
            "reason": row["reason"] or payload.get("reason"),
            "symbol": row["symbol"] or payload.get("symbol"),
            "grade": payload.get("grade"),
            "side": payload.get("side"),
            "signal_id": row["signal_id"] or payload.get("signal_id"),
            "edge_reason": payload.get("edge_reason"),
            "edge_would_allow": "null" if payload.get("edge_would_allow") is None else payload.get("edge_would_allow"),
            "slippage_reason": payload.get("slippage_pre_queue_reason"),
            "slippage_would_allow": (
                "null"
                if payload.get("slippage_pre_queue_would_allow") is None
                else payload.get("slippage_pre_queue_would_allow")
            ),
            "block_sources": payload.get("block_sources"),
            "decision_effect": payload.get("decision_effect"),
        }
        print(" ".join(f"{key}={format_payload_value(value)}" for key, value in fields.items()))
        if args.payload:
            print("  payload=" + json.dumps(payload, sort_keys=True))


def command_quality_gate_summary(connection, args):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("EXECUTION_QUALITY_GATE_DECISION",),
        order_sql="timestamp_utc ASC, id ASC",
    )
    summary = {}
    for row in rows:
        payload = parse_payload(row)
        key = (
            payload.get("mode") or "unknown",
            str(payload.get("enforcement_active")),
            str(payload.get("would_block")),
            str(payload.get("blocked")),
            row["reason"] or payload.get("reason") or "unknown",
        )
        summary[key] = summary.get(key, 0) + 1

    print(f"Execution Quality Gate Summary count={len(rows)}")
    for (mode, enforcement_active, would_block, blocked, reason), count in sorted(
        summary.items(),
        key=lambda item: (-item[1], item[0]),
    ):
        print(
            f"mode={mode} "
            f"enforcement_active={enforcement_active} "
            f"would_block={would_block} "
            f"blocked={blocked} "
            f"reason={reason} "
            f"count={count}"
        )


def safe_float(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        result = float(value)
    else:
        text = str(value).strip()
        if not text or text.lower() in {"none", "null", "nan"}:
            return None
        try:
            result = float(text)
        except Exception:
            return None
    if result != result:
        return None
    return result


def normalize_text(value, default="unknown"):
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def normalize_missing_fields(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
        except Exception:
            pass
        return [item.strip() for item in text.split(",") if item.strip()]
    return [str(value)]


def median(values):
    values = sorted(value for value in values if value is not None)
    if not values:
        return None
    midpoint = len(values) // 2
    if len(values) % 2:
        return values[midpoint]
    return (values[midpoint - 1] + values[midpoint]) / 2.0


def average(values):
    values = [value for value in values if value is not None]
    if not values:
        return None
    return sum(values) / len(values)


def format_number(value, digits=2):
    if value is None:
        return "null"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "null"


def format_percent(value, digits=1):
    if value is None:
        return "null"
    try:
        return f"{float(value) * 100:.{digits}f}%"
    except Exception:
        return "null"


def format_profit_factor(metrics):
    value = metrics.get("profit_factor")
    reason = metrics.get("profit_factor_reason")
    if value is None:
        return f"null:{reason}" if reason else "null"
    return format_number(value, 2)


def compact_missing_fields(frequency, limit=3):
    if not frequency:
        return "none"
    items = sorted(frequency.items(), key=lambda item: (-item[1], item[0]))
    return ",".join(f"{field}:{count}" for field, count in items[:limit])


def get_payload_session_context(payload):
    context = payload.get("session_context")
    if not isinstance(context, dict):
        context = {}
    session_name = (
        context.get("session_name")
        or payload.get("session_name")
        or payload.get("session")
    )
    session_profile = context.get("session_profile") or payload.get("session_profile")
    session = session_name or session_profile or "unknown"
    return {
        "session": normalize_text(session),
        "session_name": normalize_text(session_name),
        "session_profile": normalize_text(session_profile),
    }


def build_closed_trade_record(row):
    payload = parse_payload(row)
    session_context = get_payload_session_context(payload)
    missing_fields = normalize_missing_fields(payload.get("missing_fields"))
    payload_parse_error = bool(payload.get("payload_parse_error"))
    if payload_parse_error:
        missing_fields.append("payload_json")
    event_schema_version = payload.get("event_schema_version")
    if event_schema_version != "trade_closed.v2":
        missing_fields.append("event_schema_version")
    if "analysis_ready" not in payload:
        missing_fields.append("analysis_ready")

    exit_reason = row["reason"] or payload.get("exit_reason")
    emergency_flatten_exit_quantity = safe_float(payload.get("emergency_flatten_exit_quantity"))
    entry_slippage = safe_float(payload.get("entry_slippage"))
    exit_slippage = safe_float(payload.get("exit_slippage"))
    planned_r_points = safe_float(payload.get("planned_r_points"))
    slippage_values = [
        value
        for value in (entry_slippage, exit_slippage)
        if value is not None
    ]
    slippage_impact_r = None
    if planned_r_points is not None and planned_r_points > 0 and slippage_values:
        slippage_impact_r = sum(slippage_values) / planned_r_points

    exit_reason_text = normalize_text(exit_reason, default="")
    return {
        "id": row["id"],
        "timestamp_utc": row["timestamp_utc"],
        "trade_id": row["trade_id"] or payload.get("trade_id"),
        "symbol": normalize_text(row["symbol"] or payload.get("symbol")),
        "grade": normalize_text(payload.get("grade")),
        "strategy_family": normalize_text(payload.get("strategy_family")),
        "session": session_context["session"],
        "session_name": session_context["session_name"],
        "session_profile": session_context["session_profile"],
        "side": normalize_text(payload.get("side")),
        "exit_reason": normalize_text(exit_reason),
        "event_schema_version": event_schema_version,
        "analysis_ready": payload.get("analysis_ready") is True,
        "payload_parse_error": payload_parse_error,
        "missing_fields": sorted(set(missing_fields)),
        "gross_pnl": safe_float(payload.get("gross_pnl")),
        "commission": safe_float(payload.get("commission")),
        "net_pnl": safe_float(payload.get("net_pnl")),
        "net_r": safe_float(payload.get("net_r")),
        "planned_r_points": planned_r_points,
        "entry_slippage": entry_slippage,
        "exit_slippage": exit_slippage,
        "slippage_impact_r": slippage_impact_r,
        "time_exit": exit_reason_text == TIME_EXIT_REASON,
        "emergency_flatten": (
            (emergency_flatten_exit_quantity is not None and emergency_flatten_exit_quantity > 0)
            or "EMERGENCY_FLATTEN" in exit_reason_text.upper()
        ),
        "emergency_flatten_exit_quantity": emergency_flatten_exit_quantity,
        "payload": payload,
    }


def fetch_closed_trade_records(connection):
    rows = fetch_events(
        connection,
        "event_type = ?",
        ("TRADE_CLOSED",),
        order_sql="timestamp_utc ASC, id ASC",
    )
    return [build_closed_trade_record(row) for row in rows]


def is_default_performance_record(record):
    return (
        record.get("event_schema_version") == "trade_closed.v2"
        and not record.get("payload_parse_error")
        and record.get("net_pnl") is not None
    )


def select_performance_records(records, include_incomplete=False):
    if include_incomplete:
        return list(records)
    return [record for record in records if is_default_performance_record(record)]


def parse_bucket_fields(bucket_text):
    if not bucket_text:
        return list(DEFAULT_PERFORMANCE_BUCKET_FIELDS)
    fields = [field.strip() for field in bucket_text.split(",") if field.strip()]
    invalid = [field for field in fields if field not in VALID_PERFORMANCE_BUCKET_FIELDS]
    if invalid:
        raise InspectError(f"invalid_bucket_fields: {','.join(invalid)}")
    return fields or list(DEFAULT_PERFORMANCE_BUCKET_FIELDS)


def bucket_key_for_record(record, bucket_fields):
    return tuple(normalize_text(record.get(field)) for field in bucket_fields)


def bucket_label(bucket_key):
    return "|".join(bucket_key) if bucket_key else "ALL"


def build_missing_fields_frequency(records):
    frequency = {}
    for record in records:
        for field in record.get("missing_fields", []):
            frequency[field] = frequency.get(field, 0) + 1
    return frequency


def calculate_max_drawdown(records):
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for record in sorted(records, key=lambda item: (item.get("timestamp_utc") or "", item.get("id") or 0)):
        net_pnl = record.get("net_pnl")
        if net_pnl is None:
            continue
        equity += net_pnl
        if equity > peak:
            peak = equity
        drawdown = equity - peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return round(max_drawdown, 10)


def build_performance_metrics(records, min_trades):
    closed_trades = len(records)
    analysis_ready_trades = sum(1 for record in records if record.get("analysis_ready"))
    gross_values = [record["gross_pnl"] for record in records if record.get("gross_pnl") is not None]
    commission_values = [record["commission"] for record in records if record.get("commission") is not None]
    net_pnl_values = [record["net_pnl"] for record in records if record.get("net_pnl") is not None]
    net_r_values = [record["net_r"] for record in records if record.get("net_r") is not None]
    win_r_values = [value for value in net_r_values if value > 0]
    loss_r_values = [value for value in net_r_values if value < 0]
    positive_net_pnl = [value for value in net_pnl_values if value > 0]
    negative_net_pnl = [value for value in net_pnl_values if value < 0]
    slippage_impacts = [
        record["slippage_impact_r"]
        for record in records
        if record.get("slippage_impact_r") is not None
    ]

    gross_pnl_total = sum(gross_values) if gross_values else None
    commission_total = sum(commission_values) if commission_values else None
    net_pnl_total = sum(net_pnl_values) if net_pnl_values else None
    profit_factor = None
    profit_factor_reason = None
    if positive_net_pnl and negative_net_pnl:
        profit_factor = sum(positive_net_pnl) / abs(sum(negative_net_pnl))
    elif positive_net_pnl and not negative_net_pnl:
        profit_factor_reason = "no_losses"
    elif not positive_net_pnl and negative_net_pnl:
        profit_factor = 0.0
    else:
        profit_factor_reason = "no_wins_or_losses"

    winrate = None
    if net_pnl_values:
        winrate = len(positive_net_pnl) / len(net_pnl_values)

    missing_fields_frequency = build_missing_fields_frequency(records)
    avg_slippage_impact_r = average(slippage_impacts)
    analysis_ready_ratio = (analysis_ready_trades / closed_trades) if closed_trades else None

    decision_hint = "neutral_observed"
    if closed_trades < min_trades:
        decision_hint = "insufficient_sample"
    elif analysis_ready_ratio is not None and analysis_ready_ratio < 0.8:
        decision_hint = "incomplete_data"
    elif (
        (net_r_values and average(net_r_values) is not None and average(net_r_values) < 0)
        or (net_pnl_total is not None and net_pnl_total < 0)
    ):
        decision_hint = "edge_negative_observed"
    elif avg_slippage_impact_r is not None and avg_slippage_impact_r > 0.25:
        decision_hint = "slippage_warning"
    elif (
        net_r_values
        and average(net_r_values) is not None
        and average(net_r_values) > 0
        and net_pnl_total is not None
        and net_pnl_total > 0
    ):
        decision_hint = "edge_positive_observed"

    return {
        "closed_trades": closed_trades,
        "analysis_ready_trades": analysis_ready_trades,
        "net_pnl_trades": len(net_pnl_values),
        "net_r_trades": len(net_r_values),
        "gross_pnl_total": gross_pnl_total,
        "commission_total": commission_total,
        "net_pnl_total": net_pnl_total,
        "expectancy_eur": (net_pnl_total / len(net_pnl_values)) if net_pnl_values else None,
        "expectancy_r": average(net_r_values),
        "winrate": winrate,
        "average_win_r": average(win_r_values),
        "average_loss_r": average(loss_r_values),
        "profit_factor": profit_factor,
        "profit_factor_reason": profit_factor_reason,
        "max_drawdown": calculate_max_drawdown(records),
        "slippage_ready_trades": len(slippage_impacts),
        "avg_slippage_impact_r": avg_slippage_impact_r,
        "median_slippage_impact_r": median(slippage_impacts),
        "time_exit_ratio": (
            sum(1 for record in records if record.get("time_exit")) / closed_trades
            if closed_trades else None
        ),
        "emergency_flatten_ratio": (
            sum(1 for record in records if record.get("emergency_flatten")) / closed_trades
            if closed_trades else None
        ),
        "missing_fields_frequency": missing_fields_frequency,
        "decision_hint": decision_hint,
    }


def format_performance_metrics_line(bucket, metrics):
    parts = [
        f"bucket={bucket}",
        f"trades={metrics['closed_trades']}",
        f"ready={metrics['analysis_ready_trades']}",
        f"net_pnl_trades={metrics['net_pnl_trades']}",
        f"net_r_trades={metrics['net_r_trades']}",
        f"gross_pnl={format_number(metrics['gross_pnl_total'], 2)}",
        f"commission={format_number(metrics['commission_total'], 2)}",
        f"net_pnl={format_number(metrics['net_pnl_total'], 2)}",
        f"expectancy_eur={format_number(metrics['expectancy_eur'], 2)}",
        f"expectancy_r={format_number(metrics['expectancy_r'], 4)}",
        f"winrate={format_percent(metrics['winrate'], 1)}",
        f"avg_win_r={format_number(metrics['average_win_r'], 4)}",
        f"avg_loss_r={format_number(metrics['average_loss_r'], 4)}",
        f"profit_factor={format_profit_factor(metrics)}",
        f"max_dd={format_number(metrics['max_drawdown'], 2)}",
        f"slippage_ready={metrics['slippage_ready_trades']}",
        f"avg_slip_r={format_number(metrics['avg_slippage_impact_r'], 4)}",
        f"median_slip_r={format_number(metrics['median_slippage_impact_r'], 4)}",
        f"time_exit_ratio={format_percent(metrics['time_exit_ratio'], 1)}",
        f"emergency_flatten_ratio={format_percent(metrics['emergency_flatten_ratio'], 1)}",
        f"missing_top={compact_missing_fields(metrics['missing_fields_frequency'])}",
        f"decision_hint={metrics['decision_hint']}",
    ]
    return " ".join(parts)


def sort_performance_items(items, sort_key):
    def metric_value(item, name, default=0.0):
        value = item[1].get(name)
        return default if value is None else value

    if sort_key == "expectancy_r_desc":
        return sorted(items, key=lambda item: (metric_value(item, "expectancy_r", -999999.0), metric_value(item, "net_pnl_total", -999999.0)), reverse=True)
    if sort_key == "expectancy_r_asc":
        return sorted(items, key=lambda item: (metric_value(item, "expectancy_r", 999999.0), metric_value(item, "net_pnl_total", 999999.0)))
    if sort_key == "net_pnl_asc":
        return sorted(items, key=lambda item: metric_value(item, "net_pnl_total", 999999.0))
    if sort_key == "trades_desc":
        return sorted(items, key=lambda item: item[1].get("closed_trades", 0), reverse=True)
    if sort_key == "max_dd_asc":
        return sorted(items, key=lambda item: metric_value(item, "max_drawdown", 0.0))
    if sort_key == "slippage_desc":
        return sorted(items, key=lambda item: metric_value(item, "avg_slippage_impact_r", -999999.0), reverse=True)
    return sorted(items, key=lambda item: metric_value(item, "net_pnl_total", -999999.0), reverse=True)


def print_performance_selection_header(all_records, selected_records, include_incomplete):
    print(
        f"total_closed_events={len(all_records)} "
        f"selected_trades={len(selected_records)} "
        f"excluded_incomplete={len(all_records) - len(selected_records)} "
        f"include_incomplete={str(bool(include_incomplete)).lower()}"
    )


def command_performance_summary(connection, args):
    all_records = fetch_closed_trade_records(connection)
    records = select_performance_records(all_records, args.include_incomplete)
    print("Performance Summary")
    print_performance_selection_header(all_records, records, args.include_incomplete)
    metrics = build_performance_metrics(records, args.min_trades)
    print(format_performance_metrics_line("ALL", metrics))
    print("Missing Fields Frequency")
    if metrics["missing_fields_frequency"]:
        for field, count in sorted(metrics["missing_fields_frequency"].items(), key=lambda item: (-item[1], item[0])):
            print(f"{field}: {count}")
    else:
        print("none")


def command_performance_buckets(connection, args):
    bucket_fields = parse_bucket_fields(args.bucket_by)
    all_records = fetch_closed_trade_records(connection)
    records = select_performance_records(all_records, args.include_incomplete)
    print(f"Performance Buckets bucket_by={','.join(bucket_fields)} sort={args.sort} limit={args.limit}")
    print_performance_selection_header(all_records, records, args.include_incomplete)

    buckets = {}
    for record in records:
        key = bucket_key_for_record(record, bucket_fields)
        buckets.setdefault(key, []).append(record)

    items = [
        (key, build_performance_metrics(bucket_records, args.min_trades))
        for key, bucket_records in buckets.items()
    ]
    for key, metrics in sort_performance_items(items, args.sort)[:args.limit]:
        print(format_performance_metrics_line(bucket_label(key), metrics))


def command_performance_detail(connection, args):
    bucket_fields = parse_bucket_fields(args.bucket_by)
    all_records = fetch_closed_trade_records(connection)
    records = select_performance_records(all_records, args.include_incomplete)
    if args.bucket:
        records = [
            record for record in records
            if bucket_label(bucket_key_for_record(record, bucket_fields)) == args.bucket
        ]
    records = sorted(records, key=lambda item: (item.get("timestamp_utc") or "", item.get("id") or 0), reverse=True)
    print(f"Performance Detail bucket_by={','.join(bucket_fields)} limit={args.limit}")
    print_performance_selection_header(all_records, records, args.include_incomplete)
    for record in records[:args.limit]:
        fields = {
            "id": record["id"],
            "timestamp_utc": record["timestamp_utc"],
            "bucket": bucket_label(bucket_key_for_record(record, bucket_fields)),
            "trade_id": record["trade_id"],
            "symbol": record["symbol"],
            "grade": record["grade"],
            "exit_reason": record["exit_reason"],
            "net_pnl": format_number(record["net_pnl"], 2),
            "net_r": format_number(record["net_r"], 4),
            "slippage_impact_r": format_number(record["slippage_impact_r"], 4),
            "analysis_ready": str(record["analysis_ready"]).lower(),
            "schema": record["event_schema_version"] or "",
            "missing_fields": ",".join(record["missing_fields"]) if record["missing_fields"] else "none",
        }
        print(" ".join(f"{key}={value}" for key, value in fields.items()))


def command_health(db_path, args):
    print("Telemetry Database Health")
    print(f"path={db_path}")
    print(f"exists={db_path.exists()}")
    if not db_path.exists():
        print("event_count=0")
        print("status=database_missing")
        return 0

    print(f"database_size={format_bytes(db_path.stat().st_size)}")
    wal_path = Path(str(db_path) + "-wal")
    print(f"wal_exists={wal_path.exists()}")
    print(f"wal_size={format_bytes(wal_path.stat().st_size) if wal_path.exists() else '0 B'}")

    try:
        connection = open_events_db(db_path)
    except InspectError as exc:
        print(f"status={exc}")
        return 2

    with connection:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS event_count,
                MIN(timestamp_utc) AS oldest_event,
                MAX(timestamp_utc) AS newest_event,
                COUNT(DISTINCT event_type) AS event_type_count
            FROM telemetry_events
            """
        ).fetchone()
    connection.close()
    print(f"event_count={row['event_count']}")
    print(f"oldest_event={row['oldest_event']}")
    print(f"newest_event={row['newest_event']}")
    print(f"event_type_count={row['event_type_count']}")
    print("status=ok")
    return 0


def build_parser():
    parser = argparse.ArgumentParser(description="Inspect scalpingbot telemetry SQLite events.")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to telemetry_events.sqlite3")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_performance_arguments(command_parser, include_bucket_filter=False):
        command_parser.add_argument("--limit", type=int, default=20)
        command_parser.add_argument("--min-trades", type=int, default=30)
        command_parser.add_argument(
            "--bucket-by",
            default=",".join(DEFAULT_PERFORMANCE_BUCKET_FIELDS),
            help="Comma-separated bucket dimensions.",
        )
        command_parser.add_argument("--include-incomplete", action="store_true")
        command_parser.add_argument(
            "--sort",
            default="net_pnl_desc",
            choices=(
                "net_pnl_desc",
                "net_pnl_asc",
                "expectancy_r_desc",
                "expectancy_r_asc",
                "trades_desc",
                "max_dd_asc",
                "slippage_desc",
            ),
        )
        if include_bucket_filter:
            command_parser.add_argument("--bucket", help="Exact rendered bucket label to show.")

    subparsers.add_parser("summary")

    recent = subparsers.add_parser("recent")
    recent.add_argument("--limit", type=int, default=20)
    recent.add_argument("--payload", action="store_true")

    trade = subparsers.add_parser("trade")
    trade.add_argument("trade_id")
    trade.add_argument("--payload", action="store_true")

    signal = subparsers.add_parser("signal")
    signal.add_argument("signal_id")
    signal.add_argument("--payload", action="store_true")

    event_type = subparsers.add_parser("event-type")
    event_type.add_argument("event_type")
    event_type.add_argument("--limit", type=int, default=20)
    event_type.add_argument("--payload", action="store_true")

    risk_blocks = subparsers.add_parser("risk-blocks")
    risk_blocks.add_argument("--limit", type=int, default=20)
    risk_blocks.add_argument("--payload", action="store_true")

    queue_decisions = subparsers.add_parser("queue-decisions")
    queue_decisions.add_argument("--limit", type=int, default=20)
    queue_decisions.add_argument("--payload", action="store_true")

    closed_trades = subparsers.add_parser("closed-trades")
    closed_trades.add_argument("--limit", type=int, default=20)
    closed_trades.add_argument("--payload", action="store_true")

    edge_decisions = subparsers.add_parser("edge-decisions")
    edge_decisions.add_argument("--limit", type=int, default=20)
    edge_decisions.add_argument("--payload", action="store_true")

    subparsers.add_parser("edge-summary")

    slippage_decisions = subparsers.add_parser("slippage-decisions")
    slippage_decisions.add_argument("--limit", type=int, default=20)
    slippage_decisions.add_argument("--payload", action="store_true")

    subparsers.add_parser("slippage-summary")

    learning_advice = subparsers.add_parser("learning-advice")
    learning_advice.add_argument("--limit", type=int, default=20)
    learning_advice.add_argument("--payload", action="store_true")

    subparsers.add_parser("learning-summary")

    quality_gate_decisions = subparsers.add_parser("quality-gate-decisions")
    quality_gate_decisions.add_argument("--limit", type=int, default=20)
    quality_gate_decisions.add_argument("--payload", action="store_true")

    subparsers.add_parser("quality-gate-summary")

    performance_summary = subparsers.add_parser("performance-summary")
    add_performance_arguments(performance_summary)

    performance_buckets = subparsers.add_parser("performance-buckets")
    add_performance_arguments(performance_buckets)

    performance_detail = subparsers.add_parser("performance-detail")
    add_performance_arguments(performance_detail, include_bucket_filter=True)

    subparsers.add_parser("health")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    db_path = resolve_db_path(args.db)

    if args.command == "health":
        return command_health(db_path, args)

    try:
        connection = open_events_db(db_path)
    except InspectError as exc:
        print(f"telemetry_inspect_error={exc}", file=sys.stderr)
        return 2

    try:
        if args.command == "summary":
            command_summary(connection, args)
        elif args.command == "recent":
            command_recent(connection, args)
        elif args.command == "trade":
            command_trade(connection, args)
        elif args.command == "signal":
            command_signal(connection, args)
        elif args.command == "event-type":
            command_event_type(connection, args)
        elif args.command == "risk-blocks":
            command_risk_blocks(connection, args)
        elif args.command == "queue-decisions":
            command_queue_decisions(connection, args)
        elif args.command == "closed-trades":
            command_closed_trades(connection, args)
        elif args.command == "edge-decisions":
            command_edge_decisions(connection, args)
        elif args.command == "edge-summary":
            command_edge_summary(connection, args)
        elif args.command == "slippage-decisions":
            command_slippage_decisions(connection, args)
        elif args.command == "slippage-summary":
            command_slippage_summary(connection, args)
        elif args.command == "learning-advice":
            command_learning_advice(connection, args)
        elif args.command == "learning-summary":
            command_learning_summary(connection, args)
        elif args.command == "quality-gate-decisions":
            command_quality_gate_decisions(connection, args)
        elif args.command == "quality-gate-summary":
            command_quality_gate_summary(connection, args)
        elif args.command == "performance-summary":
            command_performance_summary(connection, args)
        elif args.command == "performance-buckets":
            command_performance_buckets(connection, args)
        elif args.command == "performance-detail":
            command_performance_detail(connection, args)
        else:
            parser.error(f"unknown command: {args.command}")
    except InspectError as exc:
        print(f"telemetry_inspect_error={exc}", file=sys.stderr)
        return 2
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
