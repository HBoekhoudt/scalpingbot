import html
import json
import logging
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse


DEFAULT_DB_PATH = Path("runtime_state") / "telemetry_events.sqlite3"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8088
DEFAULT_REFRESH_SECONDS = 30
SQLITE_TIMEOUT_SECONDS = 0.5
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
TIME_EXIT_REASON = "TIME_EXIT_5_MIN_MAX_DURATION"

logger = logging.getLogger("ikbr_telemetry_dashboard")


class DashboardError(Exception):
    pass


def parse_int(value, default, minimum=None, maximum=None):
    try:
        result = int(value)
    except Exception:
        result = default
    if minimum is not None and result < minimum:
        result = minimum
    if maximum is not None and result > maximum:
        result = maximum
    return result


def parse_bool(value):
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


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


def average(values):
    values = [value for value in values if value is not None]
    return sum(values) / len(values) if values else None


def normalize_text(value, default="unknown"):
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def normalize_sensitive_key(key):
    return "".join(character.lower() for character in str(key) if character.isalnum())


def is_sensitive_key(key):
    normalized_key = normalize_sensitive_key(key)
    return any(
        sensitive and sensitive in normalized_key
        for sensitive in (normalize_sensitive_key(item) for item in SENSITIVE_PAYLOAD_KEYS)
    )


def redact_payload(value):
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            redacted[key] = "[REDACTED]" if is_sensitive_key(key) else redact_payload(item)
        return redacted
    if isinstance(value, list):
        return [redact_payload(item) for item in value]
    return value


def parse_payload_json(raw):
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {"payload": parsed}
    except Exception:
        return {"payload_parse_error": True}


def payload_summary(payload, include_payload=False):
    if not isinstance(payload, dict):
        return {"payload_type": type(payload).__name__}
    summary_keys = (
        "mode",
        "phase",
        "reason",
        "would_allow",
        "would_block",
        "blocked",
        "would_adjust",
        "confidence",
        "bucket_used",
        "historical_bucket_used",
        "sample_size",
        "expectancy_r",
        "avg_slippage_impact_r",
        "recommendations",
        "block_sources",
        "bot_version",
        "bot_patch",
    )
    summary = {key: payload.get(key) for key in summary_keys if key in payload}
    if include_payload:
        summary["payload_redacted"] = redact_payload(payload)
    return summary


def resolve_db_path(path_text=None):
    return Path(path_text or os.getenv("IKBR_DASHBOARD_DB_PATH") or DEFAULT_DB_PATH).expanduser()


def open_read_only_connection(db_path):
    path = Path(db_path).expanduser()
    if not path.exists():
        raise DashboardError(f"database_not_found: {path}")
    if not path.is_file():
        raise DashboardError(f"database_path_not_file: {path}")
    uri = path.resolve().as_uri() + "?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=SQLITE_TIMEOUT_SECONDS)
    except sqlite3.Error as exc:
        raise DashboardError(f"database_open_failed: {exc}") from exc
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only = ON")
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = ? AND name = ?",
            ("table", "telemetry_events"),
        ).fetchone()
        if row is None:
            raise DashboardError("telemetry_events_table_not_found")
    except Exception:
        connection.close()
        raise
    return connection


def with_connection(db_path, builder):
    try:
        connection = open_read_only_connection(db_path)
        try:
            return {"ok": True, "data": builder(connection)}
        finally:
            connection.close()
    except DashboardError as exc:
        return {"ok": False, "error": str(exc), "db_path": str(db_path)}
    except sqlite3.Error as exc:
        return {"ok": False, "error": f"sqlite_error: {exc}", "db_path": str(db_path)}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "db_path": str(db_path)}


def row_to_event(row, include_payload=False):
    event = {column: row[column] for column in EVENT_COLUMNS if column in row.keys()}
    payload = parse_payload_json(row["payload_json"] if "payload_json" in row.keys() else None)
    event["payload_summary"] = payload_summary(payload, include_payload=include_payload)
    return event


def fetch_events(connection, event_type=None, limit=20, include_payload=False):
    columns = ", ".join(EVENT_COLUMNS + ("payload_json",))
    sql = f"SELECT {columns} FROM telemetry_events"
    params = []
    if event_type:
        sql += " WHERE event_type = ?"
        params.append(event_type)
    sql += " ORDER BY timestamp_utc DESC, id DESC LIMIT ?"
    params.append(limit)
    return [
        row_to_event(row, include_payload=include_payload)
        for row in connection.execute(sql, params).fetchall()
    ]


def counts_for(connection, column, where_sql="", params=()):
    sql = f"SELECT {column} AS key, COUNT(*) AS count FROM telemetry_events"
    if where_sql:
        sql += f" WHERE {where_sql}"
    sql += f" GROUP BY {column} ORDER BY count DESC, key ASC"
    return [
        {"key": row["key"] if row["key"] is not None else "null", "count": row["count"]}
        for row in connection.execute(sql, params).fetchall()
    ]


def parse_trade_record(row):
    payload = parse_payload_json(row["payload_json"])
    if payload.get("event_schema_version") != "trade_closed.v2":
        return None
    session_context = payload.get("session_context")
    if not isinstance(session_context, dict):
        session_context = {}
    session = (
        session_context.get("session_name")
        or payload.get("session_name")
        or session_context.get("session_profile")
        or payload.get("session_profile")
        or "unknown"
    )
    entry_slippage = safe_float(payload.get("entry_slippage"))
    exit_slippage = safe_float(payload.get("exit_slippage"))
    planned_r_points = safe_float(payload.get("planned_r_points"))
    slippage_values = [value for value in (entry_slippage, exit_slippage) if value is not None]
    slippage_impact_r = None
    if planned_r_points is not None and planned_r_points > 0 and slippage_values:
        slippage_impact_r = sum(slippage_values) / planned_r_points
    exit_reason = normalize_text(row["reason"] or payload.get("exit_reason"), default="")
    emergency_flatten_qty = safe_float(payload.get("emergency_flatten_exit_quantity"))
    return {
        "timestamp_utc": row["timestamp_utc"],
        "symbol": normalize_text(row["symbol"] or payload.get("symbol")),
        "grade": normalize_text(payload.get("grade")),
        "strategy_family": normalize_text(payload.get("strategy_family")),
        "session": normalize_text(session),
        "side": normalize_text(payload.get("side")),
        "analysis_ready": payload.get("analysis_ready") is True,
        "net_pnl": safe_float(payload.get("net_pnl")),
        "net_r": safe_float(payload.get("net_r")),
        "slippage_impact_r": slippage_impact_r,
        "time_exit": exit_reason == TIME_EXIT_REASON,
        "emergency_flatten": (
            (emergency_flatten_qty is not None and emergency_flatten_qty > 0)
            or "EMERGENCY_FLATTEN" in exit_reason.upper()
        ),
    }


def load_closed_trade_records(connection):
    rows = connection.execute(
        """
        SELECT timestamp_utc, symbol, reason, payload_json
        FROM telemetry_events
        WHERE event_type = ?
        ORDER BY timestamp_utc ASC, id ASC
        """,
        ("TRADE_CLOSED",),
    ).fetchall()
    records = []
    for row in rows:
        record = parse_trade_record(row)
        if record is not None:
            records.append(record)
    return records


def performance_metrics(records):
    sample_size = len(records)
    analysis_ready_count = sum(1 for record in records if record.get("analysis_ready"))
    net_pnl_values = [record["net_pnl"] for record in records if record.get("net_pnl") is not None]
    net_r_values = [record["net_r"] for record in records if record.get("net_r") is not None]
    positive_pnl = [value for value in net_pnl_values if value > 0]
    negative_pnl = [value for value in net_pnl_values if value < 0]
    profit_factor = None
    profit_factor_reason = None
    if positive_pnl and negative_pnl:
        profit_factor = sum(positive_pnl) / abs(sum(negative_pnl))
    elif positive_pnl and not negative_pnl:
        profit_factor_reason = "no_losses"
    elif negative_pnl and not positive_pnl:
        profit_factor = 0.0
    else:
        profit_factor_reason = "no_wins_or_losses"
    slippage_values = [record.get("slippage_impact_r") for record in records]
    return {
        "closed_trades_count": sample_size,
        "analysis_ready_count": analysis_ready_count,
        "net_pnl_total": sum(net_pnl_values) if net_pnl_values else None,
        "expectancy_r": average(net_r_values),
        "winrate": (
            len([value for value in net_r_values if value > 0]) / len(net_r_values)
            if net_r_values else None
        ),
        "profit_factor": profit_factor,
        "profit_factor_reason": profit_factor_reason,
        "avg_slippage_impact_r": average(slippage_values),
    }


def performance_buckets(records):
    bucket_fields = ("symbol", "grade", "strategy_family", "session", "side")
    buckets = {}
    for record in records:
        key = tuple(record.get(field, "unknown") for field in bucket_fields)
        buckets.setdefault(key, []).append(record)
    bucket_rows = []
    for key, bucket_records in buckets.items():
        metrics = performance_metrics(bucket_records)
        bucket_rows.append({
            "bucket": "|".join(key),
            **metrics,
        })
    return sorted(bucket_rows, key=lambda item: (-(item.get("closed_trades_count") or 0), item["bucket"]))


def event_decision_payloads(connection, event_type, limit=None):
    sql = """
        SELECT id, timestamp_utc, symbol, signal_id, state, reason, payload_json
        FROM telemetry_events
        WHERE event_type = ?
        ORDER BY timestamp_utc DESC, id DESC
    """
    params = [event_type]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    rows = connection.execute(sql, params).fetchall()
    events = []
    for row in rows:
        payload = parse_payload_json(row["payload_json"])
        events.append({
            "id": row["id"],
            "timestamp_utc": row["timestamp_utc"],
            "symbol": row["symbol"] or payload.get("symbol"),
            "signal_id": row["signal_id"] or payload.get("signal_id"),
            "state": row["state"],
            "reason": row["reason"] or payload.get("reason"),
            "payload": payload,
        })
    return events


def count_payload_field(events, field):
    counts = {}
    for event in events:
        value = event["payload"].get(field)
        if value is None:
            value = "null"
        value = str(value)
        counts[value] = counts.get(value, 0) + 1
    return [{"key": key, "count": count} for key, count in sorted(counts.items())]


def reason_counts(events):
    counts = {}
    for event in events:
        reason = event.get("reason") or "unknown"
        counts[reason] = counts.get(reason, 0) + 1
    return [{"key": key, "count": count} for key, count in sorted(counts.items())]


def compact_decision_events(events, include_payload=False):
    compact = []
    for event in events:
        payload = event["payload"]
        compact.append({
            "id": event["id"],
            "timestamp_utc": event["timestamp_utc"],
            "symbol": event["symbol"],
            "signal_id": event["signal_id"],
            "state": event["state"],
            "reason": event["reason"],
            "payload_summary": payload_summary(payload, include_payload=include_payload),
        })
    return compact


def find_latest_version_patch(connection):
    rows = connection.execute(
        """
        SELECT payload_json
        FROM telemetry_events
        WHERE payload_json IS NOT NULL
        ORDER BY timestamp_utc DESC, id DESC
        LIMIT 500
        """
    ).fetchall()
    for row in rows:
        payload = parse_payload_json(row["payload_json"])
        candidates = [payload]
        if isinstance(payload.get("extra"), dict):
            candidates.append(payload["extra"])
        for candidate in candidates:
            if candidate.get("bot_version") or candidate.get("bot_patch"):
                return {
                    "bot_version": candidate.get("bot_version"),
                    "bot_patch": candidate.get("bot_patch"),
                }
    return {"bot_version": None, "bot_patch": None}


def build_summary(connection):
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
    return {
        "event_count": row["event_count"],
        "oldest_event": row["oldest_event"],
        "newest_event": row["newest_event"],
        "event_type_count": row["event_type_count"],
        "trade_count": row["trade_count"],
        "signal_count": row["signal_count"],
        "event_type_counts": counts_for(connection, "event_type"),
        "bot_stage_distribution": counts_for(connection, "bot_stage"),
        "latest_version": find_latest_version_patch(connection),
    }


def build_performance(connection):
    records = load_closed_trade_records(connection)
    return {
        "metrics": performance_metrics(records),
        "buckets": performance_buckets(records),
    }


def build_edge(connection):
    events = event_decision_payloads(connection, "EDGE_GATE_DECISION")
    watched_reasons = {"edge_negative_observed", "degradation_warning", "slippage_warning"}
    return {
        "latest": compact_decision_events(events[:20]),
        "reason_counts": reason_counts(events),
        "would_allow_distribution": count_payload_field(events, "would_allow"),
        "warning_buckets": [
            {
                "timestamp_utc": event["timestamp_utc"],
                "symbol": event["symbol"],
                "reason": event["reason"],
                "bucket": event["payload"].get("bucket_used"),
                "sample_size": event["payload"].get("sample_size"),
                "expectancy_r": event["payload"].get("expectancy_r"),
            }
            for event in events
            if event.get("reason") in watched_reasons
        ][:50],
    }


def build_slippage(connection):
    events = event_decision_payloads(connection, "SLIPPAGE_DEFENSE_DECISION")
    warnings = {"stale_signal_warning", "entry_drift_warning", "historical_slippage_warning"}
    return {
        "latest": compact_decision_events(events[:20]),
        "reason_counts": reason_counts(events),
        "would_allow_distribution": count_payload_field(events, "would_allow"),
        "warnings": [
            {
                "timestamp_utc": event["timestamp_utc"],
                "symbol": event["symbol"],
                "phase": event["payload"].get("phase"),
                "reason": event["reason"],
                "signal_age_sec": event["payload"].get("signal_age_sec"),
                "entry_drift_r": event["payload"].get("entry_drift_r"),
                "historical_avg_slippage_impact_r": event["payload"].get("historical_avg_slippage_impact_r"),
            }
            for event in events
            if event.get("reason") in warnings
        ][:50],
    }


def build_learning(connection):
    events = event_decision_payloads(connection, "TRADE_LEARNING_ADVICE")
    recommendation_counts = {}
    for event in events:
        recommendations = event["payload"].get("recommendations")
        if not isinstance(recommendations, list):
            recommendations = []
        for recommendation in recommendations:
            recommendation_counts[str(recommendation)] = recommendation_counts.get(str(recommendation), 0) + 1
    return {
        "latest": compact_decision_events(events[:20]),
        "reason_counts": reason_counts(events),
        "confidence_distribution": count_payload_field(events, "confidence"),
        "recommendation_counts": [
            {"key": key, "count": count}
            for key, count in sorted(recommendation_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
    }


def build_quality_gate(connection):
    events = event_decision_payloads(connection, "EXECUTION_QUALITY_GATE_DECISION")
    block_source_counts = {}
    for event in events:
        block_sources = event["payload"].get("block_sources")
        if not isinstance(block_sources, list):
            block_sources = []
        for block_source in block_sources:
            block_source_counts[str(block_source)] = block_source_counts.get(str(block_source), 0) + 1
    return {
        "latest": compact_decision_events(events[:20]),
        "reason_counts": reason_counts(events),
        "mode_distribution": count_payload_field(events, "mode"),
        "enforcement_active_distribution": count_payload_field(events, "enforcement_active"),
        "would_block_distribution": count_payload_field(events, "would_block"),
        "blocked_distribution": count_payload_field(events, "blocked"),
        "block_source_counts": [
            {"key": key, "count": count}
            for key, count in sorted(block_source_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
    }


def build_recent_events(connection, query):
    event_type = (query.get("event_type") or [""])[0].strip()
    limit = parse_int((query.get("limit") or [20])[0], default=20, minimum=1, maximum=200)
    include_payload = parse_bool((query.get("payload") or ["false"])[0])
    return {
        "event_type": event_type or None,
        "limit": limit,
        "include_payload": include_payload,
        "events": fetch_events(
            connection,
            event_type=event_type or None,
            limit=limit,
            include_payload=include_payload,
        ),
    }


def build_health(db_path):
    path = Path(db_path).expanduser()
    result = {
        "ok": False,
        "db_path": str(path),
        "db_exists": path.exists(),
        "db_is_file": path.is_file() if path.exists() else False,
    }
    if not path.exists() or not path.is_file():
        result["error"] = "database_not_found" if not path.exists() else "database_path_not_file"
        return result
    connection_result = with_connection(path, build_summary)
    result["ok"] = connection_result.get("ok") is True
    if connection_result.get("ok"):
        summary = connection_result["data"]
        result["event_count"] = summary.get("event_count")
        result["newest_event"] = summary.get("newest_event")
    else:
        result["error"] = connection_result.get("error")
    return result


def json_response(handler, payload, status=200):
    body = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler, body_text, status=200, content_type="text/html; charset=utf-8"):
    body = body_text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def check_token(handler, query):
    token = handler.server.dashboard_config.get("token")
    if not token:
        return True
    auth_header = handler.headers.get("Authorization", "")
    supplied = None
    if auth_header.startswith("Bearer "):
        supplied = auth_header[len("Bearer "):].strip()
    supplied = supplied or handler.headers.get("X-Dashboard-Token")
    supplied = supplied or (query.get("token") or [None])[0]
    return supplied == token


def dashboard_html(refresh_seconds):
    refresh_seconds = parse_int(refresh_seconds, DEFAULT_REFRESH_SECONDS, minimum=5, maximum=3600)
    escaped_refresh = html.escape(str(refresh_seconds))
    template = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>IKBR Telemetry Dashboard</title>
  <style>
    :root {
      --bg: #f5f7f8;
      --panel: #ffffff;
      --text: #182026;
      --muted: #63717a;
      --line: #dce3e7;
      --green: #177245;
      --yellow: #8a6500;
      --red: #aa272f;
    }
    body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 Arial, sans-serif; }
    header { padding: 14px 18px; background: #172126; color: white; display: flex; justify-content: space-between; gap: 16px; align-items: center; }
    h1 { font-size: 18px; margin: 0; font-weight: 700; }
    main { padding: 16px; max-width: 1500px; margin: 0 auto; }
    section { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; margin-bottom: 14px; padding: 14px; }
    h2 { margin: 0 0 10px; font-size: 16px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 10px; }
    .metric { border: 1px solid var(--line); border-radius: 6px; padding: 10px; background: #fbfcfd; }
    .metric .label { color: var(--muted); font-size: 12px; }
    .metric .value { font-size: 19px; font-weight: 700; margin-top: 3px; overflow-wrap: anywhere; }
    table { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 13px; }
    th, td { padding: 7px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th { color: var(--muted); font-weight: 700; background: #f8fafb; }
    .status-clear { color: var(--green); font-weight: 700; }
    .status-warning { color: var(--yellow); font-weight: 700; }
    .status-bad { color: var(--red); font-weight: 700; }
    .muted { color: var(--muted); }
    .error { color: var(--red); font-weight: 700; }
    .toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 10px; }
    input, select, button { font: inherit; padding: 6px 8px; border: 1px solid var(--line); border-radius: 6px; background: white; }
    button { cursor: pointer; }
    pre { white-space: pre-wrap; overflow-wrap: anywhere; margin: 0; font-size: 12px; }
  </style>
</head>
<body>
  <header>
    <h1>IKBR Telemetry Dashboard</h1>
    <div class="muted" id="refreshNote">refresh __REFRESH_NOTE__s</div>
  </header>
  <main>
    <section>
      <div class="toolbar">
        <button onclick="loadAll()">Refresh</button>
        <span id="health" class="muted">loading</span>
      </div>
      <div id="overview" class="grid"></div>
    </section>
    <section><h2>Performance</h2><div id="performanceMetrics" class="grid"></div><div id="performanceBuckets"></div></section>
    <section><h2>Edge</h2><div id="edge"></div></section>
    <section><h2>Slippage</h2><div id="slippage"></div></section>
    <section><h2>Learning</h2><div id="learning"></div></section>
    <section><h2>Quality Gate</h2><div id="qualityGate"></div></section>
    <section>
      <h2>Recent Events</h2>
      <div class="toolbar">
        <input id="eventType" placeholder="event_type filter">
        <input id="limit" type="number" value="20" min="1" max="200">
        <label><input id="payload" type="checkbox"> redacted payload</label>
        <button onclick="loadRecent()">Load</button>
      </div>
      <div id="recent"></div>
    </section>
  </main>
<script>
const refreshSeconds = __REFRESH_SECONDS__;
const tokenParam = new URLSearchParams(location.search).get("token");
function apiUrl(path, params) {
  const url = new URL(path, location.origin);
  if (params) Object.entries(params).forEach(([k, v]) => { if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, v); });
  if (tokenParam) url.searchParams.set("token", tokenParam);
  return url;
}
async function fetchJson(path, params) {
  const response = await fetch(apiUrl(path, params));
  const data = await response.json();
  if (!response.ok || data.ok === false) throw new Error(data.error || response.statusText);
  return data.data || data;
}
function fmt(value) {
  if (value === null || value === undefined || value === "") return "";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(4);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
function cls(value) {
  const text = fmt(value).toLowerCase();
  if (text.includes("blocked") || text.includes("negative") || text.includes("warning") || text === "false") return "status-bad";
  if (text.includes("shadow") || text.includes("insufficient") || text.includes("neutral")) return "status-warning";
  if (text.includes("clear") || text.includes("positive") || text === "true") return "status-clear";
  return "";
}
function metrics(target, items) {
  document.getElementById(target).innerHTML = items.map(([label, value]) => `<div class="metric"><div class="label">${label}</div><div class="value ${cls(value)}">${fmt(value)}</div></div>`).join("");
}
function table(rows, columns) {
  if (!rows || !rows.length) return '<p class="muted">No rows</p>';
  const head = columns.map(c => `<th>${c[0]}</th>`).join("");
  const body = rows.map(row => `<tr>${columns.map(c => `<td class="${cls(row[c[1]])}">${fmt(row[c[1]])}</td>`).join("")}</tr>`).join("");
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}
function decisionBlock(data, columns) {
  return table(data.latest || [], columns) +
    '<h3>Reasons</h3>' + table(data.reason_counts || [], [['Reason','key'], ['Count','count']]);
}
async function loadAll() {
  await loadHealth();
  await Promise.all([loadSummary(), loadPerformance(), loadEdge(), loadSlippage(), loadLearning(), loadQualityGate(), loadRecent()]);
}
async function loadHealth() {
  try {
    const data = await fetchJson('/health');
    document.getElementById('health').textContent = data.ok ? `healthy events=${fmt(data.event_count)} newest=${fmt(data.newest_event)}` : `health error: ${data.error}`;
    document.getElementById('health').className = data.ok ? 'status-clear' : 'error';
  } catch (error) {
    document.getElementById('health').textContent = error.message;
    document.getElementById('health').className = 'error';
  }
}
async function loadSummary() {
  const data = await fetchJson('/api/summary');
  metrics('overview', [
    ['Events', data.event_count],
    ['Trades', data.trade_count],
    ['Signals', data.signal_count],
    ['Newest', data.newest_event],
    ['Event Types', data.event_type_count],
    ['Bot Patch', data.latest_version && data.latest_version.bot_patch],
  ]);
}
async function loadPerformance() {
  const data = await fetchJson('/api/performance');
  const m = data.metrics || {};
  metrics('performanceMetrics', [
    ['Closed Trades', m.closed_trades_count],
    ['Analysis Ready', m.analysis_ready_count],
    ['Net PnL', m.net_pnl_total],
    ['Expectancy R', m.expectancy_r],
    ['Winrate', m.winrate],
    ['Profit Factor', m.profit_factor],
    ['Avg Slippage R', m.avg_slippage_impact_r],
  ]);
  document.getElementById('performanceBuckets').innerHTML = table(data.buckets || [], [['Bucket','bucket'], ['Trades','closed_trades_count'], ['Net PnL','net_pnl_total'], ['Exp R','expectancy_r'], ['Winrate','winrate'], ['Slip R','avg_slippage_impact_r']]);
}
async function loadEdge() {
  const data = await fetchJson('/api/edge');
  document.getElementById('edge').innerHTML = decisionBlock(data, [['Time','timestamp_utc'], ['Symbol','symbol'], ['Reason','reason'], ['Summary','payload_summary']]) +
    '<h3>would_allow</h3>' + table(data.would_allow_distribution || [], [['Value','key'], ['Count','count']]) +
    '<h3>Warning Buckets</h3>' + table(data.warning_buckets || [], [['Time','timestamp_utc'], ['Symbol','symbol'], ['Reason','reason'], ['Bucket','bucket'], ['Sample','sample_size'], ['Exp R','expectancy_r']]);
}
async function loadSlippage() {
  const data = await fetchJson('/api/slippage');
  document.getElementById('slippage').innerHTML = decisionBlock(data, [['Time','timestamp_utc'], ['Symbol','symbol'], ['Reason','reason'], ['Summary','payload_summary']]) +
    '<h3>Warnings</h3>' + table(data.warnings || [], [['Time','timestamp_utc'], ['Symbol','symbol'], ['Phase','phase'], ['Reason','reason'], ['Signal Age','signal_age_sec'], ['Drift R','entry_drift_r'], ['Hist Slip R','historical_avg_slippage_impact_r']]);
}
async function loadLearning() {
  const data = await fetchJson('/api/learning');
  document.getElementById('learning').innerHTML = decisionBlock(data, [['Time','timestamp_utc'], ['Symbol','symbol'], ['Reason','reason'], ['Summary','payload_summary']]) +
    '<h3>Confidence</h3>' + table(data.confidence_distribution || [], [['Value','key'], ['Count','count']]) +
    '<h3>Recommendations</h3>' + table(data.recommendation_counts || [], [['Recommendation','key'], ['Count','count']]);
}
async function loadQualityGate() {
  const data = await fetchJson('/api/quality-gate');
  document.getElementById('qualityGate').innerHTML = decisionBlock(data, [['Time','timestamp_utc'], ['Symbol','symbol'], ['Reason','reason'], ['Summary','payload_summary']]) +
    '<h3>Mode</h3>' + table(data.mode_distribution || [], [['Mode','key'], ['Count','count']]) +
    '<h3>Block Sources</h3>' + table(data.block_source_counts || [], [['Source','key'], ['Count','count']]);
}
async function loadRecent() {
  const params = { event_type: document.getElementById('eventType').value, limit: document.getElementById('limit').value, payload: document.getElementById('payload').checked };
  const data = await fetchJson('/api/recent-events', params);
  document.getElementById('recent').innerHTML = table(data.events || [], [['Time','timestamp_utc'], ['Type','event_type'], ['Severity','severity'], ['Symbol','symbol'], ['Signal','signal_id'], ['Reason','reason'], ['Payload','payload_summary']]);
}
loadAll();
if (refreshSeconds > 0) setInterval(loadAll, refreshSeconds * 1000);
</script>
</body>
</html>"""
    return (
        template
        .replace("__REFRESH_SECONDS__", str(refresh_seconds))
        .replace("__REFRESH_NOTE__", escaped_refresh)
    )


class TelemetryDashboardHandler(BaseHTTPRequestHandler):
    server_version = "IKBRTelemetryDashboard/1.0"

    def log_message(self, format_text, *args):
        logger.info("%s - %s", self.address_string(), format_text % args)

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if not check_token(self, query):
            json_response(self, {"ok": False, "error": "unauthorized"}, status=401)
            return

        db_path = self.server.dashboard_config["db_path"]
        path = parsed.path.rstrip("/") or "/"
        if path == "/":
            text_response(self, dashboard_html(self.server.dashboard_config["refresh_seconds"]))
            return
        if path == "/health":
            payload = build_health(db_path)
            json_response(self, payload, status=200 if payload.get("ok") else 503)
            return

        endpoint_builders = {
            "/api/summary": build_summary,
            "/api/performance": build_performance,
            "/api/edge": build_edge,
            "/api/slippage": build_slippage,
            "/api/learning": build_learning,
            "/api/quality-gate": build_quality_gate,
        }
        if path in endpoint_builders:
            result = with_connection(db_path, endpoint_builders[path])
            json_response(self, result, status=200 if result.get("ok") else 503)
            return
        if path == "/api/recent-events":
            result = with_connection(db_path, lambda connection: build_recent_events(connection, query))
            json_response(self, result, status=200 if result.get("ok") else 503)
            return

        json_response(self, {"ok": False, "error": "not_found"}, status=404)


def build_server(host, port, db_path, token=None, refresh_seconds=DEFAULT_REFRESH_SECONDS):
    server = ThreadingHTTPServer((host, port), TelemetryDashboardHandler)
    server.dashboard_config = {
        "db_path": Path(db_path).expanduser(),
        "token": token,
        "refresh_seconds": parse_int(refresh_seconds, DEFAULT_REFRESH_SECONDS, minimum=0, maximum=3600),
    }
    return server


def main():
    logging.basicConfig(level=logging.INFO)
    db_path = resolve_db_path()
    host = os.getenv("IKBR_DASHBOARD_HOST", DEFAULT_HOST).strip() or DEFAULT_HOST
    port = parse_int(os.getenv("IKBR_DASHBOARD_PORT"), DEFAULT_PORT, minimum=1, maximum=65535)
    refresh_seconds = parse_int(
        os.getenv("IKBR_DASHBOARD_REFRESH_SECONDS"),
        DEFAULT_REFRESH_SECONDS,
        minimum=0,
        maximum=3600,
    )
    token = os.getenv("IKBR_DASHBOARD_TOKEN")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        logger.warning(
            "DASHBOARD_EXTERNAL_BIND_WARNING | host=%s token_required=%s",
            host,
            bool(token),
        )
    server = build_server(host, port, db_path, token=token, refresh_seconds=refresh_seconds)
    params = urlencode({"token": "<token>"}) if token else ""
    suffix = f"?{params}" if params else ""
    logger.info("Telemetry dashboard listening on http://%s:%s/%s db_path=%s", host, port, suffix, db_path)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Telemetry dashboard stopping")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
