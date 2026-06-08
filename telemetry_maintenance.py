import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_DB_PATH = Path("runtime_state") / "telemetry_events.sqlite3"
DEFAULT_BACKUP_DIR = Path("runtime_state") / "backups"
SQLITE_TIMEOUT_SECONDS = 5.0


class MaintenanceError(Exception):
    pass


def resolve_db_path(path_text=None):
    return Path(path_text or os.getenv("IKBR_TELEMETRY_DB_PATH") or DEFAULT_DB_PATH).expanduser()


def resolve_backup_dir(path_text=None):
    return Path(path_text or os.getenv("IKBR_TELEMETRY_BACKUP_DIR") or DEFAULT_BACKUP_DIR).expanduser()


def file_size(path):
    try:
        path = Path(path)
        return path.stat().st_size if path.exists() and path.is_file() else 0
    except Exception:
        return 0


def db_related_sizes(db_path):
    path = Path(db_path)
    return {
        "db_size_bytes": file_size(path),
        "wal_size_bytes": file_size(Path(str(path) + "-wal")),
        "shm_size_bytes": file_size(Path(str(path) + "-shm")),
    }


def open_read_only(db_path):
    path = Path(db_path).expanduser()
    if not path.exists():
        raise MaintenanceError(f"database_not_found: {path}")
    if not path.is_file():
        raise MaintenanceError(f"database_path_not_file: {path}")
    uri = path.resolve().as_uri() + "?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=SQLITE_TIMEOUT_SECONDS)
    except sqlite3.Error as exc:
        raise MaintenanceError(f"database_open_failed: {exc}") from exc
    connection.row_factory = sqlite3.Row
    return connection


def open_read_write(db_path):
    path = Path(db_path).expanduser()
    if not path.exists():
        raise MaintenanceError(f"database_not_found: {path}")
    if not path.is_file():
        raise MaintenanceError(f"database_path_not_file: {path}")
    try:
        connection = sqlite3.connect(path, timeout=SQLITE_TIMEOUT_SECONDS)
    except sqlite3.Error as exc:
        raise MaintenanceError(f"database_open_failed: {exc}") from exc
    connection.row_factory = sqlite3.Row
    return connection


def table_present(connection, table_name):
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = ? AND name = ?",
        ("table", table_name),
    ).fetchone()
    return row is not None


def scalar(connection, sql, params=(), default=None):
    row = connection.execute(sql, params).fetchone()
    if row is None:
        return default
    return row[0]


def row_to_dict(row):
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def latest_matching_event(connection, where_sql, params=()):
    row = connection.execute(
        f"""
        SELECT id, timestamp_utc, event_type, severity, symbol, signal_id, state, reason
        FROM telemetry_events
        WHERE {where_sql}
        ORDER BY timestamp_utc DESC, id DESC
        LIMIT 1
        """,
        params,
    ).fetchone()
    return row_to_dict(row)


def event_counts_by_type(connection):
    return [
        {"event_type": row["event_type"], "count": row["count"]}
        for row in connection.execute(
            """
            SELECT event_type, COUNT(*) AS count
            FROM telemetry_events
            GROUP BY event_type
            ORDER BY count DESC, event_type ASC
            """
        ).fetchall()
    ]


def bot_stage_distribution(connection):
    return [
        {"bot_stage": row["bot_stage"] if row["bot_stage"] is not None else "null", "count": row["count"]}
        for row in connection.execute(
            """
            SELECT bot_stage, COUNT(*) AS count
            FROM telemetry_events
            GROUP BY bot_stage
            ORDER BY count DESC, bot_stage ASC
            """
        ).fetchall()
    ]


def build_health(db_path):
    path = Path(db_path).expanduser()
    result = {
        "ok": False,
        "db_path": str(path),
        "db_exists": path.exists(),
        "telemetry_events_table_present": False,
        **db_related_sizes(path),
        "event_count": None,
        "oldest_event": None,
        "newest_event": None,
        "event_type_count": None,
        "latest_telemetry_write_failed_event": None,
        "latest_writer_error_event": None,
    }
    if not path.exists():
        result["error"] = "database_not_found"
        return result, 2
    if not path.is_file():
        result["error"] = "database_path_not_file"
        return result, 2

    connection = open_read_only(path)
    try:
        result["telemetry_events_table_present"] = table_present(connection, "telemetry_events")
        if not result["telemetry_events_table_present"]:
            result["error"] = "telemetry_events_table_not_found"
            return result, 2

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
        result["event_count"] = row["event_count"]
        result["oldest_event"] = row["oldest_event"]
        result["newest_event"] = row["newest_event"]
        result["event_type_count"] = row["event_type_count"]
        result["latest_telemetry_write_failed_event"] = latest_matching_event(
            connection,
            "event_type = ? OR reason LIKE ?",
            ("TELEMETRY_WRITE_FAILED", "%TELEMETRY_WRITE_FAILED%"),
        )
        result["latest_writer_error_event"] = latest_matching_event(
            connection,
            "lower(event_type) LIKE ? OR lower(reason) LIKE ? OR lower(payload_json) LIKE ?",
            ("%writer%", "%writer_error%", "%writer_error%"),
        )

        if result["event_count"] <= 0:
            result["ok"] = False
            result["warning"] = "telemetry_events_empty"
            return result, 1
        result["ok"] = True
        return result, 0
    finally:
        connection.close()


def build_stats(db_path):
    path = Path(db_path).expanduser()
    connection = open_read_only(path)
    try:
        if not table_present(connection, "telemetry_events"):
            raise MaintenanceError("telemetry_events_table_not_found")
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS event_count,
                MIN(timestamp_utc) AS oldest_event,
                MAX(timestamp_utc) AS newest_event
            FROM telemetry_events
            """
        ).fetchone()
        result = {
            "ok": True,
            "db_path": str(path),
            "event_count": row["event_count"],
            "event_counts_by_type": event_counts_by_type(connection),
            "bot_stage_distribution": bot_stage_distribution(connection),
            "oldest_event": row["oldest_event"],
            "newest_event": row["newest_event"],
            "closed_trade_count": scalar(
                connection,
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = ?",
                ("TRADE_CLOSED",),
                0,
            ),
            "edge_gate_decision_count": scalar(
                connection,
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = ?",
                ("EDGE_GATE_DECISION",),
                0,
            ),
            "slippage_defense_decision_count": scalar(
                connection,
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = ?",
                ("SLIPPAGE_DEFENSE_DECISION",),
                0,
            ),
            "trade_learning_advice_count": scalar(
                connection,
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = ?",
                ("TRADE_LEARNING_ADVICE",),
                0,
            ),
            "execution_quality_gate_decision_count": scalar(
                connection,
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = ?",
                ("EXECUTION_QUALITY_GATE_DECISION",),
                0,
            ),
            **db_related_sizes(path),
        }
        return result
    finally:
        connection.close()


def next_backup_path(backup_dir):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_name = f"telemetry_events_{timestamp}.sqlite3"
    candidate = backup_dir / base_name
    suffix = 1
    while candidate.exists():
        candidate = backup_dir / f"telemetry_events_{timestamp}_{suffix}.sqlite3"
        suffix += 1
    return candidate


def run_integrity_check(db_path):
    connection = open_read_only(db_path)
    try:
        row = connection.execute("PRAGMA integrity_check").fetchone()
        return row[0] if row is not None else "no_result"
    finally:
        connection.close()


def backup_database(db_path, backup_dir):
    source_path = Path(db_path).expanduser()
    if not source_path.exists():
        raise MaintenanceError(f"database_not_found: {source_path}")
    if not source_path.is_file():
        raise MaintenanceError(f"database_path_not_file: {source_path}")
    target_dir = Path(backup_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = next_backup_path(target_dir)

    source_connection = open_read_only(source_path)
    try:
        target_connection = sqlite3.connect(target_path, timeout=SQLITE_TIMEOUT_SECONDS)
        try:
            source_connection.backup(target_connection)
        finally:
            target_connection.close()
    finally:
        source_connection.close()

    integrity_status = run_integrity_check(target_path)
    if integrity_status != "ok":
        raise MaintenanceError(f"backup_integrity_check_failed: {integrity_status}")
    return {
        "ok": True,
        "db_path": str(source_path),
        "backup_path": str(target_path),
        "backup_size_bytes": file_size(target_path),
        "integrity_check": integrity_status,
    }


def vacuum_database(db_path, execute=False):
    path = Path(db_path).expanduser()
    sizes_before = db_related_sizes(path)
    result = {
        "ok": False,
        "db_path": str(path),
        "execute": bool(execute),
        "warning": "VACUUM can take write locks; stop the bot first when possible.",
        "size_before": sizes_before,
    }
    if not execute:
        result["dry_run"] = True
        result["message"] = "No changes made. Re-run with --execute to vacuum."
        return result, 1

    connection = open_read_write(path)
    try:
        connection.execute("VACUUM")
    finally:
        connection.close()
    result["ok"] = True
    result["dry_run"] = False
    result["size_after"] = db_related_sizes(path)
    return result, 0


def checkpoint_database(db_path, execute=False):
    path = Path(db_path).expanduser()
    result = {
        "ok": False,
        "db_path": str(path),
        "execute": bool(execute),
        "checkpoint_mode": "PASSIVE",
    }
    if not execute:
        result["dry_run"] = True
        result["message"] = "No changes made. Re-run with --execute to checkpoint WAL."
        return result, 1

    connection = open_read_write(path)
    try:
        row = connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        result["checkpoint_result"] = list(row) if row is not None else None
    finally:
        connection.close()
    result["ok"] = True
    result["dry_run"] = False
    result.update(db_related_sizes(path))
    return result, 0


def emit_result(result, json_output=False):
    if json_output:
        print(json.dumps(result, sort_keys=True, indent=2, default=str))
        return
    for key, value in result.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value, sort_keys=True, default=str)
        print(f"{key}={value}")


def build_parser():
    parser = argparse.ArgumentParser(description="Maintain IKBR telemetry SQLite database.")
    parser.add_argument("--db", help="Path to telemetry_events.sqlite3")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("health")
    subparsers.add_parser("stats")

    backup = subparsers.add_parser("backup")
    backup.add_argument("--backup-dir", help="Directory for backup files.")

    vacuum = subparsers.add_parser("vacuum")
    vacuum_group = vacuum.add_mutually_exclusive_group()
    vacuum_group.add_argument("--dry-run", action="store_true", default=True)
    vacuum_group.add_argument("--execute", action="store_true")

    checkpoint = subparsers.add_parser("checkpoint")
    checkpoint_group = checkpoint.add_mutually_exclusive_group()
    checkpoint_group.add_argument("--dry-run", action="store_true", default=True)
    checkpoint_group.add_argument("--execute", action="store_true")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    db_path = resolve_db_path(args.db)

    try:
        if args.command == "health":
            result, exit_code = build_health(db_path)
        elif args.command == "stats":
            result = build_stats(db_path)
            exit_code = 0
        elif args.command == "backup":
            result = backup_database(db_path, resolve_backup_dir(args.backup_dir))
            exit_code = 0
        elif args.command == "vacuum":
            result, exit_code = vacuum_database(db_path, execute=args.execute)
        elif args.command == "checkpoint":
            result, exit_code = checkpoint_database(db_path, execute=args.execute)
        else:
            parser.error(f"unknown command: {args.command}")
    except MaintenanceError as exc:
        result = {"ok": False, "error": str(exc), "db_path": str(db_path)}
        exit_code = 2
    except sqlite3.Error as exc:
        result = {"ok": False, "error": f"sqlite_error: {exc}", "db_path": str(db_path)}
        exit_code = 2
    except Exception as exc:
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}", "db_path": str(db_path)}
        exit_code = 2

    emit_result(result, json_output=args.json)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
