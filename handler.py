import json
import os
import re
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from pydbzengine import BasePythonChangeHandler

from logger import get_logger

logger = get_logger(__name__)

# ── Topic prefix → data‑warehouse table prefix ──────────────────────────────
TOPIC_PREFIX_MAP = {
    "cdc_1": "admin_",
    "cdc_2": "auth_",
    "cdc_3": "engine_",
    "cdc_4": "user_",
    "cdc_5": "user_betting_",
    "cdc_6": "wallet_",
}

# ── Warehouse connection pool (lazily initialised) ───────────────────────────
_connection_pool = None


def _ensure_pool():
    """Return (or create) a thread‑safe connection pool to the data warehouse."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            dbname=os.getenv("DB_NAME", "kla-klouk-data-warehouse"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "password"),
        )
        logger.info("Warehouse pool ready (db=%s)", os.getenv("DB_NAME"))
    return _connection_pool


@contextmanager
def _get_connection():
    """Yield a connection from the pool; always returns it on exit."""
    pool_obj = _ensure_pool()
    conn = pool_obj.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool_obj.putconn(conn)


# ── Helpers ──────────────────────────────────────────────────────────────────

# Regex matching ISO‑8601 / RFC‑3339 timestamps (rough but good enough for detection)
_ISO_DATETIME_RE = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"


def _sanitize_value(val):
    """Replace datetime strings with an invalid year (e.g. '0000') with None.

    PostgreSQL cannot store year 0000 (or any year < 1).  Debezium may emit
    such values when the source MySQL / MariaDB table contains a "zero date"
    (0000-00-00 or 0000-12-30).
    """
    if isinstance(val, str) and re.match(_ISO_DATETIME_RE, val):
        # Extract the year portion; if it's "0000" the value is invalid for PG
        year = val[:4]
        if year == "0000":
            return None
    return val


def _sanitize_record(record: dict | None) -> dict | None:
    """Return a new dict with all datetime values that have year 0000 set to None."""
    if record is None:
        return None
    return {k: _sanitize_value(v) for k, v in record.items()}


def _safe_parse_json(value):
    """Return a dict from *value*, which may be dict, JSON string, or None."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
    return None


def _parse_topic(topic: str):
    """Split 'cdc_1.public.announcement_details' → (prefix, schema, table).

    Ensures topic is a native Python str first to avoid JPype/Java
    String.split() which interprets '.' as a regex (any character).
    """
    topic = str(topic)
    parts = topic.split(".", 2)
    if len(parts) != 3:
        raise ValueError(f"Unexpected topic format: {topic!r}")
    return parts[0], parts[1], parts[2]


def _map_table(topic_prefix: str, source_table: str) -> str:
    """Map source table name to the data‑warehouse table name.

    Example: ('cdc_1', 'announcement_details') → 'admin_announcement_details'
    """
    prefix = TOPIC_PREFIX_MAP.get(topic_prefix)
    if prefix is None:
        raise ValueError(f"Unknown topic prefix: {topic_prefix!r}")
    return f"{prefix}{source_table}"


# ── Tables with composite unique keys (no 'id' column) ──────────────────────
TABLE_UNIQUE_KEYS: dict[str, list[str]] = {
    "admin_game_game_types": ["game_id", "game_type_id"],
    "auth_role_permissions": ["role_id", "permission_id"],
}


def _get_unique_key_columns(table: str) -> list[str] | None:
    """Return the unique-key column list for tables that lack an 'id' column."""
    return TABLE_UNIQUE_KEYS.get(table)


# ── SQL operations ───────────────────────────────────────────────────────────


def _execute_insert(conn, table: str, after: dict):
    """Insert a row; skip silently if a matching row already exists.

    Uses the "id" column when present, otherwise falls back to the
    table-specific unique-key columns defined in TABLE_UNIQUE_KEYS.
    """
    record_id = after.get("id")
    unique_keys = _get_unique_key_columns(table)

    if record_id is None and unique_keys is None:
        logger.warning("Skipping INSERT %s — no unique key columns", table)
        return

    columns = list(after.keys())
    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join("%s" for _ in columns)
    values = [after[c] for c in columns]

    if unique_keys:
        conflict_target = ", ".join(f'"{k}"' for k in unique_keys)
        conflict_clause = f'ON CONFLICT ({conflict_target}) DO NOTHING'
        pk_display = ",".join(str(after[k]) for k in unique_keys)
    else:
        conflict_clause = 'ON CONFLICT DO NOTHING'
        pk_display = str(record_id)

    sql = (
        f'INSERT INTO "{table}" ({col_list}) '
        f"VALUES ({placeholders}) "
        f"{conflict_clause}"
    )
    with conn.cursor() as cur:
        cur.execute(sql, values)
    logger.info("INSERT %s pk=%s (OK)", table, pk_display)


def _execute_update(conn, table: str, after: dict):
    """Update a row by its unique key.  Falls back to INSERT if the row doesn't exist.

    Uses the "id" column when present, otherwise falls back to the
    table-specific unique-key columns defined in TABLE_UNIQUE_KEYS.
    """
    record_id = after.get("id")
    unique_keys = _get_unique_key_columns(table)

    if record_id is None and unique_keys is None:
        logger.warning("Skipping UPDATE %s — no unique key columns", table)
        return

    if unique_keys:
        pk_columns = unique_keys
    else:
        pk_columns = ["id"]

    non_pk = [c for c in after if c not in pk_columns]
    if not non_pk:
        logger.info("Skipping UPDATE %s — only PK columns present", table)
        return

    where_clause = " AND ".join(f'"{c}" = %s' for c in pk_columns)
    set_clause = ", ".join(f'"{c}" = %s' for c in non_pk)
    values = [after[c] for c in non_pk] + [after[c] for c in pk_columns]

    pk_display = str(record_id) if not unique_keys else ",".join(
        str(after[c]) for c in unique_keys
    )

    sql = f'UPDATE "{table}" SET {set_clause} WHERE {where_clause}'
    with conn.cursor() as cur:
        cur.execute(sql, values)
        if cur.rowcount == 0:
            logger.info("UPDATE %s pk=%s → no row, trying INSERT", table, pk_display)
            _execute_insert(conn, table, after)
        else:
            logger.info("UPDATE %s pk=%s (%d row(s))", table, pk_display, cur.rowcount)


def _execute_delete(conn, table: str, before: dict, after: dict):
    """Delete a row by its unique key (id read from after, then before).

    Uses the "id" column when present, otherwise falls back to the
    table-specific unique-key columns defined in TABLE_UNIQUE_KEYS.
    """
    record = after or before or {}
    unique_keys = _get_unique_key_columns(table)

    if unique_keys:
        pk_columns = unique_keys
    else:
        record_id = record.get("id")
        if record_id is None:
            logger.warning("Skipping DELETE %s — no 'id' field", table)
            return
        pk_columns = None

    if pk_columns:
        # Use composite unique key
        missing = [k for k in pk_columns if k not in record]
        if missing:
            logger.warning(
                "Skipping DELETE %s — missing key column(s): %s", table, missing
            )
            return
        where_clause = " AND ".join(f'"{c}" = %s' for c in pk_columns)
        values = [record[c] for c in pk_columns]
        sql = f'DELETE FROM "{table}" WHERE {where_clause}'
        record_pk = ",".join(str(record[c]) for c in pk_columns)
    else:
        sql = f'DELETE FROM "{table}" WHERE "id" = %s'
        values = (record_id,)
        record_pk = str(record_id)

    with conn.cursor() as cur:
        cur.execute(sql, values)
        logger.info("DELETE %s pk=%s (%d row(s))", table, record_pk, cur.rowcount)


# ── Handler class ────────────────────────────────────────────────────────────


class CDCHandler(BasePythonChangeHandler):

    def __init__(self, db_label: str = "unknown"):
        self.db_label = db_label
        self.logger = get_logger(f"handler.{db_label}")

    def handleJsonBatch(self, records):
        self.logger.info("Received %d change events", len(records))

        for record in records:
            topic = record.sourceRecord().topic()
            value_str = str(record.value())

            try:
                event = json.loads(value_str)
            except json.JSONDecodeError:
                self.logger.error("Invalid JSON on topic %s", topic)
                continue

            payload = event.get("payload", {})

            op = payload.get("op")
            before = _sanitize_record(_safe_parse_json(payload.get("before")))
            after = _sanitize_record(_safe_parse_json(payload.get("after")))

            self.logger.info(
                "Topic: %s | Op: %s | After id=%s",
                topic,
                op,
                after.get("id") if after else None,
            )

            # Resolve target table
            try:
                topic_prefix, schema_name, source_table = _parse_topic(topic)
                target_table = _map_table(topic_prefix, source_table)
            except (ValueError, KeyError) as exc:
                self.logger.error(
                    "Skipping topic=%r — %s", topic, exc, exc_info=True
                )
                continue

            try:
                with _get_connection() as conn:
                    if op in ("c", "r"):
                        _execute_insert(conn, target_table, after)
                    elif op == "u":
                        _execute_update(conn, target_table, after)
                    elif op == "d":
                        _execute_delete(conn, target_table, before, after)
                    else:
                        self.logger.warning("Unknown op '%s' on %s", op, topic)
            except Exception as exc:
                self.logger.error(
                    "Failed %s on %s: %s", op, target_table, exc, exc_info=True
                )

    # ── Aliases for possible method name variants ──────────────────────────
    def handle_batch(self, records):
        """Python‑style alias, delegates to the main entry‑point."""
        return self.handleJsonBatch(records)
