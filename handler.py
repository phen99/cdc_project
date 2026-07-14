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


# ── SQL operations ───────────────────────────────────────────────────────────


def _execute_insert(conn, table: str, after: dict):
    """Insert a row; skip silently if the id already exists."""
    record_id = after.get("id")
    if record_id is None:
        logger.warning("Skipping INSERT %s — no 'id' field", table)
        return

    columns = list(after.keys())
    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join("%s" for _ in columns)
    values = [after[c] for c in columns]

    sql = (
        f'INSERT INTO "{table}" ({col_list}) '
        f"VALUES ({placeholders}) "
        f'ON CONFLICT DO NOTHING'
    )
    with conn.cursor() as cur:
        cur.execute(sql, values)
    logger.info("INSERT %s id=%s (OK)", table, record_id)


def _execute_update(conn, table: str, after: dict):
    """Update a row by id.  Falls back to INSERT if the row doesn't exist yet."""
    record_id = after.get("id")
    if record_id is None:
        logger.warning("Skipping UPDATE %s — no 'id' field", table)
        return

    non_pk = [c for c in after if c != "id"]
    if not non_pk:
        logger.info("Skipping UPDATE %s id=%s — only 'id' column present", table, record_id)
        return

    set_clause = ", ".join(f'"{c}" = %s' for c in non_pk)
    values = [after[c] for c in non_pk] + [record_id]

    sql = f'UPDATE "{table}" SET {set_clause} WHERE "id" = %s'
    with conn.cursor() as cur:
        cur.execute(sql, values)
        if cur.rowcount == 0:
            logger.info("UPDATE %s id=%s → no row, trying INSERT", table, record_id)
            _execute_insert(conn, table, after)
        else:
            logger.info("UPDATE %s id=%s (%d row(s))", table, record_id, cur.rowcount)


def _execute_delete(conn, table: str, before: dict, after: dict):
    """Delete a row by id (id is read from after, then before)."""
    record = after or before or {}
    record_id = record.get("id")
    if record_id is None:
        logger.warning("Skipping DELETE %s — no 'id' field", table)
        return

    sql = f'DELETE FROM "{table}" WHERE "id" = %s'
    with conn.cursor() as cur:
        cur.execute(sql, (record_id,))
        logger.info("DELETE %s id=%s (%d row(s))", table, record_id, cur.rowcount)


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
