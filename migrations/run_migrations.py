#!/usr/bin/env python3
"""Apply SQL migration files (this directory) to the data warehouse.

Connects to the warehouse using the same environment variables as
``cdc_project/handler.py``:

    DB_HOST      (default: localhost)
    DB_PORT      (default: 5432)
    DB_NAME      (default: kla-klouk-data-warehouse)
    DB_USER      (default: postgres)
    DB_PASSWORD  (default: password)

Usage:
    python migrations/run_migrations.py                       # apply all *.sql
    python migrations/run_migrations.py 001_create_user_betting_bet_records.sql
"""

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = Path(__file__).resolve().parent

# Same .env loading as cdc_project/config.py (project's own config wins).
load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=True)


def _warehouse_connection():
    """Return a psycopg2 connection to the data warehouse."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "kla-klouk-data-warehouse"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "password"),
    )


def _apply_file(conn, path: Path) -> None:
    """Execute a single .sql migration file inside a transaction."""
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"Applied {path.name}")


def main() -> None:
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
        if not target.is_absolute():
            target = MIGRATIONS_DIR / target
        files = [target]
    else:
        files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not files:
        print("No migration files found.", file=sys.stderr)
        sys.exit(1)

    conn = _warehouse_connection()
    try:
        for path in files:
            _apply_file(conn, path)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
