import os
from pathlib import Path

from dotenv import load_dotenv

# Load the project's .env file, overriding any pre-existing env vars so that
# the project always uses its own configuration (e.g. DB_COUNT).
load_dotenv(
    dotenv_path=Path(__file__).resolve().parent / ".env",
    override=True,
)


def _build_db_config(index: int) -> dict:
    """Build a Debezium properties dict for the Nth database.

    Expects env vars: DB<N>_HOST, DB<N>_PORT, DB<N>_USER,
    DB<N>_PASSWORD, DB<N>_NAME.
    """
    prefix = f"DB{index}"
    return {
        "name": f"postgres-engine-{index}",
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        "topic.prefix": f"cdc_{index}",

        "database.hostname": os.getenv(f"{prefix}_HOST"),
        "database.port": os.getenv(f"{prefix}_PORT"),
        "database.user": os.getenv(f"{prefix}_USER"),
        "database.password": os.getenv(f"{prefix}_PASSWORD"),
        "database.dbname": os.getenv(f"{prefix}_NAME"),

        "plugin.name": "pgoutput",

        "publication.name": f"etl_pub_{index}",
        "slot.name": f"etl_slot_{index}",

        "snapshot.mode": "initial",

        "decimal.handling.mode": "double",
        "time.precision.mode": "adaptive_time_microseconds",
        "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",

        "offset.storage.file.filename": f"./offsets/offsets_{index}.dat",
    }


def get_all_properties() -> list[dict]:
    """Return a list of Debezium property dicts, one per database."""
    db_count = int(os.getenv("DB_COUNT", "4"))
    return [_build_db_config(i) for i in range(1, db_count + 1)]


# ── backwards-compatible alias ──────────────────────────────────────────────
# Existing code that calls get_properties() still works, returning DB1 only.
def get_properties() -> dict:
    return _build_db_config(1)