import os
from dotenv import load_dotenv

load_dotenv()


def get_properties():
    return {
        "name": "postgres-engine",
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        "topic.prefix": "cdc",

        "database.hostname": os.getenv("DB_HOST"),
        "database.port": os.getenv("DB_PORT"),
        "database.user": os.getenv("DB_USER"),
        "database.password": os.getenv("DB_PASSWORD"),
        "database.dbname": os.getenv("DB_NAME"),

        "plugin.name": "pgoutput",

        "publication.name": "etl_pub",
        "slot.name": "etl_slot",

        "snapshot.mode": "initial",

        "decimal.handling.mode": "double",
        "time.precision.mode": "adaptive_time_microseconds",
        "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",

        "offset.storage.file.filename": "./offsets/offsets.dat",
    }