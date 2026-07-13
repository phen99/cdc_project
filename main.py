import signal
import sys
from threading import Thread

from pydbzengine import DebeziumJsonEngine

from config import get_all_properties
from handler import CDCHandler
from logger import get_logger

logger = get_logger(__name__)


def run_engine(props: dict, handler: CDCHandler, slot: int):
    """Start a Debezium engine for one database."""
    engine_name = props.get("name", f"db-{slot}")
    logger.info("Starting engine for %s...", engine_name)

    engine = DebeziumJsonEngine(properties=props, handler=handler)
    engines[slot - 1] = engine
    engine.run()

    logger.info("Engine for %s stopped", engine_name)


def shutdown(signum, frame):
    """Gracefully stop all engines on SIGINT / SIGTERM."""
    logger.warning("Shutdown signal received — stopping all engines...")
    for engine in engines:
        if engine is not None:
            try:
                engine.interrupt()
            except Exception:
                pass
    sys.exit(0)


def main():
    all_properties = get_all_properties()
    n = len(all_properties)

    logger.info("Starting CDC engines for %d databases", n)

    # Pre-allocate the engines list so it's thread-safe
    global engines
    engines = [None] * n

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    threads = []
    for i, props in enumerate(all_properties, start=1):
        handler = CDCHandler(db_label=f"DB{i}")
        t = Thread(target=run_engine, args=(props, handler, i), daemon=True)
        t.start()
        threads.append(t)

    # Block until all engines finish (they run forever until interrupted)
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
