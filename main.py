from pydbzengine import DebeziumJsonEngine

from config import get_properties
from handler import CDCHandler


def main():

    properties = get_properties()

    handler = CDCHandler()

    engine = DebeziumJsonEngine(
        properties=properties,
        handler=handler
    )

    print("Starting CDC engine...")

    engine.run()

    print("CDC engine stopped")


if __name__ == "__main__":
    main()