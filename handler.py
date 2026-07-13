# import json

# from pydbzengine import BasePythonChangeHandler


# class CDCHandler(BasePythonChangeHandler):

#     def handle_batch(self, records):

#         print(f"Received {len(records)} events")

#         for record in records:

#             value = record.value

#             print(json.dumps(value, indent=4))

#             op = value["op"]

#             if op == "c":
#                 self.insert(value)

#             elif op == "u":
#                 self.update(value)

#             elif op == "d":
#                 self.delete(value)

#     def insert(self, event):
#         print("INSERT")
#         print(event)

#     def update(self, event):
#         print("UPDATE")
#         print(event)

#     def delete(self, event):
#         print("DELETE")
#         print(event)

import json

from pydbzengine import BasePythonChangeHandler

from logger import get_logger

logger = get_logger(__name__)


class CDCHandler(BasePythonChangeHandler):

    def __init__(self, db_label: str = "unknown"):
        self.db_label = db_label
        self.logger = get_logger(f"handler.{db_label}")

    def handleJsonBatch(self, records):

        self.logger.info("Received %d change events", len(records))

        for record in records:

            self.logger.info("=" * 40)

            topic = record.sourceRecord().topic()
            event = json.loads(str(record.value()))
            payload = event["payload"]

            self.logger.info(
                "Topic: %s | Key: %s | Payload: %s",
                topic,
                record.key,
                json.dumps(payload, indent=2),
            )
            self.logger.debug("Full event value: %s", record.value)

            self.logger.info("=" * 40)