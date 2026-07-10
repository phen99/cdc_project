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

from numpy import record
from pydbzengine import BasePythonChangeHandler


class CDCHandler(BasePythonChangeHandler):

    def handleJsonBatch(self, records):

        print(f"Received {len(records)} change events")

        for record in records:

            print("======================")

            print(record)
            print("Topic:", record.sourceRecord().topic())
            event = json.loads(str(record.value()))
            payload = event["payload"]
            print("Key:", record.key)
            print("payload:", payload)
            # exit()
            print("Value:")
            print(record.value)

            print("======================")