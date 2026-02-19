import json
import time
import uuid
import random
from datetime import datetime
from google.cloud import pubsub_v1
import os

project_id=os.environ.get("PROJECT", "data-engineering")
topic_id = os.environ.get("PUBSUB_TOPIC", "orders")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

while True:
    event = {
        "order_id": str(uuid.uuid4()),
        "user_id": f"U{random.randint(100, 999)}",
        "product_id": f"P{random.randint(1, 50)}",
        "amount": round(random.randint(1, 999) / 10, 2),
        "event_time": datetime.utcnow().isoformat()
    }

    publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
    print("Published:", event)
    time.sleep(1)
