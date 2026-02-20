# kafka_consumer.py
import json
from confluent_kafka import Consumer
from django.conf import settings


def run_consumer():
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "book-service-group",
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([settings.KAFKA_TOPIC_USER_CREATED])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            continue

        data = json.loads(msg.value().decode())
        # handle_user_created(data)