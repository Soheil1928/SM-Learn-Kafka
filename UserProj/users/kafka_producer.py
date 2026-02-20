# kafka_producer.py
import json
from confluent_kafka import Producer
from django.conf import settings


producer = Producer({
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
})


def send_event(topic, key, value):
    producer.produce(topic=topic, key=str(key), value=json.dumps(value))
    producer.poll(0)
