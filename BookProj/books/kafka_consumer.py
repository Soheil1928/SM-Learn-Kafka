# books/kafka_consumer.py
import json
import logging
import threading
from confluent_kafka import Consumer, KafkaError
from django.conf import settings
from django.db import connection

logger = logging.getLogger(__name__)


class KafkaConsumerThread(threading.Thread):

    def __init__(self):
        super().__init__()
        self.daemon = True
        self.running = True
        self.consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'book-service-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        })
        self.consumer.subscribe([settings.KAFKA_TOPIC_USER_CREATED])

    def run(self):
        logger.info("Kafka Consumer for Book Service started")
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break

                self.process_message(msg)

            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")

        self.consumer.close()

    def process_message(self, msg):
        try:
            topic = msg.topic()
            key = msg.key().decode('utf-8') if msg.key() else None
            value = json.loads(msg.value().decode('utf-8'))

            logger.info(f"Book Service received: {topic} - Key: {key}")
            logger.info(f"User created: {value}")

            if topic == settings.KAFKA_TOPIC_USER_CREATED:
                self.handle_user_created(value)

        except Exception as e:
            logger.error(f"Error processing: {e}")

    def handle_user_created(self, user_data):
        """مثال: ثبت کاربر جدید در جدول لاگ Book Service"""
        logger.info(f"New user registered: {user_data.get('username')} ({user_data.get('email')})")
        # عملیات دلخواه...
        try:
            # شاید بخواهید برای کاربر جدید یک کتاب پیش‌فرض ایجاد کنید
            print(f'Book kafka consumer. user_data: {user_data}')
            pass
        finally:
            connection.close()

    def stop(self):
        self.running = False