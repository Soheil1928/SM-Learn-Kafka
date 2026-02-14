import json
import logging
import threading
from confluent_kafka import Consumer, KafkaError, KafkaException
from django.conf import settings
from django.db import connection

logger = logging.getLogger(__name__)


class KafkaConsumerThread(threading.Thread):
    """کانزیومر کافکا در یک ترد مجزا"""

    def __init__(self):
        super().__init__()
        self.daemon = True  # با بسته شدن جنگو، ترد هم بسته شود
        self.running = True
        self.consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'user-service-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
        })
        self.consumer.subscribe([settings.KAFKA_TOPIC_BOOK_CREATED])

    def run(self):
        logger.info("Kafka Consumer for User Service started")
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

                # پردازش پیام دریافتی
                self.process_message(msg)

            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")

        self.consumer.close()
        logger.info("Kafka Consumer stopped")

    def process_message(self, msg):
        """پردازش رویداد book_created از Book Service"""
        try:
            topic = msg.topic()
            key = msg.key().decode('utf-8') if msg.key() else None
            value = json.loads(msg.value().decode('utf-8'))

            logger.info(f"Received message - Topic: {topic}, Key: {key}")
            logger.info(f"Message content: {value}")

            # اینجا می‌توانید عملیات مورد نظر را انجام دهید
            # مثلاً ثبت لاگ، به‌روزرسانی کش، یا ذخیره در دیتابیس خود User Service

            if topic == settings.KAFKA_TOPIC_BOOK_CREATED:
                # مثال: یک رکورد EventLog ایجاد کنید
                # از connection.close() برای بستن خودکار کانکشن دیتابیس بعد از استفاده
                self.handle_book_created(value)

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON message")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def handle_book_created(self, book_data):
        """مدیریت رویداد ایجاد کتاب جدید"""
        # کد تجاری شما اینجا
        logger.info(f"New book created: {book_data.get('title')} by user {book_data.get('created_by')}")

        # مثال: ذخیره در یک مدل Event
        # از django.db.connection برای بستن خودکار کانکشن استفاده کنید
        try:
            # عملیات دیتابیس...
            pass
        finally:
            connection.close()  # جلوگیری از نشتی کانکشن

    def stop(self):
        """توقف مصرف‌کننده"""
        self.running = False