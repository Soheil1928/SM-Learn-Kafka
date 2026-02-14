import json
import logging
from confluent_kafka import Producer
from django.conf import settings

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.producer = Producer({
                'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
                'client.id': 'user-service-producer',
                'acks': 'all',  # تأیید از همه replicaها
                'retries': 3,
            })
        return cls._instance

    def delivery_report(self, err, msg):
        """گزارش تحویل پیام (آسنکرون)"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, topic, key, value):
        """ارسال پیام به کافکا"""
        try:
            self.producer.produce(
                topic=topic,
                key=str(key).encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # فعال‌سازی callback
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False

    def flush(self):
        """انتظار برای ارسال همه پیام‌ها"""
        self.producer.flush()


# نمونه Singleton
kafka_producer = KafkaProducerClient()