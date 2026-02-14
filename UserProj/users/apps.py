import logging

from django.apps import AppConfig

logger = logging.getLogger(__name__)


class UsersConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'users'

    def ready(self):
        """هنگام آماده‌سازی اپلیکیشن اجرا می‌شود"""
        # جلوگیری از اجرای دو باره در محیط‌های چند-پردازنده‌ای
        import os
        if os.environ.get('RUN_MAIN') or os.environ.get('RUN_MAIN') == 'true':
            from .kafka_consumer import KafkaConsumerThread
            consumer_thread = KafkaConsumerThread()
            consumer_thread.start()
            logger.info("Kafka Consumer thread started from AppConfig")
