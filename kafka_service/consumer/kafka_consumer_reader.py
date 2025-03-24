import asyncio
import functools
import logging
from abc import abstractmethod
from typing import Any

from confluent_kafka import Consumer

from config import kafka_config
from kafka_service.consumer.utils import consumer_config

logging.basicConfig(level=logging.INFO)


class KafkaMessageReader:
    def __init__(
            self,
            producer_topic: str,
            processor_name: str
    ):
        self._consumer = Consumer(
            consumer_config(kafka_config.KAFKA_CONSUMER_CONNECT_CONFIG)
        )
        self._producer_topic = producer_topic
        self._consumer.subscribe(
            topics=[producer_topic],
            on_assign=self._connection_flag_method
        )
        self._processor_name = processor_name

    def _connection_flag_method(self, *args):
        logging.info(f"{self._processor_name} successful subscribed to the topic {self._producer_topic}\n")

    @staticmethod
    def _message_is_empty(message: Any, consumer: Consumer):
        if message is None:
            consumer.commit(asynchronous=True)
            return True

        if getattr(message, "key", None) is None:
            consumer.commit(asynchronous=True)
            return True

        if message.key() is None:
            consumer.commit(asynchronous=True)
            return True

        return False

    @staticmethod
    async def _get_message(consumer):
        loop = asyncio.get_running_loop()
        poll = functools.partial(consumer.poll, 1.0)
        return await loop.run_in_executor(executor=None, func=poll)

    @abstractmethod
    async def process(self):
        pass
