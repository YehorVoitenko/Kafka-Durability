from confluent_kafka import Producer

from config.kafka_config import KAFKA_TURN_ON
from kafka_service.producer.utils import (
    producer_config,
    delivery_report,
)


class SendMessageToKafka:
    def __init__(
            self,
            producer_name: str,
            data_to_send: str,
    ):
        self._producer_name = producer_name
        self._data_to_send = data_to_send

    def _send_to_kafka(self, ):
        self._producer.produce(
            topic=self._producer_name,
            key=self._producer_name,
            value=self._data_to_send,
            on_delivery=delivery_report,
            headers={"custom-header": "any info"}
        )
        self._producer.flush()

    def send_message(self):
        if KAFKA_TURN_ON:
            self._producer = Producer(producer_config())
            self._send_to_kafka()
