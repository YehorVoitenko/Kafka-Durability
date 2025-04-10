import asyncio
import logging

from config.kafka_config import KAFKA_GROUP_ID, KAFKA_URL, KAFKA_CONSUMER_OFFSET, KAFKA_SECURED
from kafka_service.consumer.processors.processor1 import KafkaMessage1Processor
from kafka_service.consumer.processors.processor2 import KafkaMessage2Processor
from kafka_service.consumer.utils import ConsumerConfig
from kafka_service.kafka_connection_utils import get_token_for_kafka_by_keycloak

logging.basicConfig(level=logging.INFO)

consumer_config = dict(
    bootstrap_servers=KAFKA_URL,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset=KAFKA_CONSUMER_OFFSET,
    enable_auto_commit=False,
    secured=KAFKA_SECURED,
    oauth_cb=get_token_for_kafka_by_keycloak,
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanisms='OAUTHBEARER'
)


async def process_kafka_connection():
    while True:
        process_task_1 = KafkaMessage1Processor(
            config=ConsumerConfig(
                topic_to_subscribe='KafkaTesterProducer1',
                processor_name='KafkaProcessor1',
                **consumer_config
            )
        )

        process_task_2 = KafkaMessage2Processor(
            config=ConsumerConfig(
                topic_to_subscribe='KafkaTesterProducer2',
                processor_name='KafkaProcessor2',
                **consumer_config
            )
        )

        await asyncio.gather(
            process_task_1.process(),
            process_task_2.process()
        )


def init_kafka_connection():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.create_task(process_kafka_connection())

    loop.run_forever()


if __name__ == "__main__":
    init_kafka_connection()
