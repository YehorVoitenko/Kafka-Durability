import asyncio
import logging

from kafka_service.consumer.processors.processor1 import KafkaMessage1Processor
from kafka_service.consumer.processors.processor2 import KafkaMessage2Processor

logging.basicConfig(level=logging.INFO)


async def process_kafka_connection():
    while True:
        logging.info("starting kafka processors...")

        process_task_1 = KafkaMessage1Processor()
        process_task_2 = KafkaMessage2Processor()

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
