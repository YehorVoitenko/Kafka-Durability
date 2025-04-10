from kafka_service.consumer.kafka_consumer_reader import ConsumerInitializer
from kafka_service.consumer.utils import ConsumerConfig, catch_exceptions


class KafkaMessage2Processor(ConsumerInitializer):
    def __init__(
            self,
            config: ConsumerConfig
    ):

        super().__init__(config=config)
        self._config = config

    @catch_exceptions()
    async def process(self):
        while True:
            message = await self.get_message(consumer=self._consumer)
            if self.message_is_empty(message=message, consumer=self._consumer):
                continue

            message_key = message.key().decode("utf-8")
            message_value = message.value().decode("utf-8")

            print('-----------------------------')
            print('KEY', message_key)
            print('VALUE', message_value)
            print('PRODUCER', self._config.topic_to_subscribe)
            print('-----------------------------')

            self._consumer.commit(asynchronous=True)

        self._consumer.close()

