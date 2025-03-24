from kafka_service.consumer.kafka_consumer_reader import KafkaMessageReader


class KafkaMessage2Processor(KafkaMessageReader):
    _PRODUCER_TOPIC = 'KafkaTesterProducer2'

    # it needs for logging
    _PROCESSOR_NAME = 'SecondProcessor'

    def __init__(
            self,
    ):
        super().__init__(
            producer_topic=self._PRODUCER_TOPIC,
            processor_name=self._PROCESSOR_NAME
        )

    async def process(self):
        while True:
            message = await self._get_message(consumer=self._consumer)
            if self._message_is_empty(message=message, consumer=self._consumer):
                continue

            message_key = message.key().decode("utf-8")
            message_value = message.value().decode("utf-8")

            # TODO: process here some adapters
            print('-----------------------------')
            print('KEY', message_key)
            print('VALUE', message_value)
            print('CONSUMER', self._PROCESSOR_NAME)
            print('-----------------------------')

            self._consumer.commit(asynchronous=True)
            continue

        self._consumer.close()
