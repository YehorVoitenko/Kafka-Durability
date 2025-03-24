from kafka_service.consumer.kafka_consumer_reader import KafkaMessageReader


class KafkaMessage1Processor(KafkaMessageReader):
    _PRODUCER_TOPIC = 'KafkaTesterProducer1'

    # it needs for logging
    _PROCESSOR_NAME = 'FirstProcessor'

    def __init__(
            self,
    ):
        super().__init__(
            producer_topic=self._PRODUCER_TOPIC,
            processor_name=self._PROCESSOR_NAME
        )

    async def process(self):
        while True:
            try:
                message = await self._get_message(consumer=self._consumer)
                if self._message_is_empty(message=message, consumer=self._consumer):
                    continue

                message_key = message.key().decode("utf-8")
                message_value = message.value().decode("utf-8")

                if message_value in ['WRONG_VALUE']:
                    raise ValueError('You catch wrong value')

                # TODO: process here some adapters
                print('-----------------------------')
                print('KEY', message_key)
                print('VALUE', message_value)
                print('CONSUMER', self._PROCESSOR_NAME)
                print('-----------------------------')


            except Exception as e:
                print(e)

            finally:
                self._consumer.commit(asynchronous=True)
                continue

        self._consumer.close()
