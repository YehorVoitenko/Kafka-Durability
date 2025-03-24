from config.kafka_config import KAFKA_PRODUCER_TOPIC_1, KAFKA_PRODUCER_TOPIC_2
from kafka_service.producer.kafka_producer import SendMessageToKafka

for value in ['WRONG_VALUE', 'VALID_VALUE']:
    task = SendMessageToKafka(
        data_to_send=value,
        producer_name=KAFKA_PRODUCER_TOPIC_1
    )
    task.send_message()

task = SendMessageToKafka(
    data_to_send='DATA2',
    producer_name=KAFKA_PRODUCER_TOPIC_2
)
task.send_message()
