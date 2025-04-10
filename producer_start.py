from config.kafka_config import KAFKA_URL, KAFKA_PRODUCER_TOPIC_1, KAFKA_SECURED, KAFKA_PRODUCER_TOPIC_2
from kafka_service.kafka_connection_utils import get_token_for_kafka_by_keycloak
from kafka_service.producer.kafka_producer import ProducerInitializer
from kafka_service.producer.utils import ProducerConfig, DataSend

producer_config = dict(
    bootstrap_servers=KAFKA_URL,
    secured=KAFKA_SECURED,
    oauth_cb=get_token_for_kafka_by_keycloak,
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanisms='OAUTHBEARER'
)

task = ProducerInitializer(
    config=ProducerConfig(
        producer_name=KAFKA_PRODUCER_TOPIC_1,
        **producer_config
    )
)
task.send_message(
    data_to_send=DataSend(
        key='KEY1',
        value='VALUE1'
    )
)

task = ProducerInitializer(
    config=ProducerConfig(
        producer_name=KAFKA_PRODUCER_TOPIC_2,
        **producer_config
    ),

)
task.send_message(
    data_to_send=DataSend(
        key='KEY2',
        value='VALUE2'
    ))
