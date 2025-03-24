import functools

from config import kafka_config
from kafka_service.kafka_connection_utils import (
    _get_token_for_kafka_by_keycloak,
)


def producer_config():
    if not kafka_config.KAFKA_SECURED:
        return kafka_config.KAFKA_PRODUCER_CONNECT_CONFIG

    config_dict = dict()
    config_dict.update(kafka_config.KAFKA_PRODUCER_CONNECT_CONFIG)
    config_dict["oauth_cb"] = _get_token_for_kafka_by_keycloak
    return config_dict


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(
        "User record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )
