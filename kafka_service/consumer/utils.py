from kafka_service.kafka_connection_utils import (
    _get_token_for_kafka_by_keycloak,
)


def consumer_config(conf):
    if conf.get("sasl.mechanisms", "") == "OAUTHBEARER":
        conf["oauth_cb"] = _get_token_for_kafka_by_keycloak
    return conf
