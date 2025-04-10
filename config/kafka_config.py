import os

from dotenv import load_dotenv

load_dotenv()

AVAILABLE_TRUE_VALUES = ["TRUE", "Y", "YES", "1"]

KAFKA_URL = os.getenv("KAFKA_URL", "host:port")

KAFKA_SECURED = (
        str(os.getenv("KAFKA_SECURED", True)).upper() in AVAILABLE_TRUE_VALUES
)

KAFKA_PRODUCER_TOPIC_1 = os.getenv(
    "KAFKA_PRODUCER_TOPIC", "KafkaTesterProducer1"
)

KAFKA_PRODUCER_TOPIC_2 = os.getenv(
    "KAFKA_PRODUCER_TOPIC", "KafkaTesterProducer2"
)

KAFKA_KEYCLOAK_CLIENT_ID = os.getenv(
    "KAFKA_KEYCLOAK_CLIENT_ID", "client_id"
)

KAFKA_KEYCLOAK_CLIENT_SECRET = os.getenv(
    "KAFKA_KEYCLOAK_CLIENT_SECRET", "secret"
)
KAFKA_KEYCLOAK_TOKEN_URL = os.getenv(
    "KAFKA_KEYCLOAK_TOKEN_URL",
    "token_url",
)

KAFKA_KEYCLOAK_SCOPES = os.getenv("KAFKA_KEYCLOAK_SCOPES", "scope")

KAFKA_GROUP_ID = "LocalTester1"

KAFKA_CONSUMER_OFFSET = os.getenv("KAFKA_CONSUMER_OFFSET", "latest")
