import time

import requests
from fastapi import HTTPException

from config import kafka_config


def _get_token_for_kafka_by_keycloak(conf):
    payload = {
        "grant_type": "client_credentials",
        "scope": str(kafka_config.KAFKA_KEYCLOAK_SCOPES),
    }

    attempt = 5
    while attempt > 0:
        try:
            response = requests.post(
                kafka_config.KAFKA_KEYCLOAK_TOKEN_URL,
                timeout=30,
                auth=(
                    kafka_config.KAFKA_KEYCLOAK_CLIENT_ID,
                    kafka_config.KAFKA_KEYCLOAK_CLIENT_SECRET,
                ),
                data=payload,
            )
        except ConnectionError:
            time.sleep(1)
            attempt -= 1

        else:
            if response.status_code == 200:
                token = response.json()
                return token["access_token"], time.time() + float(token["expires_in"])

            time.sleep(1)
            attempt -= 1

    raise HTTPException(
        status_code=503,
        detail="Token verification service unavailable"
    )

