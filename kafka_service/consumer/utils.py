import functools
import traceback
from typing import Union, Callable

from pydantic import BaseModel, model_validator


class ConsumerConfig(BaseModel):
    topic_to_subscribe: str
    processor_name: str
    bootstrap_servers: str
    group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool
    secured: bool = False
    oauth_cb: Union[Callable, None] = None
    security_protocol: Union[str, None] = None
    sasl_mechanisms: Union[str, None] = None

    @model_validator(mode='after')
    def check_secured_config(self):
        if self.secured:
            missing_fields = []
            if self.oauth_cb is None:
                missing_fields.append("oauth_cb")

            if self.security_protocol is None:
                missing_fields.append("security_protocol")

            if self.sasl_mechanisms is None:
                missing_fields.append("sasl_mechanisms")

            if missing_fields:
                raise ValueError(
                    f"If 'secured' is True, the following fields must be set: {', '.join(missing_fields)}"
                )
        return self


def catch_exceptions(handler=None):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if handler:
                    handler(e)

                else:
                    print("Exception caught in decorator:")
                    traceback.print_exc()

        return wrapper

    return decorator
