
import requests
import json, orjson

from typing import Dict, Any
from loguru import logger
import traceback

try:
    from maestro.api.base import try_request
except:
    from api.base import try_request


class client_schedule:


    def __init__(self, host):
        self.host = host


    def is_alive(self):
        endpoint = "/schedule/is_alive"
        answer = try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error(f"The schedule server with host ({self.host}) is offline.")
            return False
        else:
            logger.info(f"The schedule server with host ({self.host}) is online.")
            return True

    def run(self):
        endpoint = "/schedule/run"
        answer = self.try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error(f"The schedule server with host ({self.host}) is offline.")
            return False
        else:
            logger.info(f"The schedule server with host ({self.host}) is online.")
            return True