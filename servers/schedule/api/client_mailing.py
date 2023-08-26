
import requests, os, traceback
import json, orjson

from loguru import logger
from pydantic import BaseModel
from typing import Dict, Any

class EmailRequest(BaseModel):
    to : str
    subject : str
    body : str


class client_mailing:
    def __init__(self, host):
        self.host = host


    def try_request( self,
                     service: str,
                     endpoint: str,
                     method: str = "get",
                     params: Dict = {},
                     body: str = "",
                     stream: bool = False,
                    ) -> Any:

        function = {
            "get" : requests.get,
            "post": requests.post,
        }[method]

        logger.info(f"Request to {service}{endpoint}...")

        request = function(f"{service}{endpoint}", params=params, data=body)

        if request.status_code != 200:
            logger.critical(f"Request failed. Got {request.status_code}")
            return None

        return request.json()
      


    def is_alive(self):
        endpoint = "/mailing/is_alive"
        answer = self.try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error(f"The mailing server with host ({self.host}) is offline.")
            return False
        else:
            logger.info(f"The executor server with host ({self.host}) is online.")
            return True


    def send(self, to, subject, body):
        endpoint = "/mailing/send"
        answer = self.try_request(
            self.host, endpoint, method="post",
            body = EmailRequest(to=to, subject=subject, body=body).json()
        )

        if answer is None:
            logger.error(f"It is not possible to send an email to {to}")
            return False
        else:
            logger.info(f"Your email was sent to {to}...")
            return True
