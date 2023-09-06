
import requests, os, traceback
import json, orjson

from loguru import logger
from pydantic import BaseModel
from typing import Dict, Any

try:
    from maestro.api.base import try_request
except:
    from api.base import try_request

class EmailRequest(BaseModel):
    to : str
    subject : str
    body : str


class client_mailing:
    def __init__(self, host):
        self.host = host


    def is_alive(self):
        endpoint = "/mailing/is_alive"
        answer = try_request(
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
