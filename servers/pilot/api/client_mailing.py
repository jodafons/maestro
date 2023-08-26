
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
        logger.info(f"Connecting to {host}...")
        self.host = host

        if not self.status():
            logger.critical("Mailing server not connected. Abort.")
        else:
            logger.info("Mailing server connected.")


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
            logger.critical(f"Request failed. Got {request.status_code} {request.response}")

        if not stream:
            return request.json()
        else:
            def generate_response():
                for payload in request.iter_lines():
                    if isinstance(payload, bytes):
                        yield orjson.loads(payload.lstrip(b"data:").rstrip(b"\n"))
                    else:
                        yield json.loads(payload.lstrip("data").rstrip("\n"))
            return generate_response()


    def status(self):
        endpoint = "/mailing/status"
        answer = self.try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error("Server offline")
            return False
        else:
            logger.info("Server online...")
            return True


    def send(self, to, subject, body):
        endpoint = "/mailing/send"
        answer = self.try_request(
            self.host, endpoint, method="post",
            body = EmailRequest(to=to, subject=subject, body=body).json()
        )

        if answer is None:
            logger.error("Email server error")
            return False
        else:
            logger.info("Email dispatcher...")
            return True
