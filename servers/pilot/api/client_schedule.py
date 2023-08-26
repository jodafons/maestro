
import requests
import json, orjson

from typing import Dict, Any
from loguru import logger



class client_schedule:


    def __init__(self, host):
        logger.info(f"Connecting to {host}...")
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
        endpoint = "/executor/status"
        answer = self.try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error("Server offline")
            return False
        else:
            logger.info("Server online...")
            return True