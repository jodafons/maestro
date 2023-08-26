

import requests
import json, orjson
from loguru import logger
from typing import Dict, Any, List
from pydantic import BaseModel


class Host(BaseModel):
    hostname : str
    dnsname  : str
    devices  : List


class client_pilot:


    def __init__(self, host):
        logger.info("Connecting to {host}...")
        self.host = host


    def try_request( self,
                     service: str,
                     endpoint: str,
                     method: str = "get",
                     params: Dict = {},
                     body: str = "",
                     stream: bool = False,
                     timeout: int = 5,
                    ) -> Any:

        function = {
            "get" : requests.get,
            "post": requests.post,
        }[method]

        request = function(f"{service}{endpoint}", params=params, data=body, timeout=timeout)

        if request.status_code != 200:
            logger.critical(f"Request failed. Got {request.status_code} {request.response}")

        return request.json()
      

    def is_alive(self):
        endpoint = "/pilot/is_alive"
        answer = self.try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error("Server offline")
            return False
        else:
            logger.info("Server online...")
            return True



    def register(self):

        hostname = socket.gethostname()
        dnsname  = socket.getfqdn()
        body = Host(hostname=hostname, dnsname=dnsname, devices=[-1])


        endpoint = "/pilot/register"
        answer = self.try_request(
            self.host, endpoint, method="post", body=body.json() ,
        )
        if answer is None:
            logger.error("Server offline")
            return False
        else:
            logger.info("Server online...")
            return True