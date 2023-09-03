import requests, socket
import json, orjson
from loguru import logger
from typing import Dict, Any, List
from pydantic import BaseModel


class Host(BaseModel):
    me       : str
    device   : int


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
                    ) -> Any:

        function = {
            "get" : requests.get,
            "post": requests.post,
        }[method]

        try:
            request = function(f"{service}{endpoint}", params=params, data=body)
        except:
            logger.error("Failed to establish a new connection.")
            return None

        if request.status_code != 200:
            logger.critical(f"Request failed. Got {request.status_code}")
            return None

        return request.json()
      

    def is_alive(self):
        endpoint = "/pilot/is_alive"
        answer = self.try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error("The pilot server is offline.")
            return False
        else:
            logger.info("The pilot server is online.")
            return True



    def register(self, me, device = -1):

        body = Host(me=me, device=device)
        endpoint = "/pilot/register"
        answer = self.try_request(
            self.host, endpoint, method="post", body=body.json() ,
        )
        if answer is None:
            logger.error(f"Not possible to register the current executor with name ({me}) into the pilot server.")
            return False
        else:
            logger.info(f"The executor with name ({me}) was registered into the pilot server.")
            return True