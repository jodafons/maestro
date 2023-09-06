import requests, socket
import json, orjson
from loguru import logger
from typing import Dict, Any, List
from pydantic import BaseModel

try:
    from maestro.api.base import try_request
except:
    from api.base import try_request


class Host(BaseModel):
    me       : str
    device   : int


class client_pilot:


    def __init__(self, host):
        logger.info("Connecting to {host}...")
        self.host = host



    def is_alive(self):
        endpoint = "/pilot/is_alive"
        answer = try_request(
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