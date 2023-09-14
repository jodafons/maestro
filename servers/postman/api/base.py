
import requests
import json, orjson
from loguru import logger
from typing import Dict, Any, List


class client:

    def __init__(self, host, service):
        self.host = host
        self.service = service

    def try_request( self, 
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
            request = function(f"{self.host}/{self.service}/{endpoint}", params=params, data=body)
        except:
            logger.error("Failed to establish a new connection.")
            return None
        if request.status_code != 200:
            logger.critical(f"Request failed. Got {request.status_code}")
            return None
        return request.json()


    def is_alive(self):
        res = self.try_request('is_alive', method="get")
        if res is None:
            logger.error(f"The schedule server with host ({self.host}) is offline.")
            return False
        else:
            logger.info(f"The schedule server with host ({self.host}) is online.")
            return True