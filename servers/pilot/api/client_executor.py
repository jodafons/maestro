
import requests
import json, orjson

from typing import Dict, Any
from loguru import logger



class client_executor:


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
        
        endpoint = "/executor/is_alive"
        answer = self.try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error(f"The executor server with host ({self.host}) is offline.")
            return False
        else:
            logger.info("The executor server with host ({self.host}) is online.")
            return True

