
__all__ = []

import requests, json, orjson
from typing import Dict, Any, List
from pydantic import BaseModel
from loguru import logger


class HandShake(BaseModel):
    host      : str=""
    status    : bool=False
    metadata  : Dict={}


class Job(BaseModel):  
  id          : int = -1
  image       : str = ""
  command     : str = ""
  envs        : str = "{}"
  binds       : str = "{}"
  workarea    : str = ""
  inputfile   : str = ""
  partition   : str = ""
  status      : str = "Unknown"

class Task(BaseModel):
  id          : int = -1
  name        : str = ""
  volume      : str = ""
  jobs        : List[Job] = []
  partition   : str = ""
  status      : str = "Unknown"




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
            logger.error(f"Request failed. Got {request.status_code}")
            return None
        return request.json()


    def ping(self):
        return False if self.try_request('ping', method="get") is None else True
