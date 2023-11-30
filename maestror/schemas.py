
__all__ = []

import requests, json, orjson
from typing import Dict, Any, List
from pydantic import BaseModel
from loguru import logger


#
# Communication between servers
#

class Request(BaseModel):
    host      : str=""
    metadata  : Dict={}

class Answer(BaseModel):
    host      : str=""
    status    : bool=True
    message   : str=""
    metadata  : Dict={}


#
# Special forms
#

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


#
# client connection
#

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
                    ) -> Answer:

        function = {
            "get" : requests.get,
            "post": requests.post,
        }[method]
        try:
            request = function(f"{self.host}/{self.service}/{endpoint}", params=params, data=body)
        except:
            logger.error("failed to establish the connection...")
            return Answer(status=False)
        if request.status_code != 200:
            logger.error(f"request failed. got {request.status_code}")
            return Answer(status=False)
        return Answer( **request.json() )
        

    def ping(self):
        return False if self.try_request('ping', method="get") is None else True
