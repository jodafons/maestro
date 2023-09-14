


import requests
import json, orjson

from typing import Dict, Any
from loguru import logger
from pydantic import BaseModel



def try_request( service: str,
                 endpoint: str,
                 method: str = "get",
                 params: Dict = {},
                 body: str = "",
                 default: Any = None
                ) -> Any:

    function = {
        "get" : requests.get,
        "post": requests.post,
    }[method]
    try:
        request = function(f"{service}{endpoint}", params=params, data=body)
    except:
        logger.error("Failed to establish a new connection.")
        return defalt
    if request.status_code != 200:
        logger.critical(f"Request failed. Got {request.status_code}")
        return default
    return request.json()


