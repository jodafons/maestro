
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