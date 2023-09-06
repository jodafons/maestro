
import requests
import json, orjson

from typing import Dict, Any
from loguru import logger
from pydantic import BaseModel

try:
    from maestro.api.base import try_request
except:
    from api.base import try_request


class Resume(BaseModel):
    size      : int
    allocated : int
    full      : bool

class Job(BaseModel):
    job_id    : int



class client_executor:


    def __init__(self, host):
        self.host = host


    def is_alive(self):
        
        endpoint = "/executor/is_alive"
        answer = try_request(
            self.host, endpoint, method="get",
        )

        if answer is None:
            logger.error(f"The executor server with host ({self.host}) is offline.")
            return False
        else:
            logger.info(f"The executor server with host ({self.host}) is online.")
            return True


    def start(self, job_id):

        endpoint = "/executor/start_job"
        answer = self.try_request(
            self.host, endpoint, method="post",
            body = Job(job_id=job_id).json(),
        )
        if answer is None:
            logger.error(f"It is not possible to start job...")
            return False
        else:
            logger.info(f"Job started into the executor...")
            return True



    def resume(self):
        
        endpoint = "/executor/resume"
        answer = self.try_request(
            self.host, endpoint, method="get",
        )
        if answer is None:
            logger.error(f"The executor server with host ({self.host}) is offline.")
            return None
        else:
            logger.info(f"The executor server with host ({self.host}) is online.")
            return Resume(**answer)


    