
import traceback

from loguru import logger
from typing import Dict, Any, List
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

try:
    from api.base import client
    from enumerations import JobStatus
    from models import Task, Job
except:
    from maestro.api.base import client
    from maestro.enumerations import JobStatus
    from maestro.models import Task, Job



class Describe(BaseModel):
    device    : int
    size      : int
    allocated : int
    full      : bool


class Email(BaseModel):
    to      : str
    subject : str
    body    : str

#
# APIs
#

class pilot(client):

    def __init__(self, host):
        client.__init__(self, host, "pilot")
        logger.info(f"Connecting to {host}...")


    def append(self, hostname):
        res = self.try_request('connect', method="post", body=Executor(hostname=hostname, device=device).json())
        return True if res else False


class executor(client):

    def __init__(self, host):
        client.__init__(self, host, "executor")
        

    def start(self, job_id):
        res = self.try_request(f"start/{job_id}", method="post")
        return True if res else False


    def describe(self): 
        res = self.try_request("describe", method="get")
        return Describe(**res) if res else None


class schedule(client):

    def __init__(self, host):
        client.__init__(self, host, "schedule")


    def run(self):
        res = self.try_request("run", method="get")
        return True if res else False


class postman(client):

    def __init__(self, host):
        client.__init__(self, host, "postman")

    def send(self, to, subject, body):
        res = self.try_request("send", method="post",body = Email(to=to, subject=subject, body=body).json())
        return True if res else False

