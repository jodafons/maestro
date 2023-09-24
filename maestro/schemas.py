
__all__ = []

from typing import Dict, Any, List
from pydantic import BaseModel


class Email(BaseModel):
    to      : str
    subject : str
    body    : str



# ping
class Executor(BaseModel):
    host      : str
    device    : int
    size      : int
    allocated : int
    full      : bool
    partition : str

# pong
class Server(BaseModel):
    database      : str
    binds         : str
    partitions    : List[str]
    executors     : List[Executor]





class Job(BaseModel):  
  id          : int
  image       : str 
  command     : str
  envs        : str
  binds       : str
  workarea    : str
  inputfile   : str
  partition   : str
  status      : str

class Task(BaseModel):
  id          : int
  name        : str
  volume      : str
  jobs        : List[Job]
  partition   : str
  status      : str