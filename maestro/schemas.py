
__all__ = []

from typing import Dict, Any, List
from pydantic import BaseModel


#
# orchestra service integration
#

class Email(BaseModel):
    to      : str = ""
    subject : str = ""
    body    : str = ""

class Executor(BaseModel):
    host      : str  = ""
    device    : int  = -1
    size      : int  = 0
    allocated : int  = 0
    full      : bool = False
    partition : str  = ""

class Server(BaseModel):
    database      : str = ""
    binds         : str = ""
    partitions    : List[str] = []
    executors     : List[Executor] = []



#
# maestro and orchestra integration
#

class Request(BaseModel):
  token       : str = ""


class Job(Request):  
  id          : int = -1
  image       : str = ""
  command     : str = ""
  envs        : str = "{}"
  binds       : str = "{}"
  workarea    : str = ""
  inputfile   : str = ""
  partition   : str = ""
  status      : str = "Unknown"

class Task(Request):
  id          : int = -1
  name        : str = ""
  volume      : str = ""
  jobs        : List[Job] = []
  partition   : str = ""
  status      : str = "Unknown"


