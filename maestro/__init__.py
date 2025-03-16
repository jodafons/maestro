__all__ = [
    "setup_logs",
    "get_argparser_formatter",
    "GB",
    "random_id",
    "random_token",
    "md5checksum",
    "StatusCode",
    ]

import uuid, hashlib, sys, argparse
from loguru import logger
from rich_argparse import RichHelpFormatter
from copy import copy

GB=1024


def get_argparser_formatter( custom : bool=True):
    if custom:
        RichHelpFormatter.styles["argparse.args"]     = "green"
        RichHelpFormatter.styles["argparse.prog"]     = "bold grey50"
        RichHelpFormatter.styles["argparse.groups"]   = "bold green"
        RichHelpFormatter.styles["argparse.help"]     = "grey50"
        RichHelpFormatter.styles["argparse.metavar"]  = "blue"
        return RichHelpFormatter
    else:
        return argparse.HelpFormatter

def setup_logs( name , level, save : bool=False, color="cyan"):
    """Setup and configure the logger"""

    logger.configure(extra={"name" : name})
    logger.remove()  # Remove any old handler
    #format="<green>{time:DD-MMM-YYYY HH:mm:ss}</green> | <level>{level:^12}</level> | <cyan>{extra[slurms_name]:<30}</cyan> | <blue>{message}</blue>"
    format="<"+color+">{extra[name]:^25}</"+color+"> | <green>{time:DD-MMM-YYYY HH:mm:ss}</green> | <level>{level:^12}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <blue>{message}</blue>"
    logger.add(
        sys.stdout,
        colorize=True,
        backtrace=True,
        diagnose=True,
        level=level,
        format=format,
    )
    if save:
        output_file = name.replace(':','_').replace('-','_') + '.log'
        logger.add(output_file, 
                   rotation="30 minutes", 
                   retention=3, 
                   format=format, 
                   level=level, 
                   colorize=False)   




def random_id():
    new_uuid = uuid.uuid4()
    return str(new_uuid)[-12:]

def random_token():
    new_uuid = str(uuid.uuid4()) + str(uuid.uuid4())
    return new_uuid.replace('-','')

def md5checksum(fname):
    md5 = hashlib.md5()
    f = open(fname, "rb")
    while chunk := f.read(4096):
        md5.update(chunk)
    return md5.hexdigest()


class StatusObj(object):

  _status = 1

  def __init__(self, sc):
    self._status = sc
    self._value  = True
    self._reason = ""

  def isFailure(self):
    if self._status < 1:
      return True
    else:
      return False
    
  def result(self, key : str=None):
     return self._value if not key else self._value[key]
  
  def reason(self):
     return self._reason
    
  def __call__(self, value=True, reason : str=""):
     
    obj = copy(self)
    obj._value = value
    obj._reason = reason
    return obj

  def __eq__(self, a, b):
    if a.status == b.status:
      return True
    else:
      return False

  def __ne__(self, a, b):
    if a.status != b.status:
      return True
    else:
      return False

  @property
  def status(self):
    return self._status



# status code enumeration
class StatusCode(object):
  """
    The status code of something
  """
  SUCCESS = StatusObj(1)
  FAILURE = StatusObj(0)
  FATAL   = StatusObj(-1)

from . import schemas
__all__.extend( schemas.__all__ )
from .schemas import *

from . import db
__all__.extend( db.__all__ )
from .db import *

from . import io
__all__.extend( io.__all__ )
from .io import *

#from . import slurm
#__all__.extend( slurm.__all__ )
#from .slurm import *

#from . import manager
#__all__.extend( manager.__all__ )
#from .manager import *

#from . import schedulers
#__all__.extend( schedulers.__all__ )
#from .schedulers import *