
__all__ = ["JobStatus"]


import time


from . import database
__all__.extend(database.__all__)
from .database import *

from . import mailing
__all__.extend(mailing.__all__)
from .mailing import *

from . import schedule
__all__.extend(schedule.__all__)
from .schedule import *



class Slot:

  def __init__(self, device=-1):
    self.device = device
    self.__enable = False
    self.__available = True

  def available( self ):
    return (self.__available and self.__enable)

  def lock( self ):
    self.available = False

  def unlock( self ):
    self.__available = True

  def enable(self):
    self.__enable=True

  def disable(self):
    self.__enable=False

  def enabled(self):
    return self.__enable


from . import consumer
__all__.extend(consumer.__all__)
from .consumer import *



class Clock:

  def __init__( self , maxseconds ):
    self.__maxseconds=maxseconds
    self.__then = None

  def __call__( self ):
    if self.__maxseconds is None:
      return False
    if not self.__then:
      self.__then = time.time()
      return False
    else:
      now = time.time()
      if (now-self.__then) > self.__maxseconds:
        # reset the time
        self.__then = None
        return True
    return False

  def reset(self):
    self.__then=None

from . import main
__all__.extend(main.__all__)
from .main import *


