
__all__ = ["JobStatus"]

SECOND = 1
MINUTE  = 60*SECOND


from . import database
__all__.extend(database.__all__)
from .database import *

from . import mailing
__all__.extend(mailing.__all__)
from .mailing import *

from . import schedule
__all__.extend(schedule.__all__)
from .schedule import *

from . import consumer
__all__.extend(consumer.__all__)
from .consumer import *

from . import main
__all__.extend(main.__all__)
from .main import *








import time

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

