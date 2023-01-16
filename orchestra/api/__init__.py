__all__ = ["remove_extension","test_locally"]




from . import DeviceParser
__all__.extend( DeviceParser.__all__ )
from .DeviceParser import *

from . import PilotParser
__all__.extend( PilotParser.__all__ )
from .PilotParser import *

from . import helper
__all__.extend( helper.__all__ )
from .helper import *

from . import TaskParser
__all__.extend( TaskParser.__all__ )
from .TaskParser import *



