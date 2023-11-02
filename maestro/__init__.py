
__all__ = ["system_info"]

import psutil, socket, platform, cpuinfo
import GPUtil as gputil

def system_info():

    # NOTE: all memory values in MB
    uname = platform.uname()
    svmem = psutil.virtual_memory()
    devices = []
    for gpu in gputil.getGPUs():
      device = {
        'name'         : gpu.name,
        'id'           : gpu.id,
        'total' : gpu.memoryTotal,
        'used'  : gpu.memoryUsed,
        'avail' : gpu.memoryFree,
        'usage' : (gpu.memoryUsed/gpu.memoryTotal) * 100,
      }
      devices.append(device)

    memory_info = {
      'total' : svmem.total/(1024**2),
      'avail' : svmem.available/(1024**2),
      'used'  : svmem.used/(1024**2),
      'usage' : svmem.percent,
    }

    cpu_info = {
      'processor'  : cpuinfo.get_cpu_info()["brand_raw"],
      'count'      : psutil.cpu_count(logical=True),
      'usage'      : psutil.cpu_percent(),
    }


    system_info = {
      'system'     : uname.system,
      'version'    : uname.version,
      'machine'    : uname.machine,
      'release'    : uname.release,
    }

    return { # return the node information
      'node'       : uname.node,
      'ip_address' : socket.gethostbyname(socket.gethostname()),
      'system'     : system_info,
      'memory'     : memory_info,
      'cpu'        : cpu_info,
      'gpu'        : devices,
    }



from . import enumerations
__all__.extend( enumerations.__all__ )
from .enumerations import *

from . import models
__all__.extend( models.__all__ )
from .models import *

from . import schemas
__all__.extend( schemas.__all__ )
from .schemas import *

#from . import parsers
#__all__.extend( parsers.__all__ )
#from .parsers import *

#from . import standalone
#__all__.extend( standalone.__all__ )
#from .standalone import *


from . import servers
__all__.extend( servers.__all__ )
from .servers import *