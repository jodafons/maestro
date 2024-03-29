
__all__ = ["system_info", "get_memory_info", "get_gpu_memory_info", "get_hostname_from_url",
           "Server", "GB", "SECONDS", "MINUTES"]

import psutil, socket, platform, cpuinfo
import GPUtil as gputil
from time import sleep
import subprocess

import warnings
warnings.filterwarnings("ignore")

GB = 1024

SECONDS=1
MINUTES=60*SECONDS


def convert_bytes(size):
    for x in ['MB', 'GB', 'TB']:
        if size < 1024.0:
            return "%3.1f %s" % (size, x)
        size /= 1024.0
    return size

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]


def get_hostname_from_url( url ):
  return url.split('@')[1].split(':')[0]

def get_memory_info(pretty=False):
    svmem = psutil.virtual_memory()
    total = convert_bytes( svmem.total/(1024**2) ) if pretty else svmem.total/(1024**2)
    avail = convert_bytes( svmem.available/(1024**2) ) if pretty else svmem.available/(1024**2)
    used  = convert_bytes( svmem.used/(1024**2) ) if pretty else svmem.used/(1024**2)
    usage = svmem.percent,
    return total, avail, used, usage


def get_gpu_memory_info(device=0, pretty=False):
    gpus = gputil.getGPUs()
    if gpus:
      gpu = gpus[device]
      total  = convert_bytes(gpu.memoryTotal) if pretty else gpu.memoryTotal
      used   = convert_bytes(gpu.memoryUsed) if pretty else gpu.memoryUsed
      avail  = convert_bytes(gpu.memoryFree) if pretty else gpu.memoryFree
      usage  = (gpu.memoryUsed/gpu.memoryTotal) * 100
      return total, avail, used, usage
    else:
      return 0,0,0,0


def get_system_info(pretty=False):

    hostname = socket.gethostname()

    # NOTE: all memory values in MB
    uname = platform.uname()
    svmem = psutil.virtual_memory()
    devices = []
    for gpu in gputil.getGPUs():
      device = { # returns always in MB
        'name'  : gpu.name,
        'id'    : gpu.id,
        'total' : convert_bytes(gpu.memoryTotal) if pretty else gpu.memoryTotal,
        'used'  : convert_bytes(gpu.memoryUsed) if pretty else gpu.memoryUsed,
        'avail' : convert_bytes(gpu.memoryFree) if pretty else gpu.memoryFree,
        'usage' : (gpu.memoryUsed/gpu.memoryTotal) * 100,
      }
      devices.append(device)

    memory_info = { # alsways in bytes, convert to MB
      'total' : convert_bytes( svmem.total/(1024**2) ) if pretty else svmem.total/(1024**2),
      'avail' : convert_bytes( svmem.available/(1024**2) ) if pretty else svmem.available/(1024**2),
      'used'  : convert_bytes( svmem.used/(1024**2) ) if pretty else svmem.used/(1024**2),
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


    #iname = [ name for name in ni.interfaces() if 'enp' in name][0]

    network_info = {
      'ip_address' : get_ip_address(),
    }

    return { # return the node information
      'hostname'   : hostname,
      'network'    : network_info,
      'system'     : system_info,
      'memory'     : memory_info,
      'cpu'        : cpu_info,
      'gpu'        : devices,
    }







class Server:

  def __init__(self , command ):
    self.command = command

  def start(self):
    print(self.command)
    self.__proc = subprocess.Popen(self.command, shell=True)
    sleep(1) # NOTE: wait for 2 seconds to check if the proc really start.
    self.__proc_stat = psutil.Process(self.__proc.pid)

  def is_alive(self):
    return True if (self.__proc and self.__proc.poll() is None) else False

  def stop(self):
    if self.is_alive():
      children = self.__proc_stat.children(recursive=True)
      for child in children:
        p=psutil.Process(child.pid)
        p.kill()
      self.__proc.kill()
      self.killed=True
    else:
      self.killed=True



from . import enumerations
__all__.extend( enumerations.__all__ )
from .enumerations import *

from . import models
__all__.extend( models.__all__ )
from .models import *

from . import schemas
__all__.extend( schemas.__all__ )
from .schemas import *

from . import clients
__all__.extend( clients.__all__ )
from .clients import *

from . import console
__all__.extend( console.__all__ )
from .console import *

from . import servers
__all__.extend( servers.__all__ )
from .servers import *