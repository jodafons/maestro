
import traceback, os
import threading
from time import time, sleep
from loguru import logger

try:
  from api.clients import *
  from api.postgres import *
except:
  from maestro.api.clients import *




class Pilot( threading.Thread ):


  def __init__(self , level: str="INFO"):

    threading.Thread.__init__(self)
    logger.level(level)
    self.executors = {}
    self.db        = postgres(os.environ["DATABASE_SERVER_HOST"])
    self.schedule  = schedule(os.environ['SCHEDULE_SERVER_HOST'])
    self.postman   = postman(os.environ['POSTMAN_SERVER_HOST'])
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    self.__lock.set()
    sleep(5)
    if not self.ping():
      logger.critical("It is not possible to power up the pilot. Abort the server.")

 
  def ping(self):

      logger.debug("ping schedule...")
      if self.schedule.ping():
          logger.debug("pong schedule.")
      else:
          logger.error("schedule server is out. Not possible to power up the pilot without this service.")
          return False

      logger.debug("ping postman...")
      if self.postman.ping():
          logger.debug("pong postman.")
      else:
          logger.error("postman server is out. Not possible to power up the pilot without this service.")
          return False

      return True


  #
  # Run thread
  #
  def run(self):

    while not self.__stop.isSet() and self.ping():
      sleep(10)
      # NOTE wait to be set
      self.__lock.wait() 
      # NOTE: when set, we will need to wait to register until this loop is read
      self.__lock.clear()
      self.loop()
      # NOTE: allow external user to incluse executors into the list
      self.__lock.set()


  #
  # Run
  #
  def loop(self):

    get_jobs = {"cpu":self.schedule.get_cpu_jobs, "gpu":self.schedule.get_gpu_jobs}
    start = time()
    # NOTE: only healthy executors  
    for host, executor in self.executors.items():
      # get all information about the executor
      if not executor.ping():
          logger.info( f"executor with host name {host} is not alive...")
          executor.retry += 1
          continue
      res = executor().describe()
      if res:
        # if is full, skip...
        if res.full :
          logger.debug("Executor is full...")
          continue
        # how many jobs to complete the queue?
        for job_id in get_jobs["gpu" if res.device>=0 else "cpu"]( res.size - res.allocated ):
          executor().start_job(job_id)
      
    end = time()
    logger.debug(f"The pilot run loop took {end-start} seconds.")
    # NOTE: remove executors with max number of retries exceeded
    self.executors = {host:executor for host, executor in self.executors.items() if not executor.to_close()}
      


  def stop(self):
    self.__stop.set()
    self.schedule.stop()
    for executor in self.executors.values():
      executor().stop()


  def connect_as( self, host ):
    if host not in self.executors.keys():
        logger.debug("join a new executor into the pilot.")
        self.__lock.wait()
        self.__lock.clear()
        # NOTE: 
        class Executor:
          def __init__(self, host, device, max_retry=5):
              self.api = executor(host)
              self.device = self.api.describe().device
              self.max_retry = max_retry
              self.retry = 0
          def __call__(self):
            return self.api
          def to_close(self):
            return self.retry>self.max_retry
        # Add into the pilot
        self.executors[host] = Executor(host)
        self.__lock.set()
        return True
    else:
        logger.warning(f"executor with name {host} exist into the pilot list.")
        return False

