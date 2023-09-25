
import traceback, os, threading
from time import time, sleep
from loguru import logger

if bool(os.environ.get("DOCKER_IMAGE",False)):
  from api.clients import *
else:
  from maestro.api.clients import *


class Pilot( threading.Thread ):


  def __init__(self , level: str="INFO", max_retry : int=5):

    threading.Thread.__init__(self)
    logger.level(level)
    self.executors = {}
    self.db        = postgres(os.environ["DATABASE_SERVER_HOST"])
    self.schedule  = schedule(os.environ['SCHEDULE_SERVER_HOST'])
    self.postman   = postman(os.environ['POSTMAN_SERVER_HOST'])
    self.binds     = os.environ.get("EXECUTOR_SERVER_BINDS","{}")
    self.partitions= os.environ.get("PILOT_AVAILABLE_PARTITIONS").split(',')
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    self.__lock.set()
    self.max_retry = max_retry
    
    retry = 0
    logger.info("checking all necessary services...")
    while not self.ping():
      sleep(5)
      logger.info("pinging...")
      retry += 1
      if retry > self.max_retry:
        raise RuntimeError("it is not possible to power up the pilot. Abort the server.")



 
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

    start = time()
    # NOTE: only healthy executors  
    for host, executor in self.executors.items():

      # get all information about the executor
      if not executor.ping():
          logger.info( f"executor with host name {host} is not alive...")
          executor.retry += 1
          continue

      # NOTE: get all information from the current executor
      res = executor.describe()

      if res:
        # if is full, skip...
        if res.full :
          logger.debug("executor is full...")
          continue

        # how many jobs to complete the queue?
        n = res.size - res.allocated
        partition = res.partition

        logger.debug(f"getting {n} jobs from {partition} partition...")
        
        for job_id in self.schedule.get_jobs( partition, n ):
          executor.start(job_id)
      
    end = time()
    logger.debug(f"the pilot run loop took {end-start} seconds.")
    # NOTE: remove executors with max number of retries exceeded
    self.executors = {host:executor for host, executor in self.executors.items() if not executor.to_close()}
      


  def stop(self):
    self.__stop.set()
    self.schedule.stop()
    for executor in self.executors.values():
      executor().stop()


  def join_as( self, host ) -> bool:
    if host not in self.executors.keys():
      logger.debug("join a new executor into the pilot.")
      self.__lock.wait()
      self.__lock.clear()
      self.executors[host] = executor(host, max_retry=self.max_retry)
      self.__lock.set()
      return True

    return False
    
  


  
