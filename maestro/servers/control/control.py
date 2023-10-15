
import traceback, os, threading
from time import time, sleep
from loguru import logger



class Pilot( threading.Thread ):


  def __init__(self , schedule, level: str="INFO", max_retry : int=5, binds : str="{}", partitions='cpu'):

    threading.Thread.__init__(self)
    logger.level(level)
    self.executors = {}
    self.schedule  = schedule
    self.binds     = binds
    self.partitions= partitions.split(',')
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    self.__lock.set()
    self.max_retry = max_retry


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
    
  


  
