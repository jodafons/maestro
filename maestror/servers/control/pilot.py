
__all__ = ["Pilot"]

import traceback, os, threading
from time import time, sleep
from maestror.servers.control.schedule import Schedule
from maestror import schemas
from loguru import logger




class Pilot( threading.Thread ):


  def __init__(self, 
               host               : str, 
               schedule           : Schedule, 
               max_retry          : int=5 
              ):

    threading.Thread.__init__(self)
    self.host      = host
    self.nodes     = {}
    self.schedule  = schedule
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    self.__lock.set()
    self.max_retry = max_retry


  def run(self):

    while not self.__stop.isSet():
      sleep(10)
      # NOTE wait to be set
      self.__lock.wait() 
      # NOTE: when set, we will need to wait to register until this loop is read
      self.__lock.clear()
      self.loop()
      # NOTE: allow external user to incluse nodes into the list
      self.__lock.set()


  def loop(self):

    start = time()
    # NOTE: only healthy nodes  

    self.schedule.loop()

    for host in self.nodes.keys():

      node = schemas.client(host, "executor")

      # get all information about the executor
      if not node.ping():
          logger.info( f"node with host name {host} is not alive...")
          self.nodes[host] += 1
          continue

      # NOTE: get all information from the current executor
      answer = node.try_request("system_info" , method="get")
      if answer.status:
        consumer = answer.metadata['consumer']
        partition = consumer['partition']; n = 10
        logger.debug(f"getting {n} jobs from {partition} partition...")
        for job_id in self.schedule.get_jobs( partition, n ):
          if node.try_request(f'start_job/{job_id}', method='post').status:
            logger.debug(f'start job sent well to the consumer node.')

    end = time()
    logger.debug(f"the pilot run loop took {end-start} seconds.")

    # NOTE: remove nodes with max number of retries exceeded
    self.nodes = {host:retry for host, retry in self.nodes.items() if retry < self.max_retry}
      

  def stop(self):
    self.__stop.set()
    logger.info("stopping schedule service...")
    self.schedule.stop()


  def join_as( self, host ) -> bool:

    if host not in self.nodes.keys():
      logger.info(f"join node {host} into the pilot.")
      self.__lock.wait()
      self.__lock.clear()
      self.nodes[host] = 0
      self.__lock.set()
      return True

    return False
    
  
  def system_info(self):

    info = {}
    for host in self.nodes.keys():
      node = schemas.client(host, "executor")
      answer = node.try_request("system_info" , method="get")
      if answer.status:
        info[host] = answer.metadata
    return info