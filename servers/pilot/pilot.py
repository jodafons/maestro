
import traceback, os
import threading

from time import time, sleep
from models import Task, Job
from sqlalchemy import and_
from loguru import logger
from enumerations import JobStatus, TaskStatus, TaskTrigger
from api.client_executor import client_executor

class consumer:
    def __init__(self, host, device, max_retry=5):
        self.api = client_executor(host)
        self.device = device
        self.max_retry = max_retry
        self.retry = 0

    def __call__(self):
        return self.api
    
    def is_alive(self):
        return self.api.is_alive()

    def to_close(self):
        return self.retry>self.max_retry

class Pilot( threading.Thread ):


  def __init__(self, db, schedule, mailing ):

    threading.Thread.__init__(self)

    self.executors = {}
    self.db = db
    self.schedule = schedule
    self.mailing = mailing
    self.__stop = threading.Event()
    self.__lock = threading.Event()
    logger.info("Checking for services...")
    sleep(10)
    if not self.ping():
      logger.critical("It is not possible to power up the pilot. Abort the server.")

 



  def ping(self):

      #
      # TODO: Need to check database connection...
      #
      #logger.info("Checking if database is alive...")
      #if db.is_alive():
      #  logger.info("The databse server is connected.")
      #else:
      #  logger.critical("The database server is out. Not possible to power up the pilot without this service.")


      logger.info("Checking if the schedule server is alive...")
      if self.schedule.is_alive():
          logger.info("The schedule server is connected.")
      else:
          logger.error("The schedule server is out. Not possible to power up the pilot without this service.")
          return False

      logger.info("Checking if the mailing server is alive...")
      if self.mailing.is_alive():
          logger.info("The mailing server is connected.")
      else:
          logger.error("The mailing server is out. Not possible to power up the pilot without this service.")
          return False

      return True

  #
  #
  #
  def run(self):


    while not self.__stop.isSet() and self.ping():


      sleep(5)
      # NOTE: when set, we will need to wait to register until this loop is read
      self.__lock.clear()
      start = time()

      logger.info("Running the schedule...")
      if not self.schedule.run():
        logger.error("Something happining into the schedule service...")
        continue

      logger.info("Checking for all executors...")
    
      # NOTE: check executor healthy
      for host, executor in self.executors.items():
        if executor.is_alive():
            executor.retry = 0
        else:
            logger.info( f"The executor with host name {host} is not alive...")
            executor.retry += 1
        # Just print all executors that will be removed next
        if executor.to_close():
            logger.info(f"The executor with name {host} will be remoded from the pilot.")

      # NOTE: remove executors with max number of retries exceeded
      self.executors = {host:executor for host, executor in self.executors if not executor.to_close()}
        

      # NOTE: only available executors  
      for hostname, executor in self.executors:

        #n = executor.size() - executor.allocated()
        # NOTE: get n jobs from the database
        #jobs = self.schedule.jobs(n)

        #while executor.available() and len(jobs) > 0:
        #  executor.start(jobs.pop())
        
        #executor.run()
        pass
      
      end = time()
      logger.info(f"The pilot run loop took {end-start} seconds.")

      # NOTE: allow external user to incluse executors into the list
      self.__lock.set()


  def stop(self):
    self.__stop.set()




  def register( self, host, device ):
    if host not in self.executors.keys():
        logger.info("Creating executor into the pilot")
        self.__lock.wait()
        self.executors[host] = consumer(host, device)
    else:
        logger.info(f"Executor with name {host} exist into the executor list.")



if __name__ == "__main__":
  pass
