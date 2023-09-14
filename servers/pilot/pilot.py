
import traceback, os
import threading

from time import time, sleep
from sqlalchemy import and_
from loguru import logger

try:
  from models import Task, Job
  from enumerations import JobStatus, TaskStatus, TaskTrigger
  from api.clients import *
except:
  from maestro.models import Task, Job
  from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger
  from maestro.api.clients import *

class Consumer:

    def __init__(self, host, device, max_retry=5):
        self.host = host
        self.api = executor(host)
        self.device = device
        self.max_retry = max_retry
        self.retry = 0

    def status(self):
        return self.api.status()
    
    def ping(self):
        return self.api.ping()

    def to_close(self):
        return self.retry>self.max_retry

    def start(self, job_id):
      return self.api.start( job_id )
      
    def run(self):
      self.api.run()




class Pilot( threading.Thread ):


  def __init__(self ):

    threading.Thread.__init__(self)
    self.executors = {}
    self.db        = database(os.environ["DATABASE_SERVER_HOST"])
    self.schedule  = schedule(os.environ['SCHEDULE_SERVER_HOST'])
    self.postman   = postman(os.environ['POSTMAN_SERVER_HOST'])
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    logger.info("Checking for services...")
    sleep(10)
    if not self.ping():
      logger.critical("It is not possible to power up the pilot. Abort the server.")

 



  def ping(self):

      logger.info("Checking database server is alive...")
      if self.db.ping():
        logger.info("The database server is connected.")
      else:
        logger.error("The database server is out. Not possible to power up without this servive.")
        return False

      logger.info("Checking if the schedule server is alive...")
      if self.schedule.ping():
          logger.info("The schedule server is connected.")
      else:
          logger.error("The schedule server is out. Not possible to power up the pilot without this service.")
          return False

      logger.info("Checking if the postman server is alive...")
      if self.postman.ping():
          logger.info("The postman server is connected.")
      else:
          logger.error("The postman server is out. Not possible to power up the pilot without this service.")
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
      
      # NOTE: remove executors with max number of retries exceeded
      self.executors = {host:executor for host, executor in self.executors.items() if not executor.to_close()}
        
      
      start = time()


      logger.info("Checking for all executors...")
    
      # NOTE: only healthy executors
      executors = {}

      # NOTE: check executor healthy
      for host, executor in self.executors.items():
        if executor.ping():
            executor.retry = 0
            executors[host] = executor
        else:
            logger.info( f"The executor with host name {host} is not alive...")
            executor.retry += 1
        # Just print all executors that will be removed next
        if executor.to_close():
            logger.info(f"The executor with name {host} will be remoded from the pilot.")


      # NOTE: only healthy executors  
      for host, executor in executors.items():
        # get all information about the executor
        res = executor.status()
        if res:
          # if is full, skip...
          if res.full :
            logger.info("Executor is full...")
            continue
          # how many jobs to complete the queue?
          n = res.size - res.allocated
          # NOTE: get n jobs from the database
          for job in self.db.get_n_jobs(n):
            executor.start(job.id)
          #executor.run()
        

      end = time()
      logger.info(f"The pilot run loop took {end-start} seconds.")

      # NOTE: allow external user to incluse executors into the list
      self.__lock.set()


  def stop(self):
    self.__stop.set()




  def connect( self, host, device ):
    if host not in self.executors.keys():
        logger.info("Creating executor into the pilot")
        self.__lock.wait()
        self.executors[host] = Consumer(host, device)
        return True
    else:
        logger.info(f"Executor with name {host} exist into the executor list.")
        return False

