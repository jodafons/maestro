
__all__ = ["Pilot"]

import traceback, threading

from time import time, sleep
from loguru import logger
from maestro.enumerations import JobStatus
from maestro import Database, schemas, models
from maestro.servers.controler import Schedule, ControlPlane
from maestro import get_hostname_from_url



class Pilot( threading.Thread ):


  def __init__(self, 
               host_url           : str, 
               db                 : models.Database,
               control_plane      : ControlPlane,
              ):

    threading.Thread.__init__(self)
    self.host_url  = host_url
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    self.__lock.set()
    self.db = models.Database(db.host)
    self.control_plane = control_plane
    self.schedule = {}



  def run(self):
    while not self.__stop.isSet():
      sleep(1)
      # NOTE wait to be set
      self.__lock.wait() 
      # NOTE: when set, we will need to wait to register until this loop is read
      self.__lock.clear()
      self.loop()
      # NOTE: allow external user to incluse nodes into the list
      self.__lock.set()


  def loop(self):

    start = time()

    # NOTE: create a schedule for each new task
    self.prepare()
  
    # TODO: control plane
    self.control_plane.loop()

    end = time()
    logger.debug(f"the pilot run loop took {end-start} seconds.")

  

  def stop(self):
    
    logger.info("stopping pilot main loop")
    self.__stop.set()

    logger.info("stopping schedule service...")
    for schedule in self.schedule.values():
      schedule.stop()
    
    self.control_plane.stop()



  def join_as( self, host_url ) -> bool:

    if host_url not in self.control_plane.dispatcher.keys():
      logger.info(f"join node {host_url} into the pilot.")
      dispatcher = Dispatcher(host_url, self.db)
      if dispatcher.configure():
        self.__lock.wait()
        self.__lock.clear()
        logger.info("attaching dispatcher into the control plane...")
        self.control_plane.push_back( dispatcher )
        self.__lock.set()
        return True
          
    return False
    
 
  def prepare(self):
    
    # create a schedule for each new task
    with self.db as session:
      tasks = session().query(models.Task).all()
      for task in tasks:
        if task.id not in self.schedule.keys():
          logger.info(f"creating a new schedule for task {task.id}")
          schedule = Schedule(task.id, self.db)
          schedule.start()
          self.schedule[task.id] = schedule
        
    self.schedule = {task_id:schedule for task_id, schedule in self.schedule.items() if schedule.is_alive()}
  



#
# A collection of slots
#
class Dispatcher(threading.Thread):

  def __init__(self, host_url            : str,
                     db                  : Database,
                     max_retry           : int=5,
                     ):
            
    threading.Thread.__init__(self)
    self.host_url  = host_url
    self.__stop    = threading.Event()
    self.db        = models.Database(db.host) 
    self.client    = schemas.client(self.host_url, "executor")
    self.max_retry = max_retry
    self.name      = host_url #get_hostname_from_url(host_url)
    

  def configure(self):
    
    if self.client.ping():
      answer = self.client.try_request("system_info" , method="get")
      consumer = answer.metadata['consumer']
      self.partition = consumer['partition']
      self.device = consumer['device']
    else:
      logger.error(f"failed to retrieve the node configuration from {self.host_url}")
      return False

    return True

  def stop(self):
    logger.info("stopping service...")
    self.__stop.set()


  def run(self):
    while not self.__stop.isSet():
      sleep(0.5)
      self.loop()
     

  def loop(self):

    logger.info("=============================================================================")    
    try:
      start = time()
      with self.db as session:

        if self.client.ping():
          self.retry = 0
          #answer = self.client.try_request("system_info" , method="get")
          #if answer.status:
          #  consumer = answer.metadata['consumer']
          #  n = consumer['avail_procs']
          #  if n > 0:
          jobs = (session().query(models.Job).filter(models.Job.status==JobStatus.ASSIGNED)\
                                                 .filter(models.Job.partition==self.partition)\
                                                 .filter(models.Job.consumer==self.name)\
                                                 .order_by(models.Job.id).all()
                 )
          jobs = [job.id for job in jobs]
          logger.debug(f"getting {len(jobs)} jobs from {self.partition} partition...")
          
          job_start=time()
          if len(jobs)>0:
            for job_id in jobs:
              self.client.try_request(f'start_job/{job_id}', method='post')
          
          job_end=time()
          logger.info(f"AKI JOAO!    start job requests toke {job_end-job_start} seconds...")

        else:
          self.retry += 1

      end = time()
      logger.info(f"server consumer toke {end-start} seconds...")

    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      self.retry+=1


    if self.retry > self.max_retry:
      logger.error("stopping dispatcher since max_retry value reached.")
      self.stop()
    logger.info("=============================================================================")    
