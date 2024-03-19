
__all__ = ["Pilot"]

import traceback, threading, datetime

from time import time, sleep
from loguru import logger
from maestro.enumerations import JobStatus
from maestro import Database, schemas, models, MINUTES
from maestro.models import Job
from maestro.servers.controler import Schedule, ControlPlane




class Pilot( threading.Thread ):


  def __init__(self, 
               host_url           : str, 
               db                 : models.Database,
               control_plane      : ControlPlane,
              ):

    threading.Thread.__init__(self)
    self.host_url      = host_url
    self.__stop        = threading.Event()
    self.db            = models.Database(db.host)
    self.control_plane = control_plane
    self.schedule = {}



  def run(self):
 
    # NOTE: This will run only at the begginer of the master node to clean-up all jobs with ASSIGNED status and consumer.
    with self.db as session:
      logger.debug("Treat jobs with status asigned assigned from the last execution...")
      jobs = session().query(Job).filter( Job.status==JobStatus.ASSIGNED ).filter(Job.consumer!="").with_for_update().all()
      for job in jobs:
        if (datetime.datetime.now() - job.timer).total_seconds() > 5*MINUTES :
          logger.info(f"resetting job {job.id}...")
          job.consumer=""
          job.consumer_retry = 0
          job.ping()
      session.commit()


    while not self.__stop.isSet():
      sleep(1)
      self.loop()


  def loop(self):

    start = time()

    # NOTE: create a schedule for each new task
    self.prepare()
  
    # TODO: control plane
    self.control_plane.loop()

    end = time()
    logger.info(f"the pilot run loop took {end-start} seconds.")

  

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
        logger.info(f"attaching {host_url} dispatcher into the control plane...")
        return self.control_plane.push_back( dispatcher )
          
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
    self.client    = schemas.client(self.host_url, "runner")
    self.max_retry = max_retry
    self.name      = host_url #get_hostname_from_url(host_url)
    self.retry     = 0

  def configure(self):
    logger.info(f"configure {self.host_url}...")
    if self.client.ping():
      answer = self.client.try_request("system_info" , method="get")
      consumer = answer.metadata['consumer']
      self.partition = consumer['partition']
      self.device = consumer['device']
    else:
      logger.error(f"failed to retrieve the node configuration from {self.host_url}")
      return False
    logger.info("configuration done...")
    return True


  def stop(self):
    logger.info("stopping service...")
    self.__stop.set()


  def run(self):
    while not self.__stop.isSet():
      sleep(0.5)
      self.loop()
     

  def loop(self):
    logger.info(f"========================= {self.name} ==============================")
    try:
      start = time()
      with self.db as session:

        if self.client.ping():
          self.retry = 0

          answer = self.client.try_request("system_info" , method="get")
          if answer.status:
            consumer = answer.metadata['consumer']
            n = consumer['avail_procs']
            if n > 0:
              jobs = (session().query(models.Job).filter(models.Job.status==JobStatus.ASSIGNED)\
                                                 .filter(models.Job.partition==self.partition)\
                                                 .filter(models.Job.consumer==self.name)\
                                                 .order_by(models.Job.id).limit(n).all()
              )
              jobs = [job.id for job in jobs]
              logger.info(f"getting {len(jobs)} jobs from {self.partition} partition...")
              if len(jobs)>0:
                job_start=time()
                body = schemas.Request(host=self.host_url, metadata={'jobs':jobs})
                self.client.try_request(f'start_job', method='post', body=body.json())
                job_end=time()
                logger.info(f"start job requests toke {job_end-job_start} seconds...")

        else:
          self.retry += 1

      end = time()
      logger.info(f"dispatcher toke {end-start} seconds...")

    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      self.retry+=1


    if self.retry > self.max_retry:
      logger.error("stopping dispatcher since max_retry value reached.")
      self.stop()

    logger.info("=======================================================")
