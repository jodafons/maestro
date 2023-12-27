


__all__ = ["ControlPlane"]

import traceback, threading

from time import time, sleep
from loguru import logger
from maestro.enumerations import JobStatus
from maestro import Database, schemas, models

from sqlalchemy.sql import func, desc
#from sqlalchemy import desc


class ControlPlane:


  def __init__(self, 
               db : models.Database,
               max_retry : int=5,
              ):

    #threading.Thread.__init__(self)
    #self.__stop    = threading.Event()
    self.db        = models.Database(db.host)
    self.dispatcher= {}
    self.max_retry = max_retry


  def push_back(self, dispatcher):
    self.dispatcher[ dispatcher.name ] = dispatcher
    dispatcher.start()


  def loop(self):

    logger.debug("starting control plane...")
    start = time()

    # NOTE: remove all dispatcher that is not alive and reached the max number of retry (not ping)
    for name, dispatcher in self.dispatcher.items():
      if not dispatcher.is_alive():
        logger.warning(f"consumer from host {name} is not alive... removing...")
    self.dispatcher = { name:dispatcher for name, dispatcher in self.dispatcher.items() if dispatcher.is_alive()}

    logger.debug("put back all jobs into the queue...")

    # NOTE: put back all jobs that reached the max number of retries for the given node 
    with self.db as session:
      
      # put jobs back in case of many retries
      for job in ( session().query(models.Job).filter(models.Job.status==JobStatus.ASSIGNED)\
                                             .filter(models.Job.consumer!="")\
                                             .filter(models.Job.consumer_retry>self.max_retry).with_for_update().all() ):
        job.consumer       = ""
        job.consumer_retry = 0
        logger.debug(f"putting job {job.id} back into the queue...")

      session.commit()

    logger.debug("assgiend jobs to the dispatcher...")

    # NOTE: assign jobs for each dispatcher
    with self.db as session:

      for name, dispatcher in self.dispatcher.items():

          logger.info(f"ranking jobs for {name} node...")
          if dispatcher.client.ping():
            answer = dispatcher.client.try_request("system_info" , method="get")
            if answer.status:

              logger.debug(f"getting info from {name}")
              consumer         = answer.metadata['consumer']
              procs            = consumer['avail_procs']
              partition        = consumer['partition']

              # NOTE: NODE memory available
              sys_avail_memory = consumer['sys_avail_memory']
              gpu_avail_memory = consumer['gpu_avail_memory']  
              
              blocked = consumer['blocked']
              if blocked:
                logger.debug("current node is blocked for new jobs, skipping...")
                continue
              
              if procs > 0:
                # get n jobs from db with status assigned, that allow to this queue and was not
                # assigned to any node
                jobs = (session().query(models.Job).filter(models.Job.status==JobStatus.ASSIGNED)\
                                               .filter(models.Job.partition==partition)\
                                               .filter(models.Job.consumer=="")\
                                               .order_by(models.Job.priority.desc())\
                                               .order_by(models.Job.id).limit(procs).all() )
            

                print([j.priority for j in jobs])
              
                logger.debug(f"we get {len(jobs)} from the database using {partition} partition...")
                
                for job_db in jobs:

                  print(f'JOB {job_db.id} - TASK {job_db.taskid}')

                  # NOTE: JOB memory estimation
                  job_sys_memory  = session().query(func.max(models.Job.sys_used_memory)).filter(models.Job.taskid==job_db.task.id).first()[0]
                  job_gpu_memory  = session().query(func.max(models.Job.gpu_used_memory)).filter(models.Job.taskid==job_db.task.id).first()[0]
                  print('AKI JOAO')
                  print(job_sys_memory)
                  print(job_gpu_memory)
               
                  if (sys_avail_memory - job_sys_memory) >= 0 and (gpu_avail_memory - job_gpu_memory) >= 0:
                    sys_avail_memory -= job_sys_memory
                    gpu_avail_memory -= job_gpu_memory
                    job_db.consumer      = name
                  else:
                    logger.warning(f"not available resouces for job {job_db.id}...")
                    continue

                  logger.debug(f"job {job_db.id} assigned to partition {partition} into node {name}")


                session.commit()

    end = time()
    logger.info(f"control plane toke {end-start} seconds...")



  def stop(self):

    #self.__stop.set()
    logger.info("stopping consumer service...")
    for dispatcher in self.dispatcher.values():
      dispatcher.stop()