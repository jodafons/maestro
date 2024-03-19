


__all__ = ["ControlPlane"]


from time import time, sleep
from loguru import logger
from maestro.enumerations import JobStatus
from maestro import  models, MINUTES
from sqlalchemy.sql import func
import queue
import datetime


class ControlPlane:


  def __init__(self, 
               db        : models.Database,
               max_retry : int=5,
               bypass_resources_policy    : bool=False,
              ):

    #threading.Thread.__init__(self)
    #self.__stop    = threading.Event()
    self.db        = models.Database(db.host)
    self.dispatcher= {}
    self.max_retry = max_retry
    self.bypass_resources_policy = bypass_resources_policy

    self.__queue = queue.Queue(maxsize=100)



  def push_back(self, dispatcher):
    if not dispatcher.name in self.dispatcher.keys():
      self.__queue.put(dispatcher)
      return True
    return False


  #
  # Loop over control
  #
  def loop(self):

    logger.debug("starting control plane...")

    while (not self.__queue.empty()):
      dispatcher = self.__queue.get_nowait()
      dispatcher.start()
      sleep(1)
      self.dispatcher[dispatcher.name]=dispatcher
     

    start = time()

    # NOTE: remove all dispatcher that is not alive and reached the max number of retry (not ping)
    for name, dispatcher in self.dispatcher.items():
      if not dispatcher.is_alive():
        logger.warning(f"consumer from host {name} is not alive... removing...")
    self.dispatcher = { name:dispatcher for name, dispatcher in self.dispatcher.items() if dispatcher.is_alive()}

    logger.debug("put back all jobs into the queue...")

    if self.bypass_resources_policy:
      logger.warning("bypassing resource policy....")

    # NOTE: put back all jobs that reached the max number of retries for the given node 
    with self.db as session:

      # NOTE: if we have jobs with ASSIGNED and consumer, we need to check:
      # max retry
      # time in assigned status      
      for job in ( session().query(models.Job).filter(models.Job.status==JobStatus.ASSIGNED)\
                                              .filter(models.Job.consumer!="").with_for_update().all() ):
        
        if (job.consumer_retry > self.max_retry) or ( (datetime.datetime.now() - job.timer).total_seconds() > 5*MINUTES ):
          job.consumer       = ""
          job.consumer_retry = 0
          job.ping()
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
                logger.warning("current node is blocked for new jobs, skipping...")
                continue
              
              if procs > 0:

                # get n jobs from db with status assigned, that allow to this queue and was not
                # assigned to any node
                jobs = (session().query(models.Job).filter(models.Job.status==JobStatus.ASSIGNED)\
                                                   .filter(models.Job.partition==partition)\
                                                   .filter(models.Job.consumer=="")\
                                                   .order_by(models.Job.priority.desc())\
                                                   .order_by(models.Job.id).limit(procs).all() )
            

              
                logger.debug(f"we get {len(jobs)} from the database using {partition} partition...")
                
                for job_db in jobs:

                  # NOTE: JOB memory estimation
                  job_sys_memory  = session().query(func.max(models.Job.sys_used_memory)).filter(models.Job.taskid==job_db.task.id).first()[0]
                  job_gpu_memory  = session().query(func.max(models.Job.gpu_used_memory)).filter(models.Job.taskid==job_db.task.id).first()[0]
               
                  logger.debug(f"job sys memory: {job_sys_memory}")
                  logger.debug(f"job gpu memory: {job_gpu_memory}")
                  
                  if ( (sys_avail_memory - job_sys_memory) >= 0 and (gpu_avail_memory - job_gpu_memory) >= 0) or (self.bypass_resources_policy):
                    sys_avail_memory -= job_sys_memory
                    gpu_avail_memory -= job_gpu_memory
                    job_db.consumer   = name
                    job_db.ping()
                  else:
                    logger.info(f"system gpu available memory {gpu_avail_memory}")
                    logger.info(f"job gpu memory required {job_gpu_memory} MB")
                    logger.info(f"system available memory {sys_avail_memory} MB")
                    logger.info( f"job memory required {job_sys_memory} MB")
                    logger.warning(f"not available resouces for job {job_db.id}...")
                    continue

                  logger.debug(f"job {job_db.id} assigned to partition {partition} into node {name}")


                session.commit()

    end = time()
    logger.debug(f"control plane toke {end-start} seconds...")



  def stop(self):

    logger.info("stopping consumer service...")
    for dispatcher in self.dispatcher.values():
      dispatcher.stop()