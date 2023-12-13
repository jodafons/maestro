
__all__ = ["Consumer"]

import os, traceback, time, threading, queue
import mlflow
from sqlalchemy import func


from time import time, sleep
from loguru import logger
from maestro.enumerations import JobStatus, TaskStatus
from maestro import Database, schemas, models
from maestro import get_system_info, get_gpu_memory_info, get_memory_info, GB
from maestro.servers.executor.job import Job
from mlflow.tracking import MlflowClient



#
# A collection of slots
#
class Consumer(threading.Thread):

  def __init__(self, host_url            : str,
                     db                  : Database,
                     device              : int=-1, 
                     timeout             : int=60, 
                     max_retry           : int=5, 
                     partition           : str='cpu',
                     max_procs           : int=os.cpu_count(),
                     reserved_memory     : float=4*GB,
                     reserved_gpu_memory : float=2*GB,
                     ):
            
    threading.Thread.__init__(self)
    self.host_url  = host_url    
    self.partition = partition
    self.timeout   = timeout
    self.max_retry = max_retry
    self.device    = device
    self.db_host   = db.host
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    self.__lock.set() 


    # getting system values
    _, sys_avail_memory, _, _ = get_memory_info()
    _, gpu_avail_memory, _, _ = get_gpu_memory_info(self.device)
    self.max_procs            = max_procs
    self.reserved_memory      = sys_avail_memory - reserved_memory
    self.reserved_gpu_memory  = gpu_avail_memory - reserved_gpu_memory


    db = models.Database(self.db_host) 
    with db as session:
      # get the server host location from the database everytime since this can change
      self.server_url   = session.get_environ( "PILOT_SERVER_URL" )
      logger.info(f"pilot url     : {self.server_url}"  )

      # get the server host location from the database everytime since this can change
      self.tracking_url = session.get_environ("TRACKING_SERVER_URL")
      if self.tracking_url != "":
        logger.info(f"tracking url  : {self.tracking_url}")
        mlflow.set_tracking_uri(self.tracking_url)

    self.queue_slots         = queue.Queue(maxsize=max_procs)
    self.queue_child_threads = queue.Queue(maxsize=max_procs*10)

    self.jobs          = {}
    self.child_threads = []


  def stop(self):
    self.__stop.set()
    for slot in self.jobs.values():
      slot.stop()


  def __len__(self):
    return len(self.jobs.keys())


  def run(self):

    while (not self.__stop.isSet()):

      sleep(0.5)
      server = schemas.client( self.server_url, 'pilot')
      answer = server.try_request(f'join', method="post", body=schemas.Request( host=self.host_url     ).json())
      if answer.status:
        logger.debug(f"connected with {answer.host}")
        self.loop()
      else:
        logger.error("not possible to connect with the server...")
 


  def start_job_thread( self, job_id : int ):
    thread = threading.Thread( target=self.start_job, args=(job_id,) )
    thread.start()
    self.queue_child_threads.put(thread)
    return True
  

  def blocked(self):
    return any([slot.job.testing for slot in self.jobs.values()])


  def start_job( self, job_id : int):

    # NOTE: If we have some testing job into the stack, we need to block the entire consumer.
    # testing jobs must run alone since we dont know how much resouces will be used.
    blocked = any([slot.job.testing for slot in self.jobs.values()])

    # NOTE: check if we have a test job waiting to run or running...
    if blocked:
      logger.warning("The consumer is blocked because we have a testing job waiting to run.")
      logger.info(f"start job toke {end-start} seconds")
      return False

    start = time()

    db = models.Database(self.db_host) 

    with db as session:

      job_db = session.get_job(job_id, with_for_update=False)

      if job_id in self.jobs.keys():
        logger.warning(f"Job {job_id} exist into the consumer. Not possible to include here.")
        job_db.consumer_retry += 1
        session.commit()
        return False


      # NOTE: optimize query to retrieve the memory peak from all jobs for the current task
      sys_used_memory  = session().query(func.max(models.Job.sys_used_memory)).filter(models.Job.taskid==job_db.task.id).first()[0]
      gpu_used_memory  = session().query(func.max(models.Job.gpu_used_memory)).filter(models.Job.taskid==job_db.task.id).first()[0]
      
      # NOTE: check if the consumer attend some resouces criteria to run the current job
      if (not self.check_resources(sys_used_memory, gpu_used_memory)):
        logger.warning(f"Job {job_id} estimated resources not available at this consumer.")
        job_db.consumer_retry += 1
        session.commit()
        return False
      
      binds = job_db.get_binds()
      task_db = job_db.task
      job = Job(  
             job_db.id,
             job_db.task.name,
             job_db.command,
             job_db.workarea,
             image=job_db.image,
             virtualenv=job_db.virtualenv,
             device=self.device,
             binds=binds,
             testing=job_db.task.status == TaskStatus.TESTING,
             run_id=job_db.run_id,
             tracking_url=self.tracking_url ,
             envs=job_db.get_envs(),
             )
      job_db.status = JobStatus.PENDING
      job_db.ping()
      if self.tracking_url!="":
        tracking = MlflowClient( self.tracking_url  )
        run_id = tracking.create_run(experiment_id=task_db.experiment_id, 
                                     run_name=job_db.name).info.run_id
        tracking.log_artifact(run_id, job_db.inputfile)
        job.run_id = run_id
        logger.debug(f"tracking job id {job_db.id} as run_id {run_id}...")
        
      
      slot = Slot(job.id, db, job, sys_used_memory, gpu_used_memory, self.tracking_url)
      self.queue_slots.put(slot)
      session.commit()
    
    end = time()
    logger.info(f"start job toke {end-start} secondss")

    return True



  def loop(self):

    start = time()

    while not self.queue_slots.empty():
      try:
        slot = self.queue_slots.get_nowait()
        self.jobs[slot.job_id] = slot
      except:
        continue

    while not self.queue_child_threads.empty():
      try:
        thread = self.queue_child_threads.get_nowait()
        self.child_threads.append(thread)
      except:
        continue

    for slot in self.jobs.values():

      logger.info(f"job id : {slot.job.id}, is_alive? {slot.is_alive()}, job.status : {slot.job.status()}")
      if slot.job.testing:
        if (not slot.lock):
          if (len(self.jobs)==1):
            logger.info(f"starting testing job with id {slot.job.id}")
            slot.start()
          else:
            logger.info("job testing waining consumer to be cleaner...")
      else:
        if not slot.lock:
          logger.info(f"starting job with if {slot.job.id}")
          slot.start()


    self.jobs = { job_id:slot for job_id, slot in self.jobs.items() if not slot.job.closed()}
    end = time()
    logger.info(f"loop job toke {end-start} seconds")


    self.child_threads = [ thread for thread in self.child_threads if thread.is_alive()]


  def check_resources(self, sys_used_memory : float, gpu_used_memory : float):

    start = time()
    nprocs = len(self.jobs)

    if  nprocs > self.max_procs:
      logger.warning("Number of procs reached the limit stablished.")
      end = time()
      #logger.info(f"check_resources toke {end-start} seconds")
      return False




    
    # NOTE: NODE memory estimation
    sys_avail_memory = self.reserved_memory - sum([slot.sys_memory for slot in self.jobs.values()])
    gpu_avail_memory = self.reserved_gpu_memory - sum([slot.gpu_memory for slot in self.jobs.values()])
    sys_avail_memory = 0 if sys_avail_memory < 0 else sys_avail_memory
    gpu_avail_memory = 0 if gpu_avail_memory < 0 else gpu_avail_memory
    

    #logger.debug(f"task:")
    #logger.debug(f"      system used memory  : {sys_used_memory} MB")
    #logger.debug(f"      gpu used memory     : {gpu_used_memory} MB")
    #logger.debug("system now:")
    #logger.debug(f"      system avail memory : {sys_avail_memory} MB")
    #logger.debug(f"      gpu avail memory    : {gpu_avail_memory} MB")


    if sys_avail_memory == 0:
      logger.warning("System memory node usage reached the limit stablished.")
      end = time()
      #logger.info(f"check_resources toke {end-start} seconds")
      return False

    if (self.device >= 0) and (gpu_avail_memory == 0):
      logger.warning("GPU memory node usage reached the limit stablished.")
      end = time()
      #logger.info(f"check_resources toke {end-start} seconds")
      return False

    #logger.debug(f"Job system used memory : {sys_used_memory} ({sys_avail_memory}) MB")
    # check if we have memory to run this workload
    if (sys_used_memory >= 0) and (sys_used_memory > sys_avail_memory):
      logger.warning("Not available memory to run this job into this consumer.")
      end = time()
      #logger.info(f"check_resources toke {end-start} seconds")
      return False  

    #logger.debug(f"Job gpu used memory    : {gpu_used_memory} ({gpu_avail_memory}) MB")
    # check if we have gpu memory to run this workload
    if (self.device >= 0) and (gpu_used_memory >= 0) and (gpu_used_memory > gpu_avail_memory):
      logger.warning("Not available GPU memory to run this job into this consumer.")
      end = time()
      #logger.info(f"check_resources toke {end-start} seconds")
      return False
  
    end = time()
    logger.info(f"check_resources toke {end-start} seconds")
    # if here, all resources available for this workload
    return True



  def system_info(self):

    d = get_system_info()
    sys_avail_memory = self.reserved_memory - sum([slot.sys_memory for slot in self.jobs.values()])
    gpu_avail_memory = self.reserved_gpu_memory - sum([slot.gpu_memory for slot in self.jobs.values()])
    blocked = any([slot.job.testing for slot in self.jobs.values()])

    d['consumer'] = {
          'url'              : self.host_url,
          'partition'        : self.partition,
          'device'           : self.device,
          'blocked'          : blocked,
          'max_procs'        : self.max_procs,
          'max_system_memory': self.reserved_memory,
          'max_gpu_memory'   : self.reserved_gpu_memory,
          'avail_procs'      : self.max_procs - len(self.jobs.keys()),
          'sys_avail_memory' : 0 if sys_avail_memory < 0 else sys_avail_memory, # NOTE: NODE memory
          'gpu_avail_memory' : 0 if gpu_avail_memory < 0 else gpu_avail_memory, # NOTE: NODE memory
        }

    d['gpu'] = d['gpu'][self.device]
    return d





class Slot(threading.Thread):

  def __init__(self, job_id, db, job, sys_memory, gpu_memory, tracking_url):
    threading.Thread.__init__(self)
    self.job_id = job_id
    self.job = job
    self.sys_memory = sys_memory
    self.gpu_memory = gpu_memory
    self.tracking_url = tracking_url
    self.db = models.Database(db.host)
    self.__stop = threading.Event()
    self.lock = False


  def run(self):
    self.lock = True
    while not self.__stop.isSet():
      sleep(0.5)
      try:
        self.loop()
      except Exception as e:
        traceback.print_exc()
        logger.error(e)


  def loop(self):

    start = time()
    with self.db as session:

      logger.debug(f"checking job id {self.job.id}")
      job_db   = session.get_job(self.job.id, with_for_update=True)
      
      tracking = MlflowClient( self.tracking_url  ) if self.tracking_url != "" else None

      # NOTE: kill job option only available with database by external trigger
      if job_db.status == JobStatus.KILL:
        logger.debug("Kill job from database...")
        self.job.kill()

      if self.job.status() == JobStatus.PENDING:
        job_db.ping()
        logger.debug(f"Job {self.job.id} is a single job...")
        if self.job.run(tracking):
          if tracking:
            tracking.log_dict(self.job.run_id, get_system_info(pretty=True), "system.json")
          logger.debug(f'Job {self.job.id} is RUNNING.')
          job_db.status = JobStatus.RUNNING
        else:
          logger.debug(f'Job {self.job.id} is BROKEN.')
          job_db.status = JobStatus.BROKEN
          self.job.to_close()

      elif self.job.status() is JobStatus.FAILED:
        logger.debug(f'Job {self.job.id} is FAILED.')
        job_db.status = JobStatus.FAILED
        self.job.to_close()

      elif self.job.status() is JobStatus.KILLED:
        logger.debug(f'Job {self.job.id} is KILLED.')
        job_db.status = JobStatus.KILLED
        self.job.to_close()
        
      elif self.job.status() is JobStatus.RUNNING:
        logger.debug(f'Job {self.job.id} is RUNNING.')
        # NOTE: update peak values for the current job
        cpu_percent, sys_used_memory, gpu_used_memory = self.job.proc_stat()
        job_db.cpu_percent      = max(cpu_percent       , job_db.cpu_percent      )
        job_db.sys_used_memory  = max(sys_used_memory   , job_db.sys_used_memory  )
        job_db.gpu_used_memory  = max(gpu_used_memory   , job_db.gpu_used_memory  )
        logger.debug(f"Job {self.job.id} consuming {job_db.sys_used_memory } MB of memory, {job_db.gpu_used_memory} "+ 
                     f"MB of GPU memory and {job_db.cpu_percent} of CPU.")
        job_db.ping()
  

        if tracking:
          # NOTE: log metrics into mlflow database
          tracking.log_metric(self.job.run_id, "sys_used_memory", job_db.sys_used_memory )
          tracking.log_metric(self.job.run_id, "gpu_used_memory", job_db.gpu_used_memory )
          tracking.log_metric(self.job.run_id, "cpu_percent"    , job_db.cpu_percent     )

      elif self.job.status() is JobStatus.COMPLETED:
        logger.debug(f'Job {self.job.id} is COMPLETED.')
        job_db.status = JobStatus.COMPLETED
        self.job.to_close()

      # update job status into the tracking server
      if tracking:
        tracking.set_tag(self.job.run_id, "Status", job_db.status)
        # add job log as artifact into the tracking server
        if self.job.closed():
          tracking.log_artifact(self.job.run_id, self.job.logpath)
      
      # update job into the database
      logger.debug("commit all changes into the database...")
      session.commit()


    if self.job.closed():
      self.stop()

    end = time()
    logger.debug(f"Run stage toke {round(end-start,4)} seconds")



  def stop(self):
    logger.info("stopping service...")
    self.__stop.set()