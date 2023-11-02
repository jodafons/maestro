
__all__ = ["Job", "Consumer"]

import os, subprocess, traceback, time, sys, threading, psutil, nvsmi
from time import time, sleep
from loguru import logger
from pprint import pprint
from copy import copy
from maestro.enumerations import JobStatus, TaskStatus
from maestro import Database, schemas, models, system_info


SYS_MEMORY_FACTOR = 1.2
GPU_MEMORY_FACTOR = 1.2


class Job:

  def __init__(self, 
               job_id: int, 
               taskname: str,
               command: str,
               workarea: str,
               device: int = -1,
               image: str = '', 
               extra_envs: dict={},
               binds = {},
               testing = False
               ):

    self.id         = job_id
    self.image      = image
    self.workarea   = workarea
    self.command    = command
    self.pending    = True
    self.broken     = False
    self.__to_close = False
    self.killed     = False
    self.env        = os.environ.copy()
    self.binds      = binds
    self.device     = device

    self.testing    = testing

  
    logger.info(f"Job will use {image} as image...")
    logger.info("Setting all environs into the singularity envs...")
    # Transfer all environ to singularity container

    self.env["SINGULARITYENV_CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
    self.env["SINGULARITYENV_CUDA_VISIBLE_DEVICES"]=str(device)
    self.env["SINGULARITYENV_TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
    self.env["SINGULARITYENV_JOB_WORKAREA"] = self.workarea
    self.env["SINGULARITYENV_JOB_IMAGE"] = self.image
    self.env["SINGULARITYENV_JOB_TASKNAME"] = taskname
    self.env["SINGULARITYENV_JOB_NAME"] = self.workarea.split('/')[-1]
    self.env["SINGULARITYENV_JOB_ID"] = str(self.id)
    self.env["SINGULARITYENV_JOB_TESTING"] = 'true' if testing else 'false'


    # process
    self.__proc = None
    self.__proc_stat = None
    self.entrypoint=self.workarea+'/entrypoint.sh'


  def run(self):

    os.makedirs(self.workarea, exist_ok=True)
    # build script command
    with open(self.entrypoint,'w') as f:
      f.write(f"cd {self.workarea}\n")
      f.write(f"{self.command.replace('%','$')}\n")

    try:
      self.pending=False
      self.killed=False
      self.broken=False

      # entrypoint 
      with open(self.entrypoint,'r') as f:
        for line in f.readlines():
          logger.info(line)
   
    
      binds=""
      for storage, volume in self.binds.items():
        binds += f'--bind {storage}:{volume} '
      command = f"singularity exec --nv --writable-tmpfs {binds} {self.image} bash {self.entrypoint}"
      command = command.replace('  ',' ') 

      print(command)
      self.__log_file = open(self.workarea+'/output.log', 'w')
      self.__proc = subprocess.Popen(command, env=self.env, shell=True, stdout=self.__log_file)

      sleep(1) # NOTE: wait for 2 seconds to check if the proc really start.
      self.__proc_stat = psutil.Process(self.__proc.pid)
      broken = self.status() == JobStatus.FAILED
      self.broken = broken
      return not broken # Lets considering the first seconds as broken

    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      self.broken=True
      return False


  def is_alive(self):
    return True if (self.__proc and self.__proc.poll() is None) else False


  def to_close(self):
    self.__to_close=True


  def closed(self):
    return self.__to_close


  def proc_stat(self):
    sys_used_memory = 0; cpu_percent = 0; gpu_used_memory = 0

    try:
      children = self.__proc_stat.children(recursive=True)
      gpu_children = nvsmi.get_gpu_processes()
      for child in children:
        p=psutil.Process(child.pid)
        sys_used_memory += p.memory_info().rss/1024**2
        cpu_percent += p.cpu_percent()
        for gpu_child in gpu_children:
          gpu_used_memory += gpu_child.used_memory if gpu_child.pid==child.pid else 0
    except:
      logger.debug("proc stat not available.")
    return cpu_percent, sys_used_memory, gpu_used_memory
      

      

  #
  # Kill the main process
  #
  def kill(self):
    if self.is_alive():
      children = self.__proc_stat.children(recursive=True)
      for child in children:
        p=psutil.Process(child.pid)
        p.kill()
      self.__proc.kill()
      self.killed=True
    else:
      self.killed=True


  #
  # Get the consumer state
  #
  def status(self):

    if self.is_alive():
      return JobStatus.RUNNING
    elif self.pending:
      return JobStatus.PENDING
    elif self.killed:
      return JobStatus.KILLED
    elif self.broken:
      return JobStatus.BROKEN
    elif (self.__proc.returncode and  self.__proc.returncode>0):
      return JobStatus.FAILED
    else:
      return JobStatus.COMPLETED


 

#
# A collection of slots
#
class Consumer(threading.Thread):

  def __init__(self, host          : str,
                     device        : int=-1, 
                     binds         : dict={}, 
                     timeout       : int=60, 
                     max_retry     : int=5, 
                     slot_size     : int=1, 
                     partition     : str='cpu',
                     db            : Database=None,
                     ):
            
    threading.Thread.__init__(self)
    self.host      = host
    self.partition = partition
    self.jobs      = {}
    self.binds     = binds
    self.timeout   = timeout
    self.max_retry = max_retry
    self.device    = device
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    self.__lock.set() 
    self.db = db

 

  def stop(self):
    self.__stop.set()


  def __len__(self):
    return len(self.jobs.keys())


  def run(self):

    while (not self.__stop.isSet()):

      sleep(5)

      # get the server host location from the database
      hostname = self.db().get_environ( "PILOT_SERVER_HOSTNAME" )
      if not hostname:
        logger.debug("hostname not available yet into the database. waiting...")
        continue
      port     = self.db().get_environ( "PILOT_SERVER_PORT" )
      host     = f"{hostname}:{port}"
      logger.debug(f"connecting to the server as {host}...")
      server   = schemas.client( host, 'pilot')

      # NOTE wait to be set
      self.__lock.wait() 
      # NOTE: when set, we will need to wait to register until this loop is read
      self.__lock.clear()      

      answer = server.try_request(f'join', method="post", body=schemas.Request( host=self.host ).json())
      if answer.status:
        logger.debug(f"connected with {answer.host}")
        self.loop()
      else:
        logger.error("not possible to connect with the server...")
 
      # NOTE: allow external user to incluse executors into the list
      self.__lock.set()



  def start_job( self, job_id ):

    self.__lock.wait()
    self.__lock.clear()

    if job_id in self.jobs.keys():
      logger.error(f"Job {job_id} exist into the consumer. Not possible to include here.")
      return False


    blocked = any([job.testing for job in self.jobs.values()])

    # check if we have a test job waiting to run or running...
    if blocked:
      logger.warning("The consumer is blocked because we have a testing job waiting to run.")
      return False


    with self.db as session:
      
      job_db = session.get_job(job_id, with_for_update=True)
   
      testing = job_db.task.status == TaskStatus.TESTING

      if (not self.check_resources(job_db)):
        logger.warning(f"Job {job_id} estimated resources not available at this consumer.")
        return False

      binds = copy(self.binds)
      binds.update(job_db.get_binds())
      envs = job_db.get_envs()
      job = Job(  
             job_db.id,
             job_db.task.name,
             job_db.command,
             job_db.workarea,
             image=job_db.image,
             device=self.device,
             extra_envs=envs,
             binds=binds,
             testing=testing,
             )
      job_db.status = JobStatus.PENDING
      job_db.ping()
      self.jobs[job_id] = job
      session.commit()

    self.__lock.set()
    logger.debug(f'Job with id {job.id} included into the consumer.')
    return True


  def loop(self):

    start = time()

    with self.db as session:

      # Loop over all available consumers
      for key, job in self.jobs.items():

        job_db = session.get_job(job.id, with_for_update=True)
        task_db = job_db.task

        # NOTE: kill job option only available with database by external trigger
        if job_db.status == JobStatus.KILL:
          logger.debug("Kill job from database...")
          job.kill()

        if job.status() == JobStatus.PENDING:
          job_db.ping()
          if job.testing:
            logger.debug(f"Job {job.id} is a testing job...")
            if len(self.jobs)==1:
              if job.run():
                logger.debug(f'Job {job.id} is RUNNING.')
                job_db.status = JobStatus.RUNNING
              else:
                logger.debug(f'Job {job.id} is BROKEN.')
                job_db.status = JobStatus.BROKEN
                job.to_close()
            else:
              logger.debug(f"Consumer has {len(self.jobs)} jobs into the list. Waiting...")
          else:
            logger.debug(f"Job {job.id} is a single job...")
            if job.run():
              logger.debug(f'Job {job.id} is RUNNING.')
              job_db.status = JobStatus.RUNNING
            else:
              logger.debug(f'Job {job.id} is BROKEN.')
              job_db.status = JobStatus.BROKEN
              job.to_close()

        elif job.status() is JobStatus.FAILED:
          logger.debug(f'Job {job.id} is FAILED.')
          job_db.status = JobStatus.FAILED
          job.to_close()

        elif job.status() is JobStatus.KILLED:
          logger.debug(f'Job {job.id} is KILLED.')
          job_db.status = JobStatus.KILLED
          job.to_close()

        elif job.status() is JobStatus.RUNNING:
          logger.debug(f'Job {job.id} is RUNNING.')
          cpu_percent, sys_used_memory, gpu_used_memory = job.proc_stat()
          job_db.cpu_percent      = max(cpu_percent       , job_db.cpu_percent      )
          job_db.sys_used_memory  = max(sys_used_memory   , job_db.sys_used_memory  )
          job_db.gpu_used_memory  = max(gpu_used_memory   , job_db.gpu_used_memory  )
          logger.debug(f"Job {job.id} consuming {job_db.sys_used_memory } MB of memory, {job_db.gpu_used_memory} "+ 
                       f"MB of GPU memory and {job_db.cpu_percent} of CPU.")
          job_db.ping()

        elif job.status() is JobStatus.COMPLETED:
          logger.debug(f'Job {job.id} is COMPLETED.')
          job_db.status = JobStatus.COMPLETED
          job.to_close()

        session.commit()

    # Loop over all jobs

    self.jobs = { job_id:job for job_id, job in self.jobs.items() if not job.closed()}

    end = time()
    current_in = len(self.jobs.keys())
    logger.debug(f"Run stage toke {round(end-start,4)} seconds")
    logger.debug(f"We have a total of {current_in} jobs into the consumer.")



  def system_info(self, detailed=False):
    d = system_info()
    d['consumer'] = {
      'host'      : self.host,
      'partition' : self.partition,
      'device'    : self.device,
      'allocated' : len(self.jobs.keys()),
    }
    sys_avail_memory = d['memory']['avail']
    gpu_avail_memory = d['gpu'][self.device]['avail'] if self.device>=0 else 0
    return d if detailed else (sys_avail_memory, gpu_avail_memory) 




  def check_resources(self, job_db : models.Job):

    # available memory into the system
    sys_avail_memory, gpu_avail_memory = self.system_info()

    # estimatate memory peak by mean for the current task
    sys_used_memory = job_db.task.sys_used_memory()
    gpu_used_memory = job_db.task.gpu_used_memory()

    logger.debug(f"Job system used memory : {sys_used_memory} ({sys_avail_memory}) MB")
    logger.debug(f"Job gpu used memory    : {gpu_used_memory} ({gpu_avail_memory}) MB")

    # check if we have memory to run this workload
    if (sys_used_memory >= 0) and (sys_used_memory * SYS_MEMORY_FACTOR > sys_avail_memory):
      logger.warning("Not available memory to run this job into this consumer.")
      return False  

    # check if we have gpu memory to run this workload
    if (self.device >= 0) and (gpu_used_memory >= 0) and (gpu_used_memory * GPU_MEMORY_FACTOR > gpu_avail_memory):
      logger.warning("Not available GPU memory to run this job into this consumer.")
      return False
   
    # if here, all resources available for this workload
    return True

  