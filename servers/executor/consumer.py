


import os, subprocess, traceback, psutil, time, sys, threading
from time import time, sleep
from loguru import logger
from pprint import pprint
from copy import copy
from collections import deque

try:
  from enumerations import JobStatus
  from models import Job as JobModel
  from api.clients import pilot, database
except:
  from maestro.enumerations import JobStatus
  from maestro.models import Job as JobModel
  from maestro.api.clients import pilot

SECONDS = 1

class Job:

  def __init__(self, 
               job_id: int, 
               job_db: JobModel,
               taskname: str,
               command: str,
               workarea: str,
               device: int = -1,
               image: str = '', 
               extra_envs: dict={},
               binds = {}
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
    self.job_db     = job_db

    # NOTE: Not an empty string
    if len(image)>0: 
      logger.info(f"Job will use {image} as image...")
      logger.info("Setting all environs into the singularity envs...")
      # Transfer all environ to singularity container
      self.env["SINGULARITYENV_JOB_WORKAREA"] = self.workarea
      self.env["SINGULARITYENV_JOB_IMAGE"] = self.image
      self.env["SINGULARITYENV_CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
      self.env["SINGULARITYENV_CUDA_VISIBLE_DEVICES"]=str(device)
      self.env["SINGULARITYENV_TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
      self.env["SINGULARITYENV_JOB_TASKNAME"] = taskname
      self.env["SINGULARITYENV_JOB_NAME"] = self.workarea.split('/')[-1]
      self.env["SINGULARITYENV_JOB_ID"] = str(self.id)
      # Update the job enviroment from external envs
      for key, value in extra_envs.items():
        self.env["SINGULARITYENV_"+key]=value
    else:
      # Transfer all environ to singularity container
      self.env["JOB_WORKAREA"] = self.workarea
      self.env["JOB_IMAGE"] = self.image
      self.env["CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
      self.env["CUDA_VISIBLE_DEVICES"]=str(device)
      self.env["TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
      self.env["JOB_TASKNAME"] = taskname
      self.env["JOB_NAME"] = self.workarea.split('/')[-1]
      self.env["JOB_ID"] = str(self.id)
      # Update the job enviroment from external envs
      for key, value in extra_envs.items():
        self.env[key]=value

    # process
    self.__proc = None
    self.__proc_stat = None
    self.entrypoint=self.workarea+'/entrypoint.sh'


  def db(self):
    return self.job_db


  #
  # Run the job process
  #
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
   
      if not self.image:
        logger.info("Running job without image...")
        command = f"bash {self.entrypoint} "
      else: # singularity
        binds=""
        for storage, volume in self.binds.items():
          binds += '--bind {storage}:{volume} '
        command = f"singularity exec --nv --writable-tmpfs {binds} {self.image} bash {self.entrypoint}"
        command = command.replace('  ',' ') 

      print(command)
      self.__proc = subprocess.Popen(command, env=self.env, shell=True)
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


  #
  # Check if the process still running
  #
  def is_alive(self):
    return True if (self.__proc and self.__proc.poll() is None) else False


  def to_close(self):
    self.__to_close=True


  def closed(self):
    return self.__to_close


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

  def __init__(self, device=-1, binds={}, timeout=60, max_retry=5, slot_size=1):
            
    threading.Thread.__init__(self)
    self.localhost = os.environ["EXECUTOR_SERVER_HOST"]
    self.db        = database(os.environ["DATABASE_SERVER_HOST"])
    self.queue     = deque()
    self.jobs      = {}
    self.binds     = binds
    self.timeout   = timeout
    self.max_retry = max_retry
    self.device    = device
    self.size      = slot_size
    self.__stop    = threading.Event()


  def stop(self):
    self.__stop.set()


  def __len__(self):
    return len(self.jobs.keys())


  #
  # Thread loop
  #
  def run(self):

    logger.info("Connecting into the server...")
    server = pilot(os.environ["PILOT_SERVER_HOST"])
    server.connect( self.localhost, self.device )

    while (not self.__stop.isSet()):
      sleep(5)
      self.loop()





  #
  # Add a job into the slot
  #
  def create( self, job_id ):

    if job_id in self.jobs.keys():
      logger.error(f"Job {job_id} exist into the consumer. Not possible to include here.")
      return False
    
    job_db = self.db.job(job_id)
    binds = copy(self.binds)
    binds.update(job_db.get_binds())
    envs = job_db.get_envs()
    job = Job(  
           job_db.id,
           job_db, 
           job_db.task.name,
           job_db.command,
           job_db.workarea,
           image=job_db.image,
           device=self.device,
           extra_envs=envs,
           binds=binds,
           )
    job.db().ping()
    self.jobs[job_id] = job
    logger.info(f'Job with id {job.id} included into the consumer.')
    return True


  #
  # Standalone loop
  #
  def loop(self):

    start = time()

    # check if we have any job into the queue
    if len(self.queue) > 0:
      self.create( self.queue.pop() )

    # Loop over all available consumers
    for key, job in self.jobs.items():

      # NOTE: kill job option only available with database by external trigger
      if job.db().status == JobStatus.KILL:
        logger.info("Kill job from database...")
        job.kill()
    
      if job.status() == JobStatus.PENDING:
        if job.run():
          logger.info(f'Job {job.id} is RUNNING.')
          job.db().status = JobStatus.RUNNING
        else:
          logger.info(f'Job {job.id} is BROKEN.')
          job.db().status = JobStatus.BROKEN
          job.to_close()

      elif job.status() is JobStatus.FAILED:
        logger.info(f'Job {job.id} is FAILED.')
        job.db().status = JobStatus.FAILED
        job.to_close()

      elif job.status() is JobStatus.KILLED:
        logger.info(f'Job {job.id} is KILLED.')
        job_db.status = JobStatus.KILLED
        job.to_close()

      elif job.status() is JobStatus.RUNNING:
        logger.info(f'Job {job.id} is RUNNING.')
        job.db().ping()

      elif job.status() is JobStatus.COMPLETED:
        logger.info(f'Job {job.id} is COMPLETED.')
        job.db().status = JobStatus.COMPLETED
        job.to_close()


    res = {job_id:job.status() for job_id, job in self.jobs.items()}
    self.jobs = { job_id:job for job_id, job in self.jobs.items() if not job.closed()}

    end = time()
    current_in = len(self.jobs.keys())
    logger.info(f"Run stage toke {round(end-start,4)} seconds")
    logger.info(f"We have a total of {current_in} jobs into the consumer.")

    # Update database values
    self.db.commit()



  def full(self):
    return len(self.jobs.keys())>=self.size


  def push_back(self, job_id):
    """
      Add a job id into the queue
    """
    if self.full():
      logger.warning(f"Executor is full. Please try once again late...")
      return False

    if job_id in self.jobs.keys():
      logger.warning(f"Job {job_id} exist into the consumer. Not possible to include here.")
      return False
    
    self.queue.append(job_id)
    logger.info(f"Job {job_id} added into the queue. Waiting the next loop to start...")
    return True


  