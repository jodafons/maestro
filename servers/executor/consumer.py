


import os, subprocess, traceback, psutil, time, sys, threading

from time import time, sleep
from loguru import logger

try:
  from enumerations import JobStatus
  from models import Job as Job_db
except:
  from maestro.enumerations import JobStatus
  from maestro.models import Job as Job_db


class Job:

  def __init__(self, 
               job_id: int, 
               taskname: str,
               command: str,
               workarea: str,
               device: int,
               image: str = None, 
               job_db: Job_db = None,
               extra_envs: dict={},
               binds = {},
               dry_run=False):

    self.id         = job_id
    self.image      = image
    self.workarea   = workarea
    self.command    = command
    self.pending    = True
    self.broken     = False
    self.__to_kill  = False
    self.__to_close = False
    self.killed     = False
    self.env        = os.environ.copy()
    self.binds      = binds
    self.job_db     = job_db

   
    # Transfer all environ to singularity container
    self.env["SINGULARITYENV_JOB_WORKAREA"] = self.workarea
    self.env["SINGULARITYENV_JOB_IMAGE"] = self.image
    self.env["SINGULARITYENV_CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
    self.env["SINGULARITYENV_CUDA_VISIBLE_DEVICES"]=str(device)
    self.env["SINGULARITYENV_TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
    self.env["SINGULARITYENV_JOB_TASKNAME"] = taskname
    self.env["SINGULARITYENV_JOB_NAME"] = self.workarea.split('/')[-1]
    #self.env["SINGULARITYENV_JOB_ID"] = self.id
    
    # Update the job enviroment from external envs
    for key, value in extra_envs.items():
      self.env[key]="SINGULARITYENV_"+value

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
        command = f"singularity exec --nv --writable-tmpfs --bind /home:/home {self.image} bash {self.entrypoint}"

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


  def to_kill(self):
    self.__to_kill=True


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
      self.__to_kill=False
      return True
    else:
      return False


  #
  # Get the consumer state
  #
  def status(self):

    if self.is_alive():
      return JobStatus.RUNNING
    elif self.pending:
      return JobStatus.PENDING
    elif self.__to_kill:
      return JobStatus.KILL
    elif self.killed:
      return JobStatus.KILLED
    elif self.broken:
      return JobStatus.BROKEN
    elif (self.__proc.returncode and  self.__proc.returncode>0):
      return JobStatus.FAILED
    else:
      return JobStatus.COMPLETED


  def ping(self):
    if self.job_db:
      self.job_db.ping()


#
# A collection of slots
#
class Consumer(threading.Thread):

  def __init__(self, me, db=None, pilot=None, device=-1, binds={}, timeout=60, max_retry=5, size=1):
    
    threading.Thread.__init__(self)
    self.jobs      = {}
    self.binds     = binds
    self.db        = db
    self.pilot     = pilot
    self.tac       = time()
    self.timeout   = timeout
    self.max_retry = max_retry
    self.device    = device
    self.me        = me
    self.size      = size
    self.__stop    = threading.Event()


  def stop(self):
    self.__stop.set()


  #
  # execute this as thread
  #
  def run(self):

    retry = 0

    while (not self.__stop.isSet()) and (retry < self.max_retry) and self.pilot:
      sleep(5)
      tic = time()

      # NOTE: If we enter here is because the server is not sent loop command anymore.
      # Probably something happing with the connection between both.
      if (tic - self.tac) > 10:
        # NOTE: Check if we have a server.
        if not self.pilot.is_alive():
          logger.info("The pilot server is not alive. Not possible to stablish a connection.")
          retry += 1
        # NOTE: Server is alive
        else:
          if self.pilot.register( me = self.me, device=self.device ):
            logger.info(f"The executor with host name {self.me} was registered into the pilot server.")
            self.tac = time()
            retry = 0
          else:
            retry+=1


    # NOTE: If here, abort eveyrthing.
    if self.pilot:
      logger.critical("Stop server condition arise because the number of retry between the pilot and the executor exceeded.")
    else:
      logger.warning("Pilot not passed to the consumer. This object will not execute in thread mode. Porbably you are running in standalone mode.")



  #
  # Add a job into the slot
  #
  def start_job( self, job_id, taskname, command, image, workarea, device=-1, extra_envs={} ):

    if job_id in self.jobs.keys():
      logger.error(f"Job {job_id} exist into the consumer. Not possible to include here.")
      return False
    
    logger.info("Creating local job...")
    job = Job(  
           job_id,
           taskname,
           command,
           image,
           workarea,
           device,
           extra_envs=extra_envs,
           job_db = self.db.job(job_id) if self.db else None,
           binds = self.binds,
           dry_run=dry_run)

    job.ping()
    self.jobs[job_id] = job
    logger.info(f'Job with id {job.id} included into the consumer.')
    return True


  def kill_job(self, job_id):
    if job_id in self.jobs.keys():
      logger.info(f"Send kill signal to job {job_id}")
      self.jobs[job_id].to_kill()
      return True
    logger.warning(f"Not possible to find job {job_id} into the consumer list.")
    return False


  def status_job(self, job_id):
    if job_id in self.jobs.keys():
      status = self.jobs[job_id].status()
      logger.info(f"Job {job_id} with status {status}")
      return status

    logger.warning(f"Not possible to find job {job_id} into the consumer list.")
    return None



  def loop(self):

    self.tac = time()
    start = time()

    # Loop over all available consumers
    for key, job in self.jobs.items():

      # NOTE: Checking with job is in KILL status by external trigger 
      if job.db() and (job.db().status == JobStatus.KILL):
        logger.info("Kill job from database...")
        job.kill()
      else:
        if job.status() == JobStatus.KILL:
          logger.info("Kill job from endpoint...")
          job.kill()


      if job.status() == JobStatus.PENDING:
        if job.run():
          logger.info(f'Job {job.id} is RUNNING.')
          if job.db():
            job.db().status = JobStatus.RUNNING
        else:
          logger.info(f'Job {job.id} is BROKEN.')
          if job.db():
            job.db().status = JobStatus.BROKEN
          job.to_close()

      elif job.status() is JobStatus.FAILED:
        logger.info(f'Job {job.id} is FAILED.')
        if job.db():
          job.db().status = JobStatus.FAILED
        job.to_close()

      elif job.status() is JobStatus.KILLED:
        logger.info(f'Job {job.id} is KILLED.')
        if job.db():
          job.db().status = JobStatus.KILLED
        job.to_close()

      elif job.status() is JobStatus.RUNNING:
        logger.info(f'Job {job.id} is RUNNING.')
        logger.info(f"Job {job.id} pinging...")
        job.ping()

      elif job.status() is JobStatus.COMPLETED:
        logger.info(f'Job {job.id} is COMPLETED.')
        if job.db():
          job.db().status = JobStatus.COMPLETED
        job.to_close()


    res = {job_id:job.status() for job_id, job in self.jobs.items()}

    self.jobs = { job_id:job for job_id, job in self.jobs.items() if not job.closed()}

    end = time()
    current_in = len(self.jobs.keys())

    logger.info(f"Run stage toke {round(end-start,4)} seconds")
    logger.info(f"We have a total of {current_in} jobs into the consumer.")

    print([(job_id, job.status(), job.db().status) for job_id, job in self.jobs.items()])

    return current_in, res


  def full(self):
    return len(self.jobs.keys())>=self.size


  def allocated(self):
    return len(self.jobs.keys())

  