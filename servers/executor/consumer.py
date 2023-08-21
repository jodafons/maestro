
__all__ = ["Consumer"]


import os, subprocess, traceback, psutil, time, sys
from time import time, sleep
from enum import Enum
from loguru import logger



class JobStatus(Enum):

    UNKNOWN    = 'Unknown'
    REGISTERED = "Registered"
    ASSIGNED   = "Assigned"
    TESTING    = "Testing"
    BROKEN     = "Broken"
    FAILED     = "Failed"
    KILL       = "Kill"
    KILLED     = "Killed"
    COMPLETED  = "Completed"
    PENDING    = "Pending"
    RUNNING    = "Running"


class Job:

  def __init__(self, 
               job_id: int, 
               taskname: str,
               command: str,
               image: str, 
               workarea: str,
               device: int,
               extra_envs: dict={},
               docker_engine: bool=False,
               binds = {'/home':'/home','/mnt/cern_data':'/mnt/cern_data'}):

    self.id       = job_id
    self.image    = image
    self.workarea = workarea
    self.command  = command
    self.pending  = True
    self.broken   = False
    self.__to_kill= False
    self.killed   = False

    self.env      = os.environ.copy()
    self.binds    = binds
    self.docker_engine = docker_engine

   
    # Transfer all environ to singularity container
    self.env["SINGULARITYENV_JOB_WORKAREA"] = self.workarea
    self.env["SINGULARITYENV_JOB_IMAGE"] = self.image
    self.env["SINGULARITYENV_CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
    self.env["SINGULARITYENV_CUDA_VISIBLE_DEVICES"]=str(device)
    self.env["SINGULARITYENV_TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
    self.env["SINGULARITYENV_JOB_TASKNAME"] = taskname
    self.env["SINGULARITYENV_JOB_NAME"] = self.workarea.split('/')[-1]
    # Update the job enviroment from external envs
    for key, value in extra_envs.items():
      self.env[key]="SINGULARITYENV_"+value

    # process
    self.__proc = None
    self.__proc_stat = None
    self.entrypoint=self.workarea+'/entrypoint.sh'


  def name(self):
    #return f'{self.taskname}_{self.id}'
    return f'{self.id}'

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

      if self.docker_engine:        
        envs_str=''
        volumes_str = '-v $HOME:$HOME '
        for key,value in self.env.items():
          if 'SINGULARITYENV' in key:
            env_name = key.replace('SINGULARITYENV_','')
            envs_str+=f'-e {env_name}={value} '
        for source, target in self.binds.items():
          volumes_str += f'-v {source}:{target} '
        command = f"docker run {volumes_str} {envs_str} --user $(id -u):$(id -g) -it {self.image} bash {self.entrypoint}"
      else:
        # singularity
        command = f"singularity exec --nv --writable-tmpfs --bind /home:/home {self.image} bash {self.entrypoint}"

      print(command)
      self.__proc = subprocess.Popen(command, env=self.env, shell=True)
      self.__proc_stat = psutil.Process(self.__proc.pid)
      return True

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







#
# A collection of slots for each device (CPU and GPU)
#
class Consumer:

  def __init__(self, device, binds, docker_engine=False):
    self.jobs = {}
    self.docker_engine=docker_engine
    self.binds = binds
    self.device = device


  #
  # Add a job into the slot
  #
  def start( self, job_id, taskname, command, image, workarea, extra_envs={} ):
    job = Job(  
           job_id,
           taskname,
           command,
           image,
           workarea,
           self.device,
           extra_envs=extra_envs,
           docker_engine=self.docker_engine,
           binds = self.binds)

    if job.name() in self.jobs.keys():
      logger.error("Job exist into the consumer. Not possible to include here.")
      return False

    self.jobs[job_id] = job
    
    logger.info(f'Job with id {job.id} included into the consumer.')
    return True


  def kill(self, job_id):
    if job_id in self.jobs.keys():
      logger.info(f"Send kill signal to job {job_id}")
      self.jobs[job_id].to_kill()
      return True

    logger.warning(f"Not possible to find job {job_id} into the consumer list.")
    return False


  def job(self, job_id):
    if job_id in self.jobs.keys():
      #logger.info(f"Job {job_id} find into consumer.")
      return self.jobs[job_id].status()

    logger.warning(f"Not possible to find job {job_id} into the consumer list.")
    return None



  def run(self):

    start = time()

    job_answer = {}
    to_remove = []

    # Loop over all available consumers
    for key, job in self.jobs.items():

      if job.status() == JobStatus.KILL:
        job.kill()

      if job.status() == JobStatus.PENDING:
        if job.run():
          logger.info(f'Job {job.id} is RUNNING.')
          pass
        else:
          logger.info(f'Job {job.id} is BROKEN.')
          to_remove.append(key)

      elif job.status() is JobStatus.FAILED:
        logger.info(f'Job {job.id} is FAILED.')
        to_remove.append(key)

      elif job.status() is JobStatus.KILLED:
        logger.info(f'Job {job.id} is KILLED.')
        to_remove.append(key)

      elif job.status() is JobStatus.RUNNING:
        logger.info(f'Job {job.id} is RUNNING.')

      elif job.status() is JobStatus.COMPLETED:
        logger.info(f'Job {job.id} is COMPLETED.')
        to_remove.append(key)


    job_answer = {job_id:job.status() for job_id, job in self.jobs.items()}
    for job_id in to_remove:
      del self.jobs[job_id]

    end = time()

    logger.info(f"Run stage toke {round(end-start,4)} seconds")

    current_in = len(self.jobs.keys())
    logger.info(f"We have a total of {current_in} jobs into the consumer.")

    return current_in, job_answer



  

