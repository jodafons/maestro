
__all__ = []

import os, subprocess, traceback, psutil, nvsmi
from time import sleep
from maestro.enumerations import JobStatus
from loguru import logger


class Job:

  def __init__(self, 
               job_id        : int, 
               taskname      : str,
               command       : str,
               workarea      : str,
               # others parameters
               device        : int=-1,
               image         : str="", 
               virtualenv    : str="",
               binds         : dict= {},
               testing       : bool=False,
               run_id        : str="",
               tracking_url  : str="",
               extra_envs    : dict={},
               ):

    self.id         = job_id
    self.image      = image
    self.workarea   = workarea
    self.command    = command
    self.run_id     = run_id

    self.pending    = True
    self.broken     = False
    self.__to_close = False
    self.killed     = False
    self.env        = os.environ.copy()
    self.binds      = binds
    self.device     = device
    self.testing    = testing

    self.virtualenv = virtualenv


    logger.info(f"Job will use {image} as image...")
    logger.info("Setting all environs into the singularity envs...")
    # Transfer all environ to singularity container
    job_name  = self.workarea.split('/')[-1]



    self.env[("" if image=="" else "SINGULARITYENV_") + "CUDA_DEVICE_ORDER"]         = "PCI_BUS_ID"
    self.env[("" if image=="" else "SINGULARITYENV_") + "CUDA_VISIBLE_DEVICES"]      = str(device)
    self.env[("" if image=="" else "SINGULARITYENV_") + "TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
    self.env[("" if image=="" else "SINGULARITYENV_") + "JOB_WORKAREA"]              = self.workarea
    self.env[("" if image=="" else "SINGULARITYENV_") + "JOB_IMAGE"]                 = self.image
    self.env[("" if image=="" else "SINGULARITYENV_") + "JOB_TASKNAME"]              = taskname
    self.env[("" if image=="" else "SINGULARITYENV_") + "JOB_NAME"]                  = job_name
    self.env[("" if image=="" else "SINGULARITYENV_") + "JOB_ID"]                    = str(self.id)
    self.env[("" if image=="" else "SINGULARITYENV_") + "JOB_DRY_RUN"]               = 'true' if testing else 'false'
    self.env[("" if image=="" else "SINGULARITYENV_") + "TRACKING_RUN_ID"]             = self.run_id
    self.env[("" if image=="" else "SINGULARITYENV_") + "TRACKING_URL"]                = tracking_url 

    for env_name, env_value in extra_envs.items():
      self.env[("" if image=="" else "SINGULARITYENV_") + env_name] = env_value

    self.logpath = self.workarea+'/output.log'

    # process
    self.__proc = None
    self.__proc_stat = None
    self.entrypoint=self.workarea+'/entrypoint.sh'

    if image!="":
      logger.info(f"running job using singularity engine... {image}")
    else:
      logger.info(f"running without singularity...")

    if virtualenv!="":
      logger.info(f"setup virtualenv to {virtualenv}")



  def run(self, tracking=None):

    os.makedirs(self.workarea, exist_ok=True)

    entrypoint = f"cd {self.workarea}\n"
    if self.virtualenv!="":
      entrypoint+=f"source {self.virtualenv}/bin/activate\n"
    entrypoint+=f"{self.command.replace('%','$')}\n"

    # build script command
    with open(self.entrypoint,'w') as f:
      f.write(entrypoint)

    try:
      self.killed=False
      self.broken=False

      # entrypoint 
      with open(self.entrypoint,'r') as f:
        for line in f.readlines():
          logger.info(line)
   
      if self.image!="":
        binds=""
        for storage, volume in self.binds.items():
          binds += f'--bind {storage}:{volume} '
        command = f"singularity exec --nv --writable-tmpfs {binds} {self.image} bash {self.entrypoint}"
        command = command.replace('  ',' ') 
      else:
        command = f"bash {self.entrypoint}"


      print(command)
      
      self.__log_file = open(self.logpath, 'w')
      self.__proc = subprocess.Popen(command, env=self.env, shell=True, stdout=self.__log_file)

      sleep(1) # NOTE: wait for 2 seconds to check if the proc really start.
      self.__proc_stat = psutil.Process(self.__proc.pid)
      self.pending=False

      broken = self.status() == JobStatus.FAILED
      self.broken = broken

      # NOTE: mlflow trackinging
      if tracking:
        tracking.log_param(self.run_id, "command"   , command     )
        tracking.log_param(self.run_id, "entrypoint", entrypoint  )
        tracking.log_dict(self.run_id , self.env, "environ.json"  )


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
    self.__log_file.close()


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
    if self.is_alive() and self.__proc:
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


 