
import os, subprocess, traceback, psutil, time, sys, threading
from maestro.enumerations import JobStatus
from maestro.models import Job as Job_db
from time import time, sleep
from loguru import logger


#
# TODO: copy from executor server. We need to move this to someplace in common in the future
#
class Job:

  def __init__(self, 
               job_id: int, 
               taskname: str,
               command: str,
               image: str, 
               workarea: str,
               device: int,
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
    self.dry_run    = dry_run

   
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
   
      if self.dry_run:
        logger.info("This is a test job...")
        command = f"bash {self.entrypoint} "
      else: # singularity
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


  def to_close(self):
    self.__to_close=True


  def closed(self):
    return self.__to_close


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





def test_job( job_db ):

    job = Job( job_id     = job_db.id, 
               taskname   = job_db.task.name,
               command    = job_db.command,
               image      = job_db.image, 
               workarea   = job_db.workarea,
               device     = -1,
               job_db     = job_db,
               extra_envs = {"JOB_LOCAL_TEST":'1'},
               binds      = {},
               dry_run    = False)


    while True:
        if job.status() == JobStatus.PENDING:
            if not job.run():
              return False
        elif job.status() == JobStatus.FAILED:
            return False
        elif job.status() == JobStatus.RUNNING:
            continue
        elif job.status() == JobStatus.COMPLETED:
            job_db.status=JobStatus.REGISTERED
            return True
        else:
            continue

    
