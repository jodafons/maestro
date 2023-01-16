
__all__ = ["Consumer"]


import os, time, subprocess, traceback, psutil
from orchestra.database.models import Device
from orchestra.status import JobStatus
from orchestra.server import Slot
from orchestra import ERROR, INFO



class Job:

  def __init__(self, job_db, slot, extra_envs={}):

    self.job_db = job_db
    self.slot = slot
    self.image = job_db.image
    self.workarea = job_db.workarea
    self.command = job_db.command
    self.pending=True
    self.broken=False
    self.killed=False
    self.env = os.environ.copy()
    # Transfer all environ to singularity container
    self.env["SINGULARITYENV_JOB_WORKAREA"] = self.workarea
    self.env["SINGULARITYENV_JOB_IMAGE"] = self.image
    self.env["SINGULARITYENV_CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
    self.env["SINGULARITYENV_CUDA_VISIBLE_DEVICES"]=str(slot.device)
    self.env["SINGULARITYENV_TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
    self.env["SINGULARITYENV_JOB_TASKNAME"] = job_db.task.name
    #self.env["SINGULARITYENV_PYTHONPATH"] = job_db.get_env("PYTHONPATH")
    self.env["SINGULARITYENV_JOB_NAME"] = self.workarea.split('/')[-1]


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
      #f.write(f"export PATH=$PATH:{self.job_db.get_env('PATH')}\n")
      f.write(f"{self.command.replace('%','$')}\n")


    try:
      self.pending=False
      self.killed=False
      self.broken=False

      # entrypoint 
      with open(self.entrypoint,'r') as f:
        for line in f.readlines():
          print(INFO+line)

      # singularity
      command = "singularity exec --nv --writable-tmpfs --bind /home:/home {image} bash {entrypoint}".format(image=self.image,
                                                                                                             entrypoint=self.entrypoint)
      print(INFO+command)
      self.__proc = subprocess.Popen(command, env=self.env, shell=True)
      time.sleep(2)
      self.__proc_stat = psutil.Process(self.__proc.pid)
      return True

    except Exception as e:
      traceback.print_exc()
      print(ERROR+e)
      self.broken=True
      return False


  #
  # Check if the process still running
  #
  def is_alive(self):
    return True if (self.__proc and self.__proc.poll() is None) else False


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

  def __init__(self, device, db):
    self.db = db
    self.device_db = device
    self.total = 0
    self.slots = [Slot(device.gpu) for _ in range(self.device_db.slots)]
    for slot_id in range(self.device_db.enabled):
        self.slots[slot_id].enable()
        self.total+=1
    self.jobs = []


  #
  # Add a job into the slot
  #
  def push_back( self, job_db ):
    slot = self.pop()
    if slot:
      job = Job( job_db , slot )
      job_db.status = JobStatus.PENDING
      self.jobs.append(job)
      job.db().ping()
      slot.lock()
      return True
    else:
      return False




  def run(self):

    print(INFO+f"Run consumer {self.device_db.host}")
    self.pull()

    deactivate_jobs = []

    # Loop over all available consumers
    for job in self.jobs:

      slot = job.slot

      if job.db().status == JobStatus.KILL:
        job.kill()

      if job.status() == JobStatus.PENDING:
        if job.run():
          job.db().status = JobStatus.RUNNING
        else:
          job.db().status = JobStatus.BROKEN
          slot.unlock()
          #deactivate_jobs.append(job)
          self.jobs.remove(job)

      elif job.status() is JobStatus.FAILED:
        job.db().status = JobStatus.FAILED
        slot.unlock()
        deactivate_jobs.append(job)

      elif job.status() is JobStatus.KILLED:
        job.db().status = JobStatus.KILLED
        slot.unlock()
        #deactivate_jobs.append(job)
        self.jobs.remove(job)

      elif job.status() is JobStatus.RUNNING:
        job.db().ping()

      elif job.status() is JobStatus.COMPLETED:
        job.db().status = JobStatus.COMPLETED
        slot.unlock()
        #deactivate_jobs.append(job)
        self.jobs.remove(job)

      # pull job status into the database
      self.db.commit()

    return True


  def available(self):
    return True if self.allocated() < self.size() else False


  def allocated( self ):
    return self.size() - sum([slot.available() for slot in self.slots])


  def size(self):
    return self.total

  

  def pull(self):

    before = self.size()
    total = 0
    self.device_db.ping()
    for idx, slot in enumerate(self.slots):
      if idx < self.device_db.enabled:
        slot.enable()
        total+=1
      else:
        slot.disable()
    self.total = total

    if total!= before:
      enabled = self.total
      total = self.device_db.slots
      print(INFO+f"Updating slots with {enabled}/{total}")
  
    self.db.commit()

  def pop(self):
    for slot in self.slots:
      if slot.available():
          return slot
    return None
