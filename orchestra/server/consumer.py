
__all__ = ["Consumer"]


import os, time, subprocess, traceback, psutil
from database.models import Device
from orchestra.status import JobStatus
from orchestra.server import Slot



class Job:

  def __init__(self, job_db, slot, extra_envs={}):

    self.job = job_db
    self.slot = slot
    self.workarea = job_db.workarea
    self.command = job_db.command
    self.pending=True
    self.broken=False
    self.killed=False
    self.env = os.environ.copy()
    self.env["JOB_WORKAREA"] = self.workarea
    self.env["CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
    self.env["CUDA_VISIBLE_DEVICES"]=str(slot.device)
    self.env["TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'
   
    # Update the job enviroment from external envs
    for key, value in extra_envs.items():
      self.env[key]=value

    # process
    self.__proc = None
    self.__proc_stat = None
   

  def db(self):
    return self.job_db


  #
  # Run the job process
  #
  def run(self):

    os.makedirs(self.workarea, exist_ok=True)
    try:
      self.pending=False
      self.killed=False
      self.broken=False
      command = 'cd %s' % self.workarea + ' && '
      command+= self.command
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
        self.__total+=1
    self.jobs = []


  #
  # Add a job into the slot
  #
  def __add__( self, job_db ):
    slot = self.pop()
    if slot:
      job = Job( job_db , slot )
      job_db.status = JobStatus.PENDING
      self.jobs.append(job)
      job.ping()
      slot.lock()
      return True
    else:
      return False




  def run(self):

    self.pull()

    deactivate_jobs = []

    # Loop over all available consumers
    for _, job in enumerate(self.jobs):

      slot = job.slot

      if job.db().status == JobStatus.KILL:
        job.kill()

      if job.status() == JobStatus.PENDING:
        if job.run():
          job.db().status = JobStatus.RUNNING
        else:
          job.db().status = JobStatus.BROKEN
          slot.unlock()
          deactivate_jobs.append(job)

      elif job.status() is JobStatus.FAILED:
        job.db().status = JobStatus.FAILED
        slot.unlock()
        deactivate_jobs.append(job)

      elif job.status() is JobStatus.KILLED:
        job.db().status = JobStatus.KILLED
        slot.unlock()
        deactivate_jobs.append(job)

      elif job.status() is JobStatus.RUNNING:
        job.db().ping()

      elif job.status() is JobStatus.DONE:
        job.db().status = JobStatus.DONE
        slot.unlock()
        deactivate_jobs.append(job)

      # pull job status into the database
      self.db.commit()

      # remove jobs from the queue
      for job in deactivate_jobs:
        self.jobs.remove(job)

    return True


  def available(self):
    return True if len(self.slots) < self.size() else False

  def allocated( self ):
    return len(self.__slots)

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
  





  def pop(self):
    for slot in self.slots:
      if slot.available():
          return slot
    return None
