
__all__ = ["Consumer"]


import os, time, subprocess, traceback, psutil
from database.models import Device
from . import JobStatus



class Job:

  def __init__(self, job_db, slot, extra_envs={}):

    self.job = job_db
    self.slot = slot
    self.volume = job_db.volume()
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
   


  #
  # Run the job process
  #
  def run(self):

    os.makedirs(self.volume, exist_ok=True)
    try:
      self.pending=False
      self.killed=False
      self.broken=False
      command = 'cd %s' % self.volume + ' && '
      command+= self.command
      self.__proc = subprocess.Popen(command, env=self.env, shell=True)
      time.sleep(2)
      self.__proc_stat = psutil.Process(self.__proc.pid)
      return True

    except Exception as e:
      traceback.print_exc()
      print(e)
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
  # Update timer into the database
  #
  def ping(self):
    self.job_db.ping()



#
# Individual Slot to control the Queue
#
class Slot:

  def __init__(self, device=-1):
    self.device = device
    self.__enable = False
    self.__available = True

  def available( self ):
    return (self.__available and self.__enable)

  def lock( self ):
    self.available = False

  def unlock( self ):
    self.__available = True

  def enable(self):
    self.__enable=True

  def disable(self):
    self.__enable=False

  def enabled(self):
    return self.__enable




#
# A collection of slots for each device (CPU and GPU)
#
class Consumer:

  def __init__(self, device, db):
    self.db = db
    self.device_db = device
    self.slots = []
    self.total = 0

    self.slots = [Slot(device.gpu) for _ in range(self.device_db.slots)]
    for slot_id in range(self.device_db.enabled):
        self.slots[slot_id].enable()
        self.__total+=1



  def run(self):

    self.pull()

    # Loop over all available consumers
    for _, job in enumerate(self.__slots):

      job_db = job.job_db
      slot = job.slot

      if job.status == JobStatus.KILL:
        job.kill()

      if job.status() == JobStatus.PENDING:
        if not job.run():
          job_db.status = JobStatus.BROKEN
          slot.unlock()
          self.__slots.remove(job)
        else: # change to running State
          job_db.status = JobStatus.RUNNING

      elif job.status() is JobStatus.FAILED:
        job_db.status = JobStatus.FAILED
        slot.unlock()
        self.__slots.remove(job)

      elif job.status() is JobStatus.KILLED:
        job_db.status = JobStatus.KILLED
        slot.unlock()
        self.__slots.remove(job)

      elif job.status() is JobStatus.RUNNING:
        job.ping()

      elif job.status() is JobStatus.DONE:
        job_db.status = JobStatus.DONE
        slot.unlock()
        self.__slots.remove(job)

      # pull job status into the database
      self.db.commit()

    return True


  def available(self):
    return True if len(self.slots) < self.size() else False


  def allocated( self ):
    return len(self.__slots)


  

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
      print('Updating slots with {enabled}/{total}')
  



  #
  # Add a job into the slot
  #
  def push_back( self, job_db ):
    slot = self.__get_slot()
    if not slot:
      return False
    job = Job( job_db , slot )
    job_db.status = JobStatus.PENDING
    self.slots.append(job)
    job.ping()
    slot.lock()




  def get_slot(self):
    for slot in self.slots:
      if slot.available():
          return slot
    return None
