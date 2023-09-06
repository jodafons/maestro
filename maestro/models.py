
__all__ = []



import datetime, traceback

try:
  from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger, job_status
except:
  from enumerations import JobStatus, TaskStatus, TaskTrigger, job_status

from sqlalchemy import create_engine, Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from loguru import logger



Base = declarative_base()


#
#   Tasks Table
#
class Task (Base):

  __tablename__ = 'task'

  # Local
  id        = Column(Integer, primary_key = True)
  name      = Column(String, unique=True)
  volume    = Column(String)
  status    = Column(String, default=TaskStatus.REGISTERED)
  trigger   = Column(String, default=TaskTrigger.WAITING )
  # Foreign 
  jobs      = relationship("Job", order_by="Job.id", back_populates="task")

  # NOTE: aux variable
  to_remove = Column(Boolean, default=False)

  # TODO: Should be removed from here.
  email     = Column(String)

  #
  # Method that adds jobs into task
  #
  def __add__ (self, job):
    self.jobs.append(job)
  

  def completed(self):
    return self.status==TaskStatus.COMPLETED

  def kill(self):
    self.trigger = TaskTrigger.KILL

  def retry(self):
    self.trigger = TaskTrigger.RETRY

  def reset(self):
    self.trigger = TaskTrigger.WAITING

  def delete(self):
    self.trigger = TaskTrigger.DELETE

  def remove(self):
    self.to_remove = True

  def resume(self):
    
    d = { str(status):0 for status in job_status }
    for job in self.jobs:
      d[job.status]+=1
    return d



#
#   Jobs Table
#
class Job (Base):

  __tablename__ = 'job'
  # Local
  id        = Column(Integer, primary_key = True)
  name      = Column(String)
  image     = Column(String)
  command   = Column(String , default="")
  status    = Column(String , default=JobStatus.REGISTERED)
  retry     = Column(Integer, default=0)
  workarea  = Column(String)
  inputfile = Column(String)
  timer     = Column(DateTime)
  envs      = Column(String)
  # Foreign
  task    = relationship("Task", back_populates="jobs")
  taskid  = Column(Integer, ForeignKey('task.id'))
  
  def get_env(self, key):
    return eval(self.envs).get(key,'')
  
  def ping(self):
    self.timer = datetime.datetime.now()

  def is_alive(self):
    return True  if (self.timer and ((datetime.datetime.now() - self.timer).total_seconds() < 30)) else False



