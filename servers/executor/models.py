
import datetime, traceback

from enumerations import JobStatus, TaskStatus, TaskTrigger
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from loguru import logger



Base = declarative_base()


#
#   Tasks Table
#
class Task (Base):

  __tablename__ = 'task'

  # Local
  id       = Column(Integer, primary_key = True)
  name     = Column(String, unique=True)
  volume   = Column(String)
  status   = Column(String, default=TaskStatus.REGISTERED)
  trigger  = Column( String, default=TaskTrigger.WAITING )
  # Foreign
  jobs     = relationship("Job", order_by="Job.id", back_populates="task")
  

  #
  # Method that adds jobs into task
  #
  def __add__ (self, job):
    self.jobs.append(job)
  
  
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



