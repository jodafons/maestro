
__all__ = ['Task', 'Job', 'Device']

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from orchestra.status import *
import datetime


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
    action   = Column( String, default=TaskAction.WAITING )

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
    command   = Column(String , default="")
    status    = Column(String , default=JobStatus.REGISTERED)
    retry     = Column(Integer, default=0)
    workarea  = Column(String)
    inputfile = Column(String)
    timer     = Column(DateTime)

    # Foreign
    task    = relationship("Task", back_populates="jobs")
    taskid  = Column(Integer, ForeignKey('task.id'))


    def ping(self):
      self.timer = datetime.datetime.now()

    def is_alive(self):
      return True  if (self.timer and ((datetime.datetime.now() - self.timer).total_seconds() < 30)) else False




class Device (Base):

  __tablename__ = 'device'

  id          = Column(Integer, primary_key = True)
  host        = Column(String)
  slots       = Column( Integer )
  enabled     = Column( Integer )
  gpu         = Column( Integer , default=-1)
  timer       = Column(DateTime)

  def ping(self):
    self.timer = datetime.datetime.now()

  def is_alive(self):
    return True  if (self.timer and ((datetime.datetime.now() - self.timer).total_seconds() < 30)) else False


