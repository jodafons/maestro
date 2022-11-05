__all__=['Task']


from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship



from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


#
#   Tasks Table
#
class Task (Base):

    __tablename__ = 'task'
  
    id        = Column(Integer, primary_key = True)
    taskname  = Column(String, unique=True)
    status    = Column(String, default="registered")
    order     = Column( String, default='waiting' )
    jobs      = relationship("Job", order_by="Job.id", back_populates="task")

    #
    # Method that adds jobs into task
    #
    def __add__ (self, job):
      self.jobs.append(job)
  
  

#   Jobs Table
#
class Job (Base):

    __tablename__ = 'job'

    # Local
    id        = Column(Integer, primary_key = True)
    command   = Column(String , default="")
    status    = Column(String , default="registered")
    retry     = Column(Integer, default=0)
    workarea  = Column(String)



    # Foreign
    task     = relationship("Task", back_populates="jobs")
    task_id  = Column(Integer, ForeignKey('task.id'))

    node     = Column(String)
    resume   = Column(String)
    timer    = Column(DateTime)



  
    def ping(self):
      self.timer = datetime.datetime.now()


    def is_alive(self):
      return True  if (self.timer and ((datetime.datetime.now() - self.timer).total_seconds() < 30)) else False



