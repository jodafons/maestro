
__all__ = ["Postgres", "Task", "Job"]


import datetime, traceback, os

from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger, job_status
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
  user      = Column(String)

  # NOTE: aux variable
  to_remove = Column(Boolean, default=False)


  #
  # Method that adds jobs into task
  #
  def __add__ (self, job):
    self.jobs.append(job)
    return self
  

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
  image     = Column(String , default="")
  command   = Column(String , default="")
  status    = Column(String , default=JobStatus.REGISTERED)
  retry     = Column(Integer, default=0)
  workarea  = Column(String)
  inputfile = Column(String)
  timer     = Column(DateTime)
  envs      = Column(String, default="{}")
  binds     = Column(String, default="{}")
  partition = Column(String, default='cpu')


  # Foreign
  task    = relationship("Task", back_populates="jobs")
  taskid  = Column(Integer, ForeignKey('task.id'))
  
  def get_envs(self):
    return eval(self.envs)
  
  def get_binds(self):
    return eval(self.binds)

  def ping(self):
    self.timer = datetime.datetime.now()

  def is_alive(self):
    return True  if (self.timer and ((datetime.datetime.now() - self.timer).total_seconds() < 30)) else False



#
#   Environ global control
#
class Environ (Base):
  __tablename__ = 'environ'
  # Local
  id        = Column(Integer, primary_key = True)
  key       = Column(String, unique=True)
  value     = Column(String)










class Postgres:

  def __init__(self, host):
    self.host=host
    self.__last_session = None
    try:
      self.__engine = create_engine(self.host)
      self.__session = sessionmaker(autocommit=False, autoflush=False, bind=self.__engine)
    except Exception as e:
      traceback.print_exc()
      logger.critical(e)

  def engine(self):
    return self.__engine

  def __del__(self):
    if self.__last_session:
      self.__last_session.close()    

  def __call__(self):
    return postgres_session( self.__session() )

  def __enter__(self):
    self.__last_session = self.__call__()
    return self.__last_session

  def __exit__(self, *args, **kwargs):
    if self.__last_session:
      self.__last_session.close()


class Session:

  def __init__( self, session):
    self.__session = session

  def __del__(self):
    self.commit()
    self.close()

  def __call__(self):
    return self.__session

  def commit(self):
    self.__session.commit()

  def close(self):
    self.__session.close()

  def generate_id( self, model  ):
    if self.__session.query(model).all():
      return self.__session.query(model).order_by(model.id.desc()).first().id + 1
    else:
      return 0

  def get_task( self, task, with_for_update=False ):
    try:
      if type(task) is int:
        task = self.__session.query(models.Task).filter(models.Task.id==task)
      elif type(task) is str:
        task = self.__session.query(models.Task).filter(models.Task.name==task)
      else:
        raise ValueError("task name or id should be passed to task retrievel...")
      return task.with_for_update().first() if with_for_update else task.first()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None

  def get_user( self, user, with_for_update=False ):
    try:
      if type(user) is int:
        user = self.__session.query(models.User).filter(models.User.id==user)
      elif type(user) is str:
        user = self.__session.query(models.User).filter(models.User.name==user)
      else:
        raise ValueError("user name or id should be passed to user retrievel...")
      return user.with_for_update().first() if with_for_update else user.first()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None

  def get_n_jobs(self, njobs, status=JobStatus.ASSIGNED, with_for_update=False):
    try:
      jobs = self.__session.query(models.Job).filter(  models.Job.status==status  ).order_by(models.Job.id).limit(njobs)
      jobs = jobs.with_for_update().all() if with_for_update else jobs.all()
      jobs.reverse()
      return jobs
    except Exception as e:
      logger.error(f"not be able to get {njobs} from database. return an empty list to the user.")
      traceback.print_exc()
      return []

  def get_job( self, job_id,  with_for_update=False):
    try:
      job = self.__session.query(models.Job).filter(models.Job.id==job_id)
      return job.with_for_update().first() if with_for_update else job.first()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None

    