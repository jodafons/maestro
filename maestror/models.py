
__all__ = ["Database", "Task", "Job", "Env"]


import datetime, traceback, os
import numpy as np
from maestror.enumerations import JobStatus, TaskStatus, TaskTrigger, job_status
from sqlalchemy import create_engine, Column, Boolean, Float, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from loguru import logger



Base = declarative_base()


#
# mimic an environ dict into the postgres database
#
class Env (Base):
  __tablename__ = 'env'
  # Local
  id        = Column(Integer, primary_key = True)
  key       = Column(String, unique=True)
  value     = Column(String)


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

  # NOTE: mlflow id param
  experiment_id    = Column(String)

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

  def count(self):
    total = { str(key):0 for key in job_status }
    for job in self.jobs:
      for s in job_status:
        if job.status==s: total[s]+=1
    return total

  def sys_used_memory(self):
    used_memory = [job.sys_used_memory for job in self.jobs if job.sys_used_memory >= 0]
    return int(np.mean(used_memory) if len(used_memory) > 0 else -1)
   
  def gpu_used_memory(self):
    used_memory = [job.gpu_used_memory for job in self.jobs if job.gpu_used_memory >= 0]
    return int(np.mean(used_memory) if len(used_memory) > 0 else -1)

  def cpu_percent(self):
    cpu_percent = [job.cpu_percent for job in self.jobs if job.cpu_percent >= 0]
    return int(np.mean(cpu_percent) if len(cpu_percent) > 0 else -1)
 



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

  # NOTE: mlflow id param
  run_id    = Column(String)

  # NOTE: extra info, can be removed in future
  sys_used_memory     = Column(Float, default=-1)
  gpu_used_memory     = Column(Float, default=-1)
  cpu_percent         = Column(Float, default=-1)
  decorator           = Column(String, default="{}")



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


  def set_decorator(self, key, value):
    decorator = eval(self.decorator)
    decorator[key]=value
    self.decorator = str(decorator)


  def get_decorator(self, key):
    return eval(self.decorator).get(key, "")


class Database:

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
    return Session( self.__session() )

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
        task = self.__session.query(Task).filter(Task.id==task)
      elif type(task) is str:
        task = self.__session.query(Task).filter(Task.name==task)
      else:
        raise ValueError("task name or id should be passed to task retrievel...")
      return task.with_for_update().first() if with_for_update else task.first()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None


  def get_n_jobs(self, njobs, status=JobStatus.ASSIGNED, with_for_update=False):
    try:
      jobs = self.__session.query(Job).filter(  Job.status==status  ).order_by(Job.id).limit(njobs)
      jobs = jobs.with_for_update().all() if with_for_update else jobs.all()
      jobs.reverse()
      return jobs
    except Exception as e:
      logger.error(f"not be able to get {njobs} from database. return an empty list to the user.")
      traceback.print_exc()
      return []


  def get_job( self, job_id,  with_for_update=False):
    try:
      job = self.__session.query(Job).filter(Job.id==job_id)
      return job.with_for_update().first() if with_for_update else job.first()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None


  def set_environ( self, key : str, value : str):
    env = self.__session.query(Env).filter(Env.key==key).first()
    if not env:
      id = self.generate_id(Env)
      logger.info(f"setting new environ ({id}) as {key}:{value}")
      env = Env( id = id, key=key, value=value)
      self.__session.add(env)
    else:
      env.value = value
    self.commit()
    

  def get_environ( self, key : str):
    try:
      env = self.__session.query(Env).filter(Env.key==key).first()
      return env.value if env else None
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None

    

