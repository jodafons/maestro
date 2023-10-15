
import traceback, os

from loguru import logger
from typing import Dict, Any, List
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

if bool(os.environ.get("DOCKER_IMAGE",False)):
    from api.base import client
    from enumerations import JobStatus
    import models, schemas
else:
    from maestro.api.base import client
    from maestro.enumerations import JobStatus
    from maestro import models 
    from maestro import schemas


class pilot(client):

    def __init__(self, host):
        client.__init__(self, host, "pilot")
        logger.info(f"connecting to {host}...")


    def join(self, host : str, device: int, size: int, 
                   allocated: int, full: bool, partition: str):
        
        body = schemas.Executor(host=host,device=device,size=size,full=full, 
                                partition=partition, allocated=allocated)
        res = self.try_request(f'join', method="post", body=body.json())
        return schemas.Server(**res) if res else None


class executor(client):

    def __init__(self, host, max_retry: int=5):
        client.__init__(self, host, "executor")
        self.max_retry = max_retry
        self.retry=0

    def start(self, job_id):
        res = self.try_request(f"start_job/{job_id}", method="post")
        return True if res else False

    def describe(self): 
        res = self.try_request("describe", method="get")
        return Executor(**res) if res else None

    def to_close(self):
        return self.retry>self.max_retry





class postgres:

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


class postgres_session:

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

    