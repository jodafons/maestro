import traceback

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

try:
    from enumerations import JobStatus
    from models import Task, Job
except:
    from maestro.enumerations import JobStatus
    from maestro.models import Task, Job


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
    logger.info("Create session.")
    return postgres_session( self.__session() )

  def __enter__(self):
    self.__last_session = self.__call__()
    return self.__last_session

  def __exit__(self, *args, **kwargs):
    if self.__last_session:
      #self.__last_session.commit()
      logger.info("Close session.")
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


  def task( self, task, with_for_update=False ):
    try:
      if type(task) is int:
        task = self.__session.query(Task).filter(Task.id==task)
      elif type(task) is str:
        task = self.__session.query(Task).filter(Task.name==task)
      else:
        raise ValueError("taskname (str) or task id (int) should be passed to task retrievel...")
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
      logger.error(f"Not be able to get {njobs} from database. Return an empty list to the user.")
      traceback.print_exc()
      return []


  def job( self, job_id,  with_for_update=False):
    try:
      job = self.__session.query(Job).filter(Job.id==job_id)
      return job.with_for_update().first() if with_for_update else job.first()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None

      
  def ping(self):
    try:
      self.__session.query(Job)
      return True
    except:
      return False