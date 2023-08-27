
import datetime, traceback

from models import Job
from enumerations import JobStatus, TaskStatus, TaskTrigger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger


class client_postgres:

  def __init__( self, host):

    self.host=host
    logger.info(f"Connecting into {host}")
    try:
      self.__engine = create_engine(host)
      Session= sessionmaker(bind=self.__engine)
      self.__session = Session()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)


  def __del__(self):
    self.commit()
    self.close()

  def session(self):
    return self.__session

  def commit(self):
    self.session().commit()

  def close(self):
    self.session().close()


  def get_n_jobs(self, njobs, status=JobStatus.ASSIGNED):
    try:
      jobs = self.session().query(Job).filter(  Job.status==status  ).order_by(Job.id).limit(njobs).with_for_update().all()
      jobs.reverse()
      return jobs
    except Exception as e:
      logger.error(f"Not be able to get {njobs} from database. Return an empty list to the user.")
      traceback.print_exc()
      return []