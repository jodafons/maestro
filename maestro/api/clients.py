
import requests, socket, traceback
import json, orjson

from loguru import logger
from typing import Dict, Any, List
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

try:
    from api.base import client
    from enumerations import JobStatus
    from models import Task, Job
except:
    from maestro.api.base import client
    from maestro.enumerations import JobStatus
    from maestro.models import Task, Job

class Executor(BaseModel):
    hostname : str
    device   : int

class ExecutorStatus(BaseModel):
    size      : int
    allocated : int
    full      : bool

class Email(BaseModel):
    to      : str
    subject : str
    body    : str

#
# APIs
#

class pilot(client):

    def __init__(self, host):
        client.__init__(self, host, "pilot")
        logger.info(f"Connecting to {host}...")


    def connect(self, hostname, device = -1):

        res = self.try_request('connect', method="post", body=Executor(hostname=hostname, device=device).json())
        if res is None:
            logger.error(f"Not possible to register the current executor with name ({hostname}) into the pilot server.")
            return False
        else:
            logger.info(f"The executor with name ({hostname}) was registered into the pilot server.")
            return True




class executor(client):

    def __init__(self, host):
        client.__init__(self, host,"executor")


    def start(self, job_id):

        res = self.try_request(f"start/{job_id}", method="post")
        if res is None:
            logger.error(f"It is not possible to start job...")
            return False
        else:
            logger.info(f"Job started into the executor...")
            return True


    def run(self):

        res = self.try_request("run", method="get")
        if res is None:
            logger.error(f"It is not possible to run the executor...")
            return False
        else:
            logger.info(f"Run executor...")
            return True


    def status(self):
        
        res = self.try_request("status", method="get")
        if res is None:
            logger.error(f"The executor server with host ({self.host}) is offline.")
            return None
        else:
            logger.info(f"The executor server with host ({self.host}) is online.")
            return ExecutorStatus(**res)




class schedule(client):


    def __init__(self, host):
        client.__init__(self, host, "schedule")


    def run(self):
        res = self.try_request("run", method="get")
        if res is None:
            logger.error(f"The schedule server with host ({self.host}) is offline.")
            return False
        else:
            logger.info(f"The schedule server with host ({self.host}) is online.")
            return True




class postman(client):

    def __init__(self, host):
        client.__init__(self, host, "postman")

    def send(self, to, subject, body):
        res = self.try_request("send", method="post",body = Email(to=to, subject=subject, body=body).json())
        if res is None:
            logger.error(f"It is not possible to send an email to {to}")
            return False
        else:
            logger.info(f"Your email was sent to {to}...")
            return True





class database:

  def __init__( self, host):

    self.host=host
    try:
      self.__engine = create_engine(host)
      self.__session_maker = sessionmaker(bind=self.__engine)
      # NOTE: create a session
      self.__session = self.__session_maker()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)

  def __del__(self):
    self.commit()
    self.close()

  def engine(self):
    return self.__engine

  def session(self):
    return self.__session


  #
  # NOTE: expose the session maker in case of create a new session
  #
  def __call__(self):
    return self.__session_maker()

  def commit(self):
    self.session().commit()

  def close(self):
    self.session().close()

  def generate_id( self, model  ):
    if self.session().query(model).all():
      return self.session().query(model).order_by(model.id.desc()).first().id + 1
    else:
      return 0


  def task( self, task ):
    try:
      if type(task) is int:
        return self.session().query(Task).filter(Task.id==task).with_for_update().first()
      elif type(task) is str:
        return self.session().query(Task).filter(Task.name==task).with_for_update().first()
      else:
        logger.error("taskname (str) or task id (int) should be passed to task retrievel...")
        return None
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None


  def get_n_jobs(self, njobs, status=JobStatus.ASSIGNED):
    try:
      jobs = self.session().query(Job).filter(  Job.status==status  ).order_by(Job.id).limit(njobs).with_for_update().all()
      jobs.reverse()
      return jobs
    except Exception as e:
      logger.error(f"Not be able to get {njobs} from database. Return an empty list to the user.")
      traceback.print_exc()
      return []

  def job( self, job_id ):
    try:
      return self.session().query(Job).filter(Job.id==job_id).first()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None

      
  def is_alive(self):
    try:
      self.session().query(Job)
      return True
    except:
      return False