
import datetime, traceback

from maestro.enumerations import job_status
from maestro.models import Task
from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger
from prettytable import PrettyTable

class client_postgres:

  def __init__( self, host):

    self.host=host
    #logger.info(f"Connecting into {host}")
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


  def engine(self):
    return self.__engine

  def tasks(self):
    try:
      return self.session().query(Task).all()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return []


  def generate_id( self, model  ):
    if self.session().query(model).all():
      return self.session().query(model).order_by(model.id.desc()).first().id + 1
    else:
      return 0



  def resume( self , skip_status = [JobStatus.COMPLETED]):
    
    cols = ["ID", "Task"]; cols.extend(job_status); cols.extend(["Status"])
    t = PrettyTable(cols)
    for task in self.tasks():
        if task.status in skip_status:
          continue  
        resume = task.resume()
        values = [task.id, task.name]
        values.extend( [ resume[status] for status in job_status ] )
        values.extend( [task.status] )
        t.add_row( values )
    return t


  def task( self, name ):
    try:
      return self.session().query(Task).filter(Task.name==name).first()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return None



