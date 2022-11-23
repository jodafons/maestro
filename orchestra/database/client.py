
__all__ = ["postgres_client"]


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.database import Task, Job, Device
from orchestra.status import TaskStatus, JobStatus
from orchestra import INFO, ERROR
import traceback


class postgres_client:

  def __init__( self, host):

    self.host=host
    print(INFO+f"Connecting into {host}")

    try:
      self.__engine = create_engine(host)
      Session= sessionmaker(bind=self.__engine)
      self.__session = Session()
    except Exception as e:
      traceback.print_exc()
      print(ERROR+e)


  def __del__(self):
    self.commit()
    self.close()


  def session(self):
    return self.__session

  def commit(self):
    self.session().commit()


  def close(self):
    self.session().close()


  def task( self, name ):
    try:
      return self.session().query(Task).filter(Task.name==name).first()
    except Exception as e:
      traceback.print_exc()
      print(ERROR+e)
      return None


  def tasks(self):
    try:
      return self.session().query(Task).all()
    except Exception as e:
      traceback.print_exc()
      print(ERROR+e)
      return None


  def devices(self):
    try:
      return self.session().query(Device).all()
    except Exception as e:
      traceback.print_exc()
      print(ERROR+e)
      return None



  def generate_id( self, model  ):
    if self.session().query(model).all():
      return self.session().query(model).order_by(model.id.desc()).first().id + 1
    else:
      return 0











