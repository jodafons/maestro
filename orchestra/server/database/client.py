
__all__ = ["Database"]


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.database import Task, Job, Device
from orchestra.utils import *
from orchestra.status import TaskStatus, JobStatus
import traceback
from colorama import *
from colorama import init
init(autoreset=True)


INFO = Style.BRIGHT + Fore.GREEN
ERROR = Style.BRIGHT + Fore.RED



class postgres_client:

  def __init__( self, host):

    self.host=host
    print(INFO+f"Connecting into {host}")

    try:
      self.__engine = create_engine(url)
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


  def create( self, name, workarea ):
                         
    try:
      task = Task(
        id=self.generateId(Task),
        name=name,
        workarea=workarea,
        status=TaskStatus.REGISTERED)
      return task
    except Exception as e:
      traceback.print_exc()
      print(ERROR+e)
      return None


  def attach( self, task, jobname, inputfile, command, 
              status=JobStatus.REGISTERED, 
              id=None):
    try:
      job = Job(
        id=self.generateId(Job) if id is None else id,
        command=command,
        name = jobname,
        inputfile=inputfile,
        status=status)
      task+=job
      return job
    except Exception as e:
      traceback.print_exc()
      print(ERROR+e)
      return None


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



  def generateId( self, model  ):
    if self.session().query(model).all():
      return self.session().query(model).order_by(model.id.desc()).first().id + 1
    else:
      return 0











