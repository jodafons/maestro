
__all__ = ['postgres_client']


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.db import Task, Job, Device
from orchestra.utils import *
import traceback

from colorama import *
from colorama import init
init(autoreset=True)


class postgres_client:

    def __init__(self, host):
        self.host = host
        try:
            print(Style.BRIGHT + Fore.GREEN + "Connecting with " + host)
            self.__engine = create_engine(host)
            Session= sessionmaker(bind=self.__engine)
            self.session = Session()
        except Exception as e:
            traceback.print_exc()
            print(e)

    def __del__(self):
        self.commit()
        self.session.close()

    def commit(self):
        self.session.commit()


    #
    # Generate the ID given the model schemma
    #
    def generateId( self, model  ):
        if self.session().query(model).all():
            return self.session().query(model).order_by(model.id.desc()).first().id + 1
        else:
            return 0


    def task( self, taskname ):
        try:
            return self.session().query(Task).filter(Task.taskname==taskname).first()
        except Exception as e:
            traceback.print_exc()
            print(Style.BRIGHT + Fore.RED + e)
            return None


    def tasks(self):
        try:
            return self.session().query(Task).all()
        except Exception as e:
            traceback.print_exc()
            print(Style.BRIGHT + Fore.RED + e)
            return None