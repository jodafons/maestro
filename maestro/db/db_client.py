__all__ = [
    "get_db_service", 
    "recreate_db"
]


import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# local package
from maestro import random_id
from .models import DBJob, DBTask, DBDataset, Base
from .models import Job, Task

__db_service = None


#
# DB services
#

class DBService:

    def __init__(self, db_string : str=os.environ.get("DB_STRING","")):
        self.__engine    = create_engine(db_string, pool_size=50, max_overflow=0)
        self.__session   = sessionmaker(bind=self.__engine,autocommit=False, autoflush=False)
        self.db_string   = db_string

    def task(self, task_id : str) -> DBTask:
        return DBTask(task_id, self.__session)

    def job(self, job_id : str) -> DBJob:
        return DBJob(job_id, self.__session)

    def dataset(self, dataset_id : str) -> DBDataset:
        return DBDataset(dataset_id, self.__session)

    def __call__(self):
        return self.__session()
    
    def session(self):
        return self.__session()
    
    def engine(self):
        return self.__engine

    def save_job(self, job : Job):
        session = self.session()
        try:
            session.add(job)
            session.commit()
        finally:
            session.close()

  
    def save_task(self, task: Task):
        session = self.session()
        task.start_time = datetime.now()
        try:
            task_monitor            = TaskMonitor()
            task_monitor.task_id    = task.task_id
            task_monitor.user_id    = task.user_id
            task_monitor.name       = task.name
            task_monitor.registered = len(task.jobs)
            task_monitor.assigned   = 0
            task_monitor.pending    = 0
            task_monitor.running    = 0
            task_monitor.failed     = 0
            task_monitor.broken     = 0
            task_monitor.kill       = 0
            task_monitor.killed     = 0
            task_monitor.completed  = 0 
            task_monitor.start_time = task.start_time
            task_monitor.update()
            session.add(task)
            session.add(task_monitor)
            session.commit()
        finally:
            session.close()

    def check_task_existence( self, task_id : str ) -> bool:
        return self.task(task_id).check_existence()

    def check_job_existence( self, job_id : str ) -> bool:
        return self.job(job_id).check_existence()

    def check_task_existence_by_name( self,  name : str ) -> bool:
        session = self.__session()
        try:
           dataset = session.query( 
                    session.query(Task).filter_by(name=name).exists() 
           ).scalar()
           return dataset
        finally:
            session.close()    

    def fetch_task_from_name( self,  name : str) -> str:
        session = self.__session()
        try:
           task = session.query(Task).filter_by(name=name).one()
           return task.task_id
        finally:
            session.close() 
            
   
 
def get_db_service( db_string : str=os.environ.get("DB_STRING","")):
    global __db_service
    if not __db_service:
        __db_service = DBService(db_string)
    return __db_service


def recreate_db():
    db_service = get_db_service()
    Base.metadata.drop_all(db_service.engine())
    Base.metadata.create_all(db_service.engine())