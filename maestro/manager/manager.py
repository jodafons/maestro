
__all__ = ["Manager","get_manager_service"]

import traceback, os


from loguru           import logger
from typing           import Dict
from sqlalchemy       import and_
from maestro          import StatusCode
from maestro          import JobStatus
from maestro          import schemas
from maestro.db       import get_db_service, models
from .user            import UserManager
from .task            import TaskManager
from .dataset         import DatasetManager
from .image           import ImageManager

__manager_service = None


class Manager:

    def __init__(self, host : str, envs : Dict[str,str]):
        self.host=host
        self.envs=envs
        logger.info(f"manager...")

    def image(self, user_id : str) -> ImageManager:
        return ImageManager(user_id, self.host,self.envs)

    def task(self, user_id : str) -> TaskManager:
        return TaskManager(user_id, self.host,self.envs)

    def user(self):
        return UserManager(self.host,self.envs)

    def dataset(self, user_id : str) -> DatasetManager:
        return DatasetManager(user_id , self.host,self.envs)


def get_manager_service( host : str=os.environ.get("ORCH_HOST", ""), envs={}) -> Manager:
    global __manager_service
    if not __manager_service:
        __manager_service = Manager(host,envs=envs)
    return __manager_service