__all__ = ["get_io_service"]

import os
import shutil
import pickle
from loguru         import logger
from typing         import Any
from expand_folders import expand_folders
from maestro        import schemas, random_id, md5checksum
from maestro.db     import get_db_service, models


__io_service = None


class IOJob:

    def __init__(self, job_id : str, volume : str):
        self.volume   = volume
        self.job_id   = job_id
        db_service    = get_db_service()
        self.task_id  = db_service.job(job_id).fetch_task()
        self.basepath = f"{self.volume}/tasks/{self.task_id}/{self.job_id}"

    def mkdir(self):
        os.makedirs(self.basepath, exist_ok=True)
        return self.basepath


class IODataset:

    def __init__(self, dataset_id : str, volume : str):
        self.volume     = volume
        self.dataset_id = dataset_id
        db_service      = get_db_service()
        self.name       = db_service.dataset(dataset_id).fetch_name()
        self.basepath   = f"{self.volume}/datasets/{name}"

    def count(self):
        return len(expand_folders(self.basepath)) if os.path.exists(self.basepath) else 0
        
    def mkdir(self):
        os.makedirs(self.basepath, exist_ok=True)
        return self.basepath
    
    def files(self):
        db_service = get_db_service()
        return db_service.dataset(self.dataset_id).get_all_file_ids()
    
    def save(self, filepath : str, filename : str=None ):
        
        if not filename:
            filename = filepath.split('/')[-1]
        targetpath = f"{self.basepath}/{filename}"
        
        try:
            shutil.copy( filepath, targetpath)
        except:
            logger.error(f"its not possible to copy from {filepath} to {targetpath}")
            return False
        
        try:
            db_service      = get_db_service()
            if not db_service.dataset(self.dataset_id).check_file_existence_by_name( filename ):
                with db_service() as session:
                    file_id = random_id()
                    dataset_db = session.query(models.Dataset).filter_by(dataset_id=self.dataset_id).one()
                    file_db          = models.File(file_id=file_id, dataset_id=self.dataset_id)
                    file_db.name     = filename
                    file_db.file_md5 = md5checksum( filepath )
                    dataset_db.files.append(file_db)
                    session.commit()
            else:
                with db_service() as session:
                    file_db = session.query(models.File).filter_by(dataset_id=self.dataset_id).filter_by(name=filename).one()
                    file_db.file_md5 = md5checksum( filepath )
                    session.commit()
        except:
            logger.error(f"its not possible to commit into the database.")
            return False
        
        return True




    def load(self, filename : str) -> Any:
        
        filepath=f"{self.basepath}/{filename}"
        if not self.check_existence(filename):
            raise QioError(f"file with name {filename} does not exist into the dataset and storage.")
        
        if filename.endswith(".pkl"):
            with open(filepath, 'rb') as f:
                object = pickle.load(f)
        elif filename.endswith(".json"):
            with open(filepath, 'r') as f:
                object = schemas.json_load(f)
        else:
            raise QioError(f"Its not possible load file with name {filename} using this extension.")
        return object

    def check_existence(self, filename):
        return os.path.exists(f"{self.basepath}/{filename}")



class IOService:

    def __init__(self, volume : str):
        self.volume = volume
        os.makedirs(f"{volume}", exist_ok=True)
        os.makedirs(f"{volume}/tasks", exist_ok=True)
        os.makedirs(f"{volume}/datasets", exist_ok=True)
        

    def job(self, job_id : str) -> IOJob:
        return IOJob(job_id, self.volume)

    def dataset(self, dataset_id : str) -> IODataset:
        return IODataset(dataset_id, self.volume)


#
# get database service
#
def get_io_service( volume : str=f"{os.getcwd()}/volume" ) -> IOService:
    global __io_service
    if not __io_service:
        __io_service = IOService(volume)
    return __io_service