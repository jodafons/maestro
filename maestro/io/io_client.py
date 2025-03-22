__all__ = ["get_io_service"]

import os
import shutil
import pickle
from loguru         import logger
from typing         import Any, Callable
from expand_folders import expand_folders
from maestro        import schemas, random_id, md5checksum
from maestro.db     import get_db_service, models


__io_service = None


class IOJob:

    def __init__(self, job_id : str, volume : str):
        self.volume    = volume
        self.job_id    = job_id
        db_service     = get_db_service()   
        user_id        = db_service.job(job_id).fetch_owner()
        self.user_name = db_service.user(user_id).fetch_name()
        task_id        = db_service.job(job_id).fetch_task()
        self.task_name = db_service.task(task_id).fetch_name()
        self.basepath  = f"{self.volume}/tasks/{self.task_name}/{self.job_id}"

    def mkdir(self):
        os.makedirs(self.basepath, exist_ok=True)
        return self.basepath


class IODataset:

    def __init__(self, dataset_id : str, volume : str):
        self.volume     = volume
        self.dataset_id = dataset_id
        db_service      = get_db_service()
        user_id         = db_service.dataset(dataset_id).fetch_owner()
        self.user_name  = db_service.user(user_id).fetch_name()
        self.name       = db_service.dataset(dataset_id).fetch_name()
        self.basepath   = f"{self.volume}/datasets/{self.name}"

    def count(self):
        return len(self.files())
        
        
    def mkdir(self):
        os.makedirs(self.basepath, exist_ok=True)
        return self.basepath
    
    def files(self, with_file_id : bool=False):
        db_service = get_db_service()
        files = db_service.dataset(self.dataset_id).get_all_file_ids()
        return list(files.keys()) if with_file_id else files
    
    
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
                    dataset_db       = session.query(models.Dataset).filter_by(dataset_id=self.dataset_id).one()
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


    def load(self, filename : str, load_f : Callable=None) -> Any:
        
        filepath=f"{self.basepath}/{filename}"
        if not self.check_existence(filename):
            raise RuntimeError(f"file with name {filename} does not exist into the dataset and storage.")
        
        if filename.endswith(".pkl"):
            with open(filepath, 'rb') as f:
                object = pickle.load(f)
        elif filename.endswith(".json"):
            with open(filepath, 'r') as f:
                object = schemas.json_load(f)
        elif load_f:
            object = load_f(filepath)
        else:
            raise RuntimeError(f"Its not possible load file with name {filename} using this extension.")
        return object

    def check_existence(self, filename):
        return os.path.exists(f"{self.basepath}/{filename}")



class IOImage:

    def __init__(self, dataset_id : str, volume : str):
        self.volume     = volume
        self.dataset_id = dataset_id
        db_service      = get_db_service()
        user_id         = db_service.dataset(dataset_id).fetch_owner()
        self.user_name  = db_service.user(user_id).fetch_name()
        self.name       = db_service.dataset(dataset_id).fetch_name()
        self.basepath   = f"{self.volume}/images/{self.name}"
        
    def mkdir(self):
        os.makedirs(self.basepath, exist_ok=True)
        return self.basepath
    
    def path(self):
        image = io_service.dataset(self.dataset_id).files()
        return f"{self.basepath}/{image[0]}" if len(image) == 1 else None

    def check_existence(self, filename):
        return io_service.dataset(self.dataset_id).count() == 1    
        
        



class IOService:

    def __init__(self, volume : str):
        self.volume = volume
        os.makedirs(f"{volume}", exist_ok=True)  

    def job(self, job_id : str) -> IOJob:
        return IOJob(job_id, self.volume)

    def dataset(self, dataset_id : str) -> IODataset:
        return IODataset(dataset_id, self.volume)

    def image(self, dataset_id : str) -> IOImage:
        return IOImage(dataset_id, self.volume)
#
# get database service
#
def get_io_service( volume : str=f"{os.getcwd()}/volume" ) -> IOService:
    global __io_service
    if not __io_service:
        __io_service = IOService(volume)
    return __io_service