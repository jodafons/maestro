__all__ = ["Dataset"]


from tabulate import tabulate
from typing import List, Union, Dict
from .client import get_session_api
from . import schemas


class Dataset:
    '''
  
    '''

    def __init__(
        self,
        name        : str,
        description : str="",
    ) -> None:
        
        self.__api_client = get_dell_runtime_session_api()
        self.name         = name
        self.description  = description
        if self.__api_client.dataset().check_existence(name):
            self.dataset_id = self.__api_client.dataset().identity(name)
        else:
            self.dataset_id = None

    def print(self):
        files = self.describe()
        headers = ["filename", "md5", "size (mb)"]
        table = [ [f['filename'], f['md5'], f['filesize_mb']] for f in files ]
        table = tabulate(table, headers=headers, tablefmt="psql")
        print(f"Dataset name : {self.name}"   )
        print(f"dataset id   : {self.dataset_id}")
        print(table)

    def describe(self) -> Union[schemas.Dataset,None]:
        return self.__api_client.dataset().describe(self.name).files if self.dataset_id else None
        
    def list(self) -> List[Dict]:
        return self.describe().files if self.dataset_id else []
        
    def create(self) -> str:
        if not self.dataset_id:
            self.dataset_id = self.__api_client.dataset().create( self.name, self.description, allow_users=self.allow_users)
        return self.dataset_id
        
    def upload(self, files : Union[List[str], str] ) -> bool:
        return self.__api_client.dataset().upload( self.name, files ) if self.dataset_id else False
                   
    def download( self,  targetfolder : str=None ) -> bool:
        return self.__api_client.dataset().download(self.name, targetfolder) if self.dataset_id else False
        