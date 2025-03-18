

__all__ = [
    "remote_app", 
    "raise_authentication_failure", 
    "raise_http_exception"
    ]


import requests

from loguru import logger
from typing import List
from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import JSONResponse

from maestro import schemas
from maestro.db import get_db_service
#from maestro.manager import get_manager_service


remote_app    = APIRouter()


def raise_http_exception(sc):
    if sc.isFailure():
        raise HTTPException(detail=sc.reason(), status_code=504)

def raise_authentication_failure( request : Request , only_role : int=None):
    token = request.headers["token"]
    db_service = get_db_service()
    if not db_service.check_token_existence(token):
        raise HTTPException(detail="Not authorized user. the token is not found", status_code=409)


from . import user
__all__.extend( user.__all__ )
from .user import *

#from . import task
#__all__.extend( task.__all__ )
#from .task import *

from . import dataset
__all__.extend( dataset.__all__ )
from .dataset import *

#from . import image
#__all__.extend( image.__all__ )
#from .image import *

