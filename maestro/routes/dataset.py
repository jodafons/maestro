__all__ = ["dataset_app"]

import io

from loguru import logger
from typing import Optional
from fastapi import APIRouter, File, UploadFile, Request, Form, HTTPException
from fastapi.responses import RedirectResponse, StreamingResponse 


from maestro.db import get_db_service
from maestro import schemas, get_manager_service
from maestro.routes import remote_app, raise_authentication_failure, raise_http_exception



dataset_app = APIRouter()


@dataset_app.put("/dataset/options/{option}" , status_code=200, tags=['dataset'])
async def options( 
    user_id        : str,
    option         : str,
    params_str     : str=Form(),
):
    manager = get_manager_service()
    params  = schemas.json_decode(params_str)
    name    = params['name'] if 'name' in params else ""

    if option=="create":
        description  = params['description']
        users        = params['allow_users']     
        dataset = schemas.Dataset(name=name, 
                                  user_id=user_id, 
                                  description=description,
                                  users=users)
        sc = manager.dataset(user_id).create( dataset )

    elif option=="describe":
        sc   = manager.dataset(user_id).describe(name)

    elif option=="exist":
        filename = params["filename"] if "filename" in params else None
        sc = manager.dataset(user_id).check_existence(name, filename=filename)

    elif option=="list":
        match_with = params["match_with"]
        sc = manager.dataset(user_id).list(match_with)

    elif option=="identity":
        name = params["name"]
        sc = manager.dataset(user_id).identity(name)

    else:
        raise HTTPException(detail=f"option {option} does not exist into the database service.")

    raise_http_exception(sc)
    return sc.result()



@dataset_app.put("/dataset/download", status_code=200, tags=['dataset']) 
async def download( 
    user_id     : str,
    name        : str = Form(),
    filename    : str = Form(),
) -> StreamingResponse : 
    
    manager = get_manager_service()
    sc = manager.dataset(user_id).download( name, filename )
    raise_http_exception(sc)
    zipfilename = sc.result()
    logger.info(f"reading from {zipfilename}...")
    # NOTE: need to check if this is the better way to transfer a large file
    with open(zipfilename , 'rb') as f:
        stream = io.BytesIO(f.read())
    return StreamingResponse(stream, media_type="application/octet-stream", status_code=200)
  

@dataset_app.put("/dataset/upload/" , status_code=200, tags=['dataset'])
async def upload( 
    user_id                : str,
    name                   : str=Form(),
    filename               : str=Form(),
    expected_file_md5      : str=Form(),
    file                   : Optional[UploadFile]=File(None),
) -> bool:
    manager = get_manager_service()
    sc = manager.dataset().upload( name, filename, expected_file_md5, force_overwrite=False, file=file )
    raise_http_exception(sc)
    return sc.result()


#
# remote
#

@remote_app.put("/remote/dataset/options/{option}", status_code=200, tags=['remote'])
async def options( 
    option  : str,
    request : Request
): 

    db_service = get_db_service()    
    raise_authentication_failure(request)
    return RedirectResponse(f"/dataset/options/{option}")


@remote_app.put("/remote/dataset/upload", status_code=200, tags=['remote'])
async def upload( 
    request : Request
) -> bool: 
    
    raise_authentication_failure(request)
    return RedirectResponse(f"/dataset/upload")


@remote_app.put("/remote/dataset/download", status_code=200, tags=['remote'])
async def download( 
    request : Request
) -> StreamingResponse : 
    
    db_service = get_db_service()
    raise_authentication_failure(request)
    return RedirectResponse(f"/dataset/download")
