__all__ = ["task_app"]

import io
import traceback
import pickle

from loguru import logger
from fastapi import APIRouter, HTTPException, HTTPException, Form, Request
from fastapi.responses import StreamingResponse, RedirectResponse


from qio import get_db_service, schemas, get_manager_service
from qio.routes import remote_app, raise_authentication_failure, raise_http_exception


task_app     = APIRouter()



@task_app.put("/task/{option}" , status_code=200, tags=['task'])
async def task_options( 
    user_id        : str,
    option         : str,
    params_str     : str=Form(),
):
    manager = get_manager_service()
    params  = schemas.json_decode(params_str)

    if option=="create":
        tasks    = [schemas.TaskInputs(**task) for task in params["tasks"]]
        sc       = manager.task(user_id).create_task_group(tasks)
        
    elif option=="status":
        task_id = params["task_id"]
        sc = manager.task(user_id).status(task_id)
        
    elif option=="describe":
        task_id = params["task_id"]
        sc = manager.task(user_id).describe(task_id)

    elif option=="list":
        match_with = params["match_with"]
        sc = manager.task(user_id).list(match_with)
    
    elif option=="identity":
        name = params["name"]
        sc = manager.task(user_id).identity(name)

    else:
        raise HTTPException(detail=f"option {option} does not exist into the database service.", status_code=409)

    raise_http_exception(sc)
    return sc.result()


@remote_app.put("/remote/task/{option}", status_code=200, tags=['remote'])
async def options( 
    option  : str,
    request : Request
): 
    db_service = get_db_service()    
    raise_authentication_failure(request)
    return RedirectResponse(f"/task/options/{option}")