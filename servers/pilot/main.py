

import uvicorn, os
from time import sleep
from fastapi import FastAPI, HTTPException

if bool(os.environ.get("DOCKER_IMAGE",False)):
  from api.clients import *
  from pilot import Pilot
  import models, schemas
  from enumerations import TaskStatus, JobStatus
else:
  from servers.pilot.pilot import Pilot
  from maestro.api.clients import *
  from maestro import models, schemas
  from maestro.enumerations import TaskStatus, JobStatus

def verify_token(session , request):
    logger.info(f"checking token {request.token}")
    user = session().query(models.User).filter(models.User.token==request.token).first()
    if user:
        logger.info(f"requested made by {user.name}.")
    return True if user else False



app   = FastAPI()
pilot = Pilot(level = os.environ.get("PILOT_LOGGER_LEVEL","INFO"))


@app.on_event("shutdown")
async def shutdown_event():
    pilot.stop()


@app.on_event("startup")
async def startup_event():
    pilot.start()


@app.get("/pilot/ping")
async def ping():
    return {"message": "pong"}


@app.get("/pilot/stop") 
async def stop():
    pilot.stop()
    return {"message", "pilot was stopped by external signal."}


@app.get("/pilot/describe") 
async def describe() -> schemas.Server : 
    return schemas.Server( 
                           database  = pilot.db.host, 
                           binds     = str(pilot.binds),
                           partitions= pilot.partitions ,
                           executors = [executor.describe() for executor in pilot.executors.values() if executor.ping()]
                           )
    

@app.post("/pilot/join")
async def join( executor : schemas.Executor ) -> schemas.Server:
    pilot.join_as( executor.host )
    return schemas.Server( 
                           database  = pilot.db.host, 
                           binds     = str(pilot.binds),
                           partitions= pilot.partitions ,
                           executors = [executor.describe() for executor in pilot.executors.values() if executor.ping()]
                           )
    
#
# database manipulation
#


@app.post("/pilot/create") 
async def create( task : schemas.Task ) :

    with pilot.db as session:
        if not verify_token(session , task):
            raise HTTPException(status_code=400 , detail="not authenticated properly.")
        if session.get_task(task.name):
            raise HTTPException(status_code=418, detail=f"task exist into database.")
        if task.partition not in pilot.partitions:
            raise HTTPException(status_code=418, detail=f"{task.partition} partition not available. partitions should be {pilot.partitions}")
        task_id = session.generate_id( models.Task )
        job_id_begin = session.generate_id( models.Job )
        task_db = models.Task( id = task_id, name = task.name, volume = task.volume )
        for idx, job in enumerate(task.jobs):
            job_db = models.Job( id         = job_id_begin + idx , 
                                 image      = job.image,
                                 command    = job.command,
                                 workarea   = job.workarea,
                                 envs       = job.envs,
                                 binds      = job.binds,
                                 partition  = job.partition,
                                 inputfile  = job.inputfile )
            task_db+=job_db
        session().add(task_db)
        session.commit()
        return {"message", f"task created with id {task_db.id}"}


@app.post("/pilot/kill") 
async def kill( task : schemas.Task ) :


    with pilot.db as session:
        if not verify_token(session , task):
            raise HTTPException(status_code=400 , detail="not authenticated properly.")
        task_db = session.get_task(task.id)
        if not task_db:
            raise HTTPException(status_code=418, detail=f"task exist into database.")

        task_db.kill()
        session.commit()
        logger.info(f"Sending kill signal to task {task_db.id}")
        return {"message", f"kill signal sent to task {task_db.id}"}


@app.post("/pilot/retry") 
async def retry( task : schemas.Task ) :

    with pilot.db as session:
        if not verify_token(session , task):
            raise HTTPException(status_code=400 , detail="not authenticated properly.")
        task_db = session.get_task(task.id)
        if not task_db:
            raise HTTPException(status_code=418, detail=f"task exist into database.")
        if task_db.completed():
            raise HTTPException(status_code=418, detail=f"The task was completed. not possible to retry it." )    
        task_db.retry()
        session.commit()
        logger.info(f"Sending retry signal to task {task_db.id}")
        return {"message", f"retry signal sent to task {task_db.id}"}


@app.post("/pilot/delete") 
async def delete( task : schemas.Task ) :

    with pilot.db as session:
        if not verify_token(session , task):
            raise HTTPException(status_code=400 , detail="not authenticated properly.")
        task_db = session.get_task(task.id)
        if not task_db:
            raise HTTPException(status_code=418, detail=f"task not exist into database.")
  
        task_db.delete()
        session.commit()
        return {"message", f"task deleted into the database"}




if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
