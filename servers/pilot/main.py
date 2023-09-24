

import uvicorn, os
from fastapi import FastAPI, HTTPException

if bool(os.environ.get("DOCKER_IMAGE",False)):
  from api.clients import *
  from pilot import Pilot
  import models, schemas
else:
  from servers.pilot.pilot import Pilot
  from maestro.api.clients import *
  from maestro import models, schemas



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


@app.get("/pilot/task/{name}") 
async def task( name : str) :

    with db as session:
        task_db = session.get_task(name)
        if not task_db:
            raise HTTPException(status_code=404, detail=f"task exist into database.")
        
        jobs = []
        for job_db in task_db.jobs:
            job = schemas.Job(  id          = job_db.id, 
                                name        = job_db.name, 
                                image       = job_db.image, 
                                command     = job_db.command,
                                envs        = job_db.envs, 
                                inputfile   = job_db.inputfile, 
                                workarea    = job_db.workarea,
                                partition   = job_db.partition,
                                status      = job_db.status,
                                )
            jobs.append(job)
        return schemas.Task( name = task_db.name, id = task_db.id, jobs=jobs, volume=task_db.volume )
       

@app.get("/pilot/create") 
async def create( task : schemas.Task ) :

    with pilot.db as session:

        if session.get_task(task.name):
            raise HTTPException(status_code=404, detail=f"task exist into database.")
        if task.partition not in pilot.partitions:
            raise HTTPException(status_code=404, detail=f"{task.partition} partition not available.")
        
        task_id = session.generate_id( models.Task )
        job_id_begin = session.generate_id( models.Job )
        task_db = models.Task( id = task_id, name = task.name, volume = task.volume )
        for idx, job in enumerate(task.jobs):
            job_db = models.Job( id         = job_id_begin + idx , 
                                 name       = job.name,
                                 image      = job.image,
                                 command    = job.command,
                                 workarea   = job.workarea,
                                 envs       = job.envs,
                                 binds      = job.binds,
                                 inputfile  = job.inputfile )
            task_db+=job_db
        session().add(task_db)
        session.commit()
        return {"message", f"task created with id {task_id}"}


@app.get("/pilot/kill") 
async def kill( task_id : int ) :

    with pilot.db as session:
        task_db = session.get_task(task.name)
        if not task_db:
            raise HTTPException(status_code=404, detail=f"task exist into database.")
        task_db.kill()
        session.commit()
        return {"message", f"kill signal sent to task {task_db.id}"}


@app.get("/pilot/retry") 
async def retry( task_id : int ) :

    with pilot.db as session:
        task_db = session.get_task(task.name)
        if not task_db:
            raise HTTPException(status_code=404, detail=f"task exist into database.")
        if task_db.completed():
            raise HTTPException(status_code=404, detail=f"The task was completed. not possible to retry it." )    
        task_db.retry()
        session.commit()
        return {"message", f"retry signal sent to task {task_db.id}"}


@app.get("/pilot/delete") 
async def delete( task_id : int ) :

    with pilot.db as session:

        task_db = session.get_task(task_id)
        if not task_db:
            raise HTTPException(status_code=404, detail=f"task exist into database.")
  
        task_db.delete()
        session.commit()
        while task_db.status != TaskStatus.REMOVED:
          logger.info(f"waiting for schedule... task with status {task_db.status}")
          sleep(2)
        logger.info("task stopped and ready to be removed...")
        session().query(models.Job).filter(models.Job.taskid==task_id).delete()
        session().query(models.Task).filter(models.Task.id==task_id).delete()
        session.commit()
        return {"message", f"task deleted into the database"}




if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
