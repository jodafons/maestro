

import uvicorn, os
from time import sleep
from fastapi import FastAPI, HTTPException
from maestro.servers.control_plane import Pilot
from maestro import models, schemas




level      = os.environ.get("CONTROL_PLANE_LOGGER_LEVEL","INFO")
from_email = os.environ['POSTMAN_SERVER_EMAIL_FROM']
password   = os.environ['POSTMAN_SERVER_EMAIL_PASSWORD']
database   = os.environ["DATABASE_SERVER_HOST"]
from_email = os.environ['POSTMAN_SERVER_EMAIL_FROM']
password   = os.environ['POSTMAN_SERVER_EMAIL_PASSWORD']
to_email   = os.environ['POSTMAN_SERVER_EMAIL_TO']




app      = FastAPI()
db       = Postgres(database)
schedule = Schedule(db, level=level)
pilot    = Pilot(schedule, level=level)



@app.on_event("shutdown")
async def shutdown_event():
    schedule.stop()
    pilot.stop()


@app.on_event("startup")
async def startup_event():
    schedule.start()
    pilot.start()


@app.get("/pilot/ping")
async def ping():
    return {"message": "pong"}


@app.get("/pilot/stop") 
async def stop():
    schedule.stop()
    pilot.stop()
    return {"message", "pilot was stopped by external signal."}


@app.get("/pilot/describe") 
async def describe() -> schemas.Server : 
    return schemas.Server( 
                           database  = db.host, 
                           binds     = str(pilot.binds),
                           partitions= pilot.partitions ,
                           executors = [executor.describe() for executor in pilot.executors.values() if executor.ping()]
                           )
    

@app.post("/pilot/join")
async def join( executor : schemas.Executor ) -> schemas.Server:
    pilot.join_as( executor.host )
    return schemas.Server( 
                           database  = db.host, 
                           binds     = str(pilot.binds),
                           partitions= pilot.partitions ,
                           executors = [executor.describe() for executor in pilot.executors.values() if executor.ping()]
                           )
    
#
# database manipulation
#


@app.post("/pilot/create") 
async def create( task : schemas.Task ) :

    with db as session:
        #if not verify_token(session , task):
        #    raise HTTPException(status_code=400 , detail="not authenticated properly.")
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

    with db as session:
        #if not verify_token(session , task):
        #    raise HTTPException(status_code=400 , detail="not authenticated properly.")
        task_db = session.get_task(task.id)
        if not task_db:
            raise HTTPException(status_code=418, detail=f"task exist into database.")

        task_db.kill()
        session.commit()
        logger.info(f"Sending kill signal to task {task_db.id}")
        return {"message", f"kill signal sent to task {task_db.id}"}




@app.post("/pilot/retry") 
async def retry( task : schemas.Task ) :

    with db as session:
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

    with db as session:
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
