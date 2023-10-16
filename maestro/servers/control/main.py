#
# enviroments for configuration:
#
# POSTMAN_SERVER_EMAIL_FROM
# POSTMAN_SERVER_EMAIL_PASSWORD
# POSTMAN_SERVER_EMAIL_TO
# DATABASE_SERVER_RECREATE
# DATABASE_SERVER_HOST
# SCHEDULE_LOGGER_LEVEL
# PILOT_LOGGER_LEVEL
# PILOT_SERVER_PORT
#

import uvicorn, os, socket
from time import sleep
from fastapi import FastAPI, HTTPException
from maestro import models, schemas, Database, Schedule, Pilot
from maestro.models import Base
from loguru import logger



port        = int(os.environ.get("PILOT_SERVER_PORT", 5000 ))
hostname    = os.environ.get("PILOT_SERVER_HOSTNAME", f"http://{socket.getfqdn()}")
host        = f"{hostname}:{port}"


app      = FastAPI()
db       = Database(os.environ["DATABASE_SERVER_HOST"])


if bool(os.environ.get("DATABASE_SERVER_RECREATE"    , '1')):
    logger.info("clean up the entire database and recreate it...")
    Base.metadata.drop_all(db.engine())
    Base.metadata.create_all(db.engine())
    logger.info("Database created...")
else:
    logger.info("set the enviroment with the pilot current location at the network...")


with db as session:
    # rewrite all environs into the database
    session.set_environ( "PILOT_SERVER_HOSTNAME" , hostname)
    session.set_environ( "PILOT_SERVER_PORT" , port)




schedule = Schedule(db, level=os.environ.get("SCHEDULE_LOGGER_LEVEL","INFO"))
pilot    = Pilot(schedule, level=os.environ.get("PILOT_LOGGER_LEVEL","INFO"))



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



    

@app.post("/pilot/join")
async def join( handshake : schemas.HandShake ) -> schemas.HandShake:
    pilot.join_as( handshake.host )
    return schemas.HandShake( host = pilot.localhost metadata = {"binds" : str(pilot.binds)})
    
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
