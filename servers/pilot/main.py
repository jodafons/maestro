
import uvicorn, os

from fastapi import FastAPI, HTTPException

if bool(os.environ.get("DOCKER_IMAGE",False)):
    from pilot import Pilot
    from schemas import *
    from models import Job as Job_db
    from models import Task as Task_db
else:
    from servers.pilot.pilot import Pilot
    from maestro.schemas import *
    from maestro.models import Job as Job_db
    from maestro.models import Task as Task_db

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
async def describe() :

    executor_desc = []
    for executor in pilot.executors.values:
        if executor.ping():
            res = executor.describe()
            executor_desc.append(res)
    return Server( host      = pilot.host, 
                   database  = pilot.db.host, 
                   binds     = pilot.binds,
                   partition = pilot.partitions ,
                   executors = executor_desc )


@app.post("/pilot/join")
async def join( executor : Executor ) -> Server:
    pilot.join_as( executor.host )
    return describe()


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
            job = Job(  id          = job_db.id, 
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
        return Task( name = task_db.name, id = task_db.id, jobs=jobs, volume=task_db.volume )
       

@app.get("/pilot/create") 
async def create( task : Task ) :

    with pilot.db as session:

        if session.get_task(task.name):
            raise HTTPException(status_code=404, detail=f"task exist into database.")
        if task.partition not in pilot.partitions:
            raise HTTPException(status_code=404, detail=f"{task.partition} partition not available.")
        
        task_id = session.generate_id( Task_db )
        job_id_begin = session.generate_id( Job_db )
        task_db = Task_db ( id = task_id, name = task.name, volume = task.volume )
        for idx, job in enumerate(task.jobs):
            job_db = Job_db( id         = job_id_begin + idx , 
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
async def delete( task : Task ) :

    with pilot.db as session:
        task_db = session.get_task(task.name)
        if not task_db:
            raise HTTPException(status_code=404, detail=f"task exist into database.")
  
        task_db.delete()
        session.commit()
        while task_db.status != TaskStatus.REMOVED:
          logger.info(f"waiting for schedule... task with status {task_db.status}")
          sleep(2)
        logger.info("task stopped and ready to be removed...")
        session().query(Job).filter(Job.taskid==task_id).delete()
        session().query(Task).filter(Task.id==task_id).delete()
        session.commit()
        return {"message", f"task deleted into the database"}




if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
