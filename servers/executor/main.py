
import sys, os, tempfile, socket, traceback

from time import time, sleep
from fastapi import FastAPI
from pydantic import BaseModel
from loguru import logger


try:
    from consumer import Consumer
    from api.client_pilot import client_pilot
    from api.client_postgres import client_postgres
except:
    traceback.print_exc()
    from servers.executor.consumer import Consumer
    from maestro.api.client_pilot import client_pilot
    from maestro.api.client_postgres import client_postgres



class Resume(BaseModel):
    size      : int
    allocated : int
    full      : bool

class Job(BaseModel):
    job_id    : int


database_host = os.environ["DATABASE_SERVER_HOST"]
pilot_host    = os.environ["PILOT_SERVER_HOST"]
me            = os.environ["EXECUTOR_SERVER_HOST"]

device        = int(os.environ.get("EXECUTOR_SERVER_DEVICE"   ,'-1'))
max_retry     = int(os.environ.get("EXECUTOR_SERVER_MAX_RETRY", '5'))
timeout       = int(os.environ.get("EXECUTOR_SERVER_TIMEOUT"  , '5'))
slot_size     = int(os.environ.get("EXECUTOR_SERVER_SLOT_SIZE", '1'))
binds         = eval(os.environ.get("EXECUTOR_SERVER_BINDS"   ,"{}"))


app      = FastAPI()
pilot    = client_pilot(pilot_host)
db       = client_postgres(database_host)

consumer = Consumer(me, pilot=pilot, db=db, device=device, binds=binds, 
                    max_retry=max_retry, timeout=timeout, size=slot_size)

# Start thread with pilot
consumer.start()


@app.get("/executor/is_alive")
async def is_alive() -> bool:
    return True


@app.get("/executor/run")
async def run() -> int:
    return consume.loop()




@app.post("/executor/start") 
async def start(Job: job) -> bool:

    job_db = client_postgres.job(job.job_id)
    return consumer.start_job( job_db.id, 
                               job_db.task.name, 
                               job_db.command, 
                               job_db.image,
                               job_db.workarea, 
                               device=device, 
                               extra_envs=job_db.extra_envs):



@app.get("/executor/test") 
async def test() -> bool:

    command = """python -c 'import time; time.sleep(10)'"""
    workarea = tempfile.mkdtemp()

    status = consumer.start_job(job_id=0, 
                                taskname="test", 
                                command=command, 
                                image="/mnt/cern_data/images/python_3.10.sif", 
                                workarea=workarea)
    
    logger.info("Job test is intothe consumer")
    while consumer.job(0) is not None:
        logger.info("Pulse the consumer...")
        consumer.loop()
        sleep(2)
    logger.info("Job test is not into the consumer anymore")

    return True



@app.get("/executor/resume")
async def resume() -> Resume:
    return Resume(size=consumer.size, allocated=consumer.allocated(), full=consumer.full())



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)
