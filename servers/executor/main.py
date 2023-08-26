
import sys, os, tempfile, socket

from time import time, sleep
from fastapi import FastAPI
from pydantic import BaseModel
from loguru import logger
from consumer import Consumer
from api.client_pilot import client_pilot
from api.client_postgres import client_postgres


class JobRequest(BaseModel):
    id : int
    command : str
    taskname : str
    image : str
    workarea : str
    
class JobAnswer(BaseModel):
    id : int
    time : float
    status : str



app = FastAPI()

device = -1
binds = {
        '/home'         :'/home', 
        '/mnt/cern_data':'/mnt/cern_data'
        }

database_host = os.environ["DATABASE_SERVER_HOST"]
pilot_host    = os.environ["PILOT_SERVER_HOST"]
me            = os.environ["EXECUTOR_SERVER_HOST"]
device        = int(os.environ["EXECUTOR_SERVER_DEVICE"])
max_retry     = int(os.environ["EXECUTOR_SERVER_MAX_RETRY"])
timeout       = int(os.environ["EXECUTOR_SERVER_TIMEOUT"])


pilot = client_pilot(pilot_host)
db    = client_postgres(database_host)

# Create consumer
consumer = Consumer(me, pilot, db, device=device, binds=binds, max_retry=max_retry, timeout=timeout)

# Start thread
consumer.start()


@app.get("/executor/status")
async def status() -> bool:
    return True


@app.get("/executor/run")
async def run() -> int:
    return consume.loop()


@app.post("/executor/start") 
async def start(job: JobRequest) -> str:
    return "ok"


@app.get("/executor/test") 
async def test() -> bool:

    command = """python -c 'import time; time.sleep(10)'"""
    workarea = tempfile.mkdtemp()

    status = consumer.start_job(job_id=0, 
                                taskname="test", 
                                command=command, 
                                image="/mnt/cern_data/images/python_3.10.sif", 
                                workarea=workarea,
                                dry_run=True)
    
    logger.info("Job test is intothe consumer")
    while consumer.job(0) is not None:
        logger.info("Pulse the consumer...")
        consumer.loop()
        sleep(2)
    logger.info("Job test is not into the consumer anymore")

    return True





if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)
