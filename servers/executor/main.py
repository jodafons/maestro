
import sys,os, tempfile
import socket

from time import time, sleep
from fastapi import FastAPI
from pydantic import BaseModel
from loguru import logger
from consumer import Consumer
from api.client_pilot import client_pilot

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
pilot_host = os.environ["PILOT_SERVER_HOST"]

# Create consumer
consumer = Consumer(device, binds, host=database_host)

logger.info("Startup executor and trying to connect to the pilot...")
pilot = client_pilot( pilot_host )
if pilot.is_alive():
    logger.info("Schedule connected...")
    hostname = socket.gethostname()
    address = socket.getfqdn()
    logger.info(f"Sync node ({address}) with the pilot...")
    if pilot.register():
        logger.into("Node registered...")
    else:
        logger.critical("Not possible to register the node into the pilot.")

    




@app.get("/executor/status")
async def status() -> bool:
    return True

@app.get("/executor/pulse")
async def pulse() -> int:
    return consume.run()





@app.get("/executor/test") 
async def test() -> str:

    command = """python -c 'import time; time.sleep(10)'"""
    workarea = tempfile.mkdtemp()

    status = consumer.start(job_id=0, 
                            taskname="test", 
                            command=command, 
                            image="/mnt/cern_data/images/python_3.10.sif", 
                            workarea=workarea,
                            dry_run=True)
    
    logger.info("Job test is intothe consumer")
    while consumer.job(0) is not None:
        logger.info("Pulse the consumer...")
        consumer.run()
        sleep(2)
    logger.info("Job test is not into the consumer anymore")

    return 'test'



@app.post("/executor/start") 
async def start(job: JobRequest) -> str:
    return "ok"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)
