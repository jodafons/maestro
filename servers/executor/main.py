
import sys,os
from time import time, sleep
from fastapi import FastAPI
from pydantic import BaseModel
from loguru import logger
from consumer import Consumer

class Job(BaseModel):
    id : int
    


app = FastAPI()

device = 0
binds = {
        #'/home':'/home', 
        #'/mnt/cern_data':'/mnt/cern_data'
        }


executor = Consumer(device, binds, docker_engine=True)

@app.on_event("startup")
async def startup():
    pass

@app.get("/executor/status")
async def status() -> str:
    return "online"

@app.get("/executor/pulse")
async def pulse() -> int:
    return consume.run()


@app.get("/executor/test") 
async def test() -> str:

    command = """python -c 'import time; time.sleep(10)'"""
    workarea = os.getcwd()
    status = executor.start(job_id=0, 
                            taskname="test", 
                            command=command, 
                            image="python:3.10", 
                            workarea=workarea)
    
    #logger.info("Job test is intothe consumer")
    while executor.job(0) is not None:
        logger.info("Pulse the consumer...")
        executor.run()
        sleep(2)
    #logger.info("Job test is not into the consumer anymore")

    

    return 'test'

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80)
