
import sys,os, tempfile

from time import time, sleep
from fastapi import FastAPI
from pydantic import BaseModel
from loguru import logger
from consumer import Consumer
from api.database import postgres_client, Base, Task, Job


host = os.environ['DATABASE_SERVER_HOST']



app = FastAPI()


db = postgres_client(host)

schedule = Schedule(db)


device = 0
binds = {
        '/home':'/home', 
        '/mnt/cern_data':'/mnt/cern_data'
        }


executor = Consumer(device, binds)


@app.get("/executor/status")
async def status() -> str:
    return "online"

@app.get("/executor/pulse")
async def pulse() -> int:
    return consume.run()





@app.get("/executor/test") 
async def test() -> str:

    command = """python -c 'import time; time.sleep(10)'"""
    workarea = tempfile.mkdtemp()

    status = executor.start(job_id=0, 
                            taskname="test", 
                            command=command, 
                            image="/mnt/cern_data/images/python_3.10.sif", 
                            workarea=workarea)
    
    logger.info("Job test is intothe consumer")
    while executor.job(0) is not None:
        logger.info("Pulse the consumer...")
        executor.run()
        sleep(2)
    logger.info("Job test is not into the consumer anymore")

    return 'test'



@app.post("/executor/start") 
async def start(job: JobRequest) -> str:
    return "ok"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
