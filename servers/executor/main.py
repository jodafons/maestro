
import sys, os, tempfile, socket, traceback

from time import time, sleep
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from loguru import logger


try:
    from consumer import Consumer
    from api.clients import ExecutorStatus
    from models import Job as JobModel
except:
    from servers.executor.consumer import Consumer
    from maestro.api.clients import ExecutorStatus





consumer = Consumer(device   = int(os.environ.get("EXECUTOR_SERVER_DEVICE"   ,'-1')), 
                    binds    = eval(os.environ.get("EXECUTOR_SERVER_BINDS"   ,"{}")), 
                    max_retry= int(os.environ.get("EXECUTOR_SERVER_MAX_RETRY", '5')), 
                    timeout  = int(os.environ.get("EXECUTOR_SERVER_TIMEOUT"  , '5')), 
                    slot_size= int(os.environ.get("EXECUTOR_SERVER_SLOT_SIZE", '1')))


# Start thread with pilot
consumer.start()

# create the server
app = FastAPI()


@app.get("/executor/ping")
async def ping() -> bool:
    return True


@app.post("/executor/start/{job_id}") 
async def start(job_id: int) -> bool:
    return consumer.push_back( job_id )
    

@app.get("/executor/stop") 
async def stop() -> bool:
    return consumer.stop()
    

@appls.get("/executor/status")
async def status() -> ExecutorStatus:
    return ExecutorStatus(size=consumer.size, allocated=consumer.allocated(), full=consumer.full())



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)
