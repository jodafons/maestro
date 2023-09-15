
import sys, os, tempfile, socket, traceback

from time import time, sleep
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from loguru import logger


try:
    from consumer import Consumer
    from api.clients import ExecutorStatus
except:
    from servers.executor.consumer import Consumer
    from maestro.api.clients import ExecutorStatus



consumer = Consumer(device   = int(os.environ.get("EXECUTOR_SERVER_DEVICE"   ,'-1')), 
                    binds    = eval(os.environ.get("EXECUTOR_SERVER_BINDS"   ,"{}")), 
                    max_retry= int(os.environ.get("EXECUTOR_SERVER_MAX_RETRY", '5')), 
                    timeout  = int(os.environ.get("EXECUTOR_SERVER_TIMEOUT"  , '5')), 
                    slot_size= int(os.environ.get("EXECUTOR_SERVER_SLOT_SIZE", '1')),
                    level    = os.environ.get("EXECUTOR_LOGGER_LEVEL", "INFO"     ))


# Start thread with pilot
consumer.start()

# create the server
app = FastAPI()


@app.get("/executor/ping")
async def ping() -> bool:
    return {"message": "pong"}


@app.post("/executor/start/{job_id}") 
async def start(job_id: int):
    if not consumer.push_back( job_id )
        raise HTTPException(status_code=404, detail=f"Not possible to include {job_id} into the pipe.")
    return {"message", f"Job {job_id} was included into the pipe."}


@app.get("/executor/stop") 
async def stop() -> bool:
    consumer.stop()
    return {"message", "Executor was stopped by external signal."}


@appls.get("/executor/status")
async def status() -> ExecutorStatus:
    return ExecutorStatus(size=consumer.size, allocated=consumer.allocated(), full=consumer.full())



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)
