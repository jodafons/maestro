
import uvicorn, os, socket

from fastapi import FastAPI, HTTPException


if bool(os.environ.get("DOCKER_IMAGE",False)):
    from consumer import Consumer
    from schemas import *
else:
    from servers.executor.consumer import Consumer
    from maestro.schemas import *



port           = int(os.environ.get("EXECUTOR_SERVER_PORT", 6001000 ))
local_host     = f"http://{socket.getfqdn()}:{str(port)}"
server_host    = os.environ["PILOT_SERVER_HOST"]



consumer = Consumer(local_host, 
                    server_host   = server_host, 
                    device        = int(os.environ.get("EXECUTOR_SERVER_DEVICE"   ,'-1')), 
                    binds         = eval(os.environ.get("EXECUTOR_SERVER_BINDS"   ,"{}")), 
                    max_retry     = int(os.environ.get("EXECUTOR_SERVER_MAX_RETRY", '5')), 
                    timeout       = int(os.environ.get("EXECUTOR_SERVER_TIMEOUT"  , '5')), 
                    slot_size     = int(os.environ.get("EXECUTOR_SERVER_SLOT_SIZE", '0')),
                    level         = os.environ.get("EXECUTOR_LOGGER_LEVEL", "INFO"     ),
                    partition     = os.environ.get("EXECUTOR_PARTITION", "cpu"         ),
                    )


# create the server
app = FastAPI()



@app.on_event("startup")
async def startup_event():
    consumer.start()


@app.get("/executor/start") 
async def start():
    consumer.start()
    return {"message", "Executor was started by external signal."}


@app.get("/executor/ping")
async def ping():
    return {"message": "pong"}


@app.get("/executor/stop") 
async def stop():
    consumer.stop()
    return {"message", "Executor was stopped by external signal."}


@app.on_event("shutdown")
async def shutdown_event():
    consumer.stop()


#
#
#


@app.post("/executor/start_job/{job_id}") 
async def start_job(job_id: int):
    if not consumer.start_job( job_id ):
        raise HTTPException(status_code=404, detail=f"Not possible to include {job_id} into the pipe.")
    return {"message", f"Job {job_id} was included into the pipe."}


@app.get("/executor/describe")
async def describe() -> Executor:
    return Executor(host=consumer.localhost ,
                    size=consumer.size, 
                    allocated=len(consumer), 
                    full=consumer.full(), 
                    partition=consumer.partition,
                    device=consumer.device)


@app.post("/executor/update")
async def update( executor : Executor ) :
    consumer.partition = executor.partition
    consumer.size      = executor.size
    consumer.device    = executor.device
    return {"message", f"consumer was updated."}



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
