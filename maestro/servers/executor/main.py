
import uvicorn, os, socket

from fastapi import FastAPI, HTTPException
from maestro.servers.executor.consumer import Consumer
from maestro import schemas



port        = int(os.environ.get("EXECUTOR_SERVER_PORT", 6000 ))
hostname    = os.environ.get("EXECUTOR_SERVER_HOSTNAME" ,  f"http://{socket.getfqdn()}")
host        = f"{hostname}:{port}"


consumer = Consumer(host, 
                    db            = Database(os.environ["DATABASE_SERVER_HOST"]),
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


@app.post("/executor/start_job/{job_id}") 
async def start_job(job_id: int):
    if not consumer.start_job( job_id ):
        raise HTTPException(status_code=404, detail=f"Not possible to include {job_id} into the pipe.")
    return {"message", f"Job {job_id} was included into the pipe."}




#@app.get("/executor/system_info")
#async def system_info() -> schemas.Executor:
#    return schemas.Executor( **consumer.system_info() )





if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
