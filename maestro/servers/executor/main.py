
import uvicorn, os, socket, wandb
from fastapi import FastAPI, HTTPException
from maestro import schemas, Consumer, Database
from loguru import logger


port        = int(os.environ.get("EXECUTOR_SERVER_PORT", 5000 ))
hostname    = os.environ.get("EXECUTOR_SERVER_HOSTNAME" , f"http://{socket.getfqdn()}")
host        = f"{hostname}:{port}"


# wandb login
token = os.environ.get("EXECUTOR_SERVER_WANDB_TOKEN","")
if token!="":
    logger.info("create wandb connection...")
    wandb.login(key=token, relogin=True)


consumer = Consumer(host, 
                    db            = Database(os.environ["DATABASE_SERVER_HOST"]),
                    device        = int(os.environ.get("EXECUTOR_SERVER_DEVICE"   ,'0')), 
                    binds         = eval(os.environ.get("EXECUTOR_SERVER_BINDS"   ,"{}")), 
                    max_retry     = int(os.environ.get("EXECUTOR_SERVER_MAX_RETRY", '5')), 
                    timeout       = int(os.environ.get("EXECUTOR_SERVER_TIMEOUT"  , '5')), 
                    slot_size     = int(os.environ.get("EXECUTOR_SERVER_SLOT_SIZE", '1')),
                    partition     = os.environ.get("EXECUTOR_PARTITION", "gpu"          ),
                    )


# create the server
app = FastAPI()


@app.on_event("startup")
async def startup_event():
    consumer.start()


@app.get("/executor/start") 
async def start() -> schemas.Answer:
    consumer.start()
    return schemas.Answer( host=consumer.host, message="executor was started by external signal." )


@app.get("/executor/ping")
async def ping() -> schemas.Answer:
    return schemas.Answer( host=consumer.host, message="pong" )


@app.get("/executor/stop") 
async def stop() -> schemas.Answer:
    consumer.stop()
    return schemas.Answer( host=consumer.host, message="executor was stopped by external signal." )


@app.on_event("shutdown")
async def shutdown_event():
    consumer.stop()


@app.post("/executor/start_job/{job_id}") 
async def start_job(job_id: int) -> schemas.Answer:
    if not consumer.start_job( job_id ):
        raise HTTPException(status_code=404, detail=f"Not possible to include {job_id} into the pipe.")
    return schemas.Answer( host=consumer.host, message=f"Job {job_id} was included into the pipe." )


@app.get("/executor/system_info")
async def system_info() -> schemas.Answer:
    return schemas.Answer( host=consumer.host, metadata=consumer.system_info(detailed=True) )



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
