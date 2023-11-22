
import uvicorn, os, socket
from fastapi import FastAPI, HTTPException
from maestro import schemas, Consumer, Database, system_info
from loguru import logger
import mlflow


# node information
sys_info = system_info()


# executor endpoint
port     = int(os.environ.get("EXECUTOR_SERVER_PORT", 5000 ))
host     = sys_info['network']['ip_address']
url      = f"http://{host}:{port}"

# database endpoint
database_url = os.environ["DATABASE_SERVER_URL"]


consumer = Consumer(url, 
                    db            = Database(database_url),
                    device        = int(os.environ.get("EXECUTOR_SERVER_DEVICE"   ,'0')), 
                    binds         = eval(os.environ.get("EXECUTOR_SERVER_BINDS"   ,"{}")), 
                    max_retry     = int(os.environ.get("EXECUTOR_SERVER_MAX_RETRY", '5')), 
                    timeout       = int(os.environ.get("EXECUTOR_SERVER_TIMEOUT"  , '5')), 
                    partition     = os.environ.get("EXECUTOR_PARTITION", "gpu"          ),
                    cpu_limit     = float(os.environ.get("EXECUTOR_CPU_LIMIT", "80")    )
                    )


# create the server
app = FastAPI()


@app.on_event("startup")
async def startup_event():
    consumer.start()


@app.get("/executor/start") 
async def start() -> schemas.Answer:
    consumer.start()
    return schemas.Answer( host=consumer.url, message="executor was started by external signal." )


@app.get("/executor/ping")
async def ping() -> schemas.Answer:
    return schemas.Answer( host=consumer.url, message="pong" )


@app.get("/executor/stop") 
async def stop() -> schemas.Answer:
    consumer.stop()
    return schemas.Answer( host=consumer.url, message="executor was stopped by external signal." )


@app.on_event("shutdown")
async def shutdown_event():
    consumer.stop()


@app.post("/executor/start_job/{job_id}") 
async def start_job(job_id: int) -> schemas.Answer:
    submitted = consumer.start_job( job_id )
    return schemas.Answer( host=consumer.url, message=f"Job {job_id} was included into the pipe.", metadata={'submitted':submitted})


@app.get("/executor/system_info")
async def system_info() -> schemas.Answer:
    return schemas.Answer( host=consumer.url, metadata=consumer.system_info(detailed=True) )



if __name__ == "__main__":
    uvicorn.run("main:app", host=host, port=port, reload=False)
