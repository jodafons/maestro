#!/usr/bin/env python



import uvicorn, os, socket, mlflow

from fastapi import FastAPI, HTTPException
from maestror import schemas, Consumer, Database
from maestror import system_info as get_system_info
from loguru import logger



def run(
        database_url: str,             
        port        : int   = 6000,
        device      : int   = -1,
        binds       : dict  = eval(os.environ.get("EXECUTOR_SERVER_BINDS", "{}")), 
        partition   : str   = "cpu",
        max_procs   : int   = os.cpu_count(),
        ):


    # node information
    sys_info = get_system_info()

    # executor endpoint
    host     = sys_info['network']['ip_address']
    host_url = f"http://{host}:{port}"


    consumer = Consumer(host_url, 
                        db            = Database(database_url),
                        device        = device,  
                        binds         = binds, 
                        partition     = partition,
                        max_procs     = max_procs,
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

    
    uvicorn.run(app, host=host, port=port, reload=False)



if __name__ == "__main__":

    run(os.environ["DATABASE_SERVER_URL"])            
                