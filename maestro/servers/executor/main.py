#!/usr/bin/env python

import uvicorn, os, socket, mlflow

from fastapi import FastAPI, HTTPException
from maestro import schemas, Consumer, Database
from maestro import system_info as get_system_info
from loguru import logger


def run( args ):

    # node information
    sys_info = get_system_info()

    # executor endpoint
    host     = sys_info['network']['ip_address']
    host_url = f"http://{host}:{args.executor_port}"


    consumer = Consumer(host_url, 
                        db            = Database(args.database_url),
                        device        = args.device,  
                        partition     = args.partition,
                        max_procs     = args.max_procs,
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


    @app.post("/executor/start_job") 
    async def start_job(req : schemas.Request) -> schemas.Answer:
        jobs = req['metadata']['jobs']
        print(jobs)
        submitted = consumer.start_job( jobs )
        return schemas.Answer( host=consumer.url, message=f"jobs was included into the pipe.", metadata={'submitted':submitted})


    @app.get("/executor/system_info")
    async def system_info() -> schemas.Answer:
        return schemas.Answer( host=consumer.url, metadata=consumer.system_info(detailed=True) )


    uvicorn.run(app, host=host, port=args.executor_port, reload=False)



         