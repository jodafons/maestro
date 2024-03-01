#!/usr/bin/env python

import uvicorn, sys, logging

from fastapi import FastAPI
from maestro import schemas, Consumer, Database
from maestro import get_system_info
from loguru import logger


def setup_logs( server_name , level):
    """Setup and configure the logger"""

    logger.configure(extra={"server_name" : server_name})
    logger.remove()  # Remove any old handler
    logger.add(
        sys.stdout,
        colorize=True,
        backtrace=True,
        diagnose=True,
        level=level,
        format="<green>{time}</green> | <level>{level:^12}</level> | <cyan>{extra[server_name]:<30}</cyan> | <blue>{message}</blue>",
    )

    output_file = server_name.replace(':','_').replace('-','_') + '.log'
    logger.add(output_file, rotation="30 minutes", retention=3)



def run( args ):

    # node information
    sys_info = get_system_info()

    # runner endpoint
    host     = sys_info['network']['ip_address']
    host_url = f"http://{host}:{args.runner_port}"


    device      = int(args.device)
    device_name = f'cuda:{device}' if device>=0 else 'cpu'
    hostname    = sys_info['hostname']
    server_name = f"{hostname}:{device_name}"


    setup_logs( server_name, args.message_level)

    consumer = Consumer(host_url, 
                        db        = Database(args.database_url),
                        device    = int(args.device),  
                        partition = args.partition,
                        max_procs = args.max_procs,
                        )


    # create the server
    app = FastAPI()

    @app.on_event("startup")
    async def startup_event():
        consumer.start()


    @app.get("/runner/start") 
    async def start() -> schemas.Answer:
        consumer.start()
        return schemas.Answer( host=consumer.host_url, message="runner was started by external signal." )


    @app.get("/runner/ping")
    async def ping() -> schemas.Answer:
        return schemas.Answer( host=consumer.host_url, message="pong" )


    @app.get("/runner/stop") 
    async def stop() -> schemas.Answer:
        consumer.stop()
        return schemas.Answer( host=consumer.host_url, message="runner was stopped by external signal." )


    @app.on_event("shutdown")
    async def shutdown_event():
        consumer.stop()


    @app.post("/runner/start_job") 
    async def start_job( req : schemas.Request ) -> schemas.Answer:
        jobs = req.metadata['jobs']
        if not consumer.blocked():
            for job_id in jobs:
                consumer.start_job(job_id)
        return schemas.Answer( host=consumer.host_url, message=f"jobs was included into the pipe.")


    @app.get("/runner/system_info")
    async def system_info() -> schemas.Answer:
        return schemas.Answer( host=consumer.host_url, metadata=consumer.system_info() )


    uvicorn.run(app, host=host, port=args.runner_port, reload=False, log_level="warning")



         