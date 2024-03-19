#!/usr/bin/env python

import uvicorn, sys, os, signal, time

from fastapi import FastAPI
from maestro import schemas, Consumer, Database, Server
from maestro import get_system_info
from loguru import logger


def setup_logs( server_name , level, save : bool=True):
    """Setup and configure the logger"""

    logger.configure(extra={"server_name" : server_name})
    logger.remove()  # Remove any old handler
    format="<green>{time}</green> | <level>{level:^12}</level> | <cyan>{extra[server_name]:<30}</cyan> | <blue>{message}</blue>"
    logger.add(
        sys.stdout,
        colorize=True,
        backtrace=True,
        diagnose=True,
        level=level,
        format=format,
    )
    if save:
        output_file = server_name.replace(':','_').replace('-','_') + '.log'
        logger.add(output_file, 
                   rotation="30 minutes", 
                   retention=3, 
                   format=format, 
                   level=level, 
                   colorize=False)
        

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


    if device>=0 and not device in [gpu['id'] for gpu in sys_info['gpu']]:
        logger.critical("gpu device not found into the host. abort")
        sys.exit(1)

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
        logger.info("stop event...")
        os.kill(os.getpid(), signal.SIGTERM) # force to move to shutdown event
        return schemas.Answer( host=consumer.host_url, message="runner was stopped by external signal." )


    @app.on_event("shutdown")
    async def shutdown_event():
        logger.info("shutdown event...")
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
        logger.info("getting info...")
        return schemas.Answer( host=consumer.host_url, metadata=consumer.system_info() )


    uvicorn.run(app, host=host, port=args.runner_port, reload=False, log_level="warning")



def boot_discovery(args):

    local_runners = {}
    sys_info      = get_system_info()
    hostname    = sys_info['hostname']
    server_name = f"{hostname}:discovery"
    setup_logs( server_name, args.message_level ,save=False)

    if args.device=='gpu' and len(sys_info['gpu'])>0:
        devices = [gpu['id'] for gpu in sys_info['gpu']]
    elif args.device=='cpu':
        devices = [-1]
    elif args.device=='auto':
        devices = [gpu['id'] for gpu in sys_info['gpu']] if len(sys_info['gpu'])>0 else [-1]
    else: # this is a string with format: 0 or 0,1,2
        devices = [int(d) for d in args.device.split(',')]

    logger.info("we found these devices into this host...")
    local_runners = {}
    if args.max_procs > 0:
        runner_port = args.runner_port
        for device in devices:
            device_name = f'cuda:{device}' if device>=0 else 'cpu'
            logger.info(f'starting runner with device {device} and {args.max_procs} slots...')
            runner = Server(f"maestro run runner --disable-boot-discovery --max-procs {args.max_procs} --device {device} --partition {args.partition} --runner-port {runner_port} --database-url {args.database_url}")
            runner_port+=1
            local_runners[device_name] = runner
            runner.start()
            time.sleep(2)

    while not all([runner.is_alive() in local_runners.values()]):
        time.sleep(10)
        for device_name, runner in local_runners.items():
            if runner.is_alive():
                logger.debug(f"{device_name} still alive...")