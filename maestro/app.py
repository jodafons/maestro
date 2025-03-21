#!/usr/bin/env python

import os
import time
import uvicorn
import argparse
import traceback

from time              import sleep
from loguru            import logger
from fastapi           import FastAPI
from fastapi.responses import PlainTextResponse
from maestro           import setup_logs
from maestro           import get_manager_service, create_user
from maestro           import get_scheduler_service
from maestro           import get_backend_service
from maestro           import routes
from maestro.db        import get_db_service, recreate_db
from maestro.io        import get_io_service


def run( args ):
    
    #
    # startup
    #
    setup_logs(args.name,args.message_level)
    
    #
    # get volume
    #
    get_io_service(args.volume)


    #
    # get database
    #
    db_booted = False
    while not db_booted:
        try:
            if args.db_string=="":
                db_string = f"sqlite:///{args.volume}/db/database.db"
                os.makedirs(f"{args.volume}/db", exist_ok=True)
            else:
                db_string = args.db_string    
            logger.info(f"db_string: {db_string}")       
            get_db_service(db_string)
            db_booted = True
        except:
            time.sleep(2)
            traceback.print_exc()
            logger.warning("waiting for the database...")

    if args.db_recreate:
        logger.info("recreating database...")
        recreate_db()


 
    #
    # create app
    #
    app = FastAPI(title=__name__)
    app.include_router(routes.remote_app)
    app.include_router(routes.user_app)
    app.include_router(routes.dataset_app)
    #app.include_router(routes.task_app)
    app.include_router(routes.image_app)


    @app.on_event("startup")
    def startup_event():
        envs={}
        get_manager_service(envs=envs)
        create_user()
        get_slurm_service(args.slurm_account)
        scheduler_service = get_scheduler_service()
        scheduler_service.start()    


    @app.on_event("shutdown")
    def shutdown_event():
        scheduler_service = get_scheduler_service()
        scheduler_service.stop()
        logger.info("shutdown event...")   
            

    @app.get("/status")
    async def get_status():
        return PlainTextResponse("OK", status_code=200)


    #app_level = "warning"
    app_level = 'info'
    port = int(args.host.split(':')[2])
    uvicorn.run(app, port=port, log_level=app_level, host="0.0.0.0")
                




def args_parser():

    
    common_parser = argparse.ArgumentParser(description = '', add_help = False)

    common_parser.add_argument('--host', action='store', dest='host', required = False, 
                        default="http://localhost:7000",
                        help = "the host url.") 
    
    common_parser.add_argument('-n','--name', action='store', dest='name', required = False, 
                        default="mastro-master",
                        help = "the server name.")
    
    common_parser.add_argument('-l','--message-level', action='store', dest='message_level', required = False, 
                        default="INFO",
                        help = "the message level. default can be passed by ORCH_MESSAGE_LEVEL environ.")
    
    common_parser.add_argument('-v','--volume', action='store', dest='volume', required = True, 
                        help = "the volume used to store everything. ") 
    


    database_parser = argparse.ArgumentParser(description = '', add_help = False)

    database_parser.add_argument('--database-string', action='store', dest='db_string', type=str,
                                 required=False, default="",
                                 help = "the database url used to store all tasks and jobs. default can be passed by DB_STRING environ.")
    
    database_parser.add_argument('--database-recreate', action='store_true', dest='db_recreate', 
                                 required=False , 
                                 help = "recreate the postgres SQL database.")     


    slurm_parser = argparse.ArgumentParser(description = '', add_help = False)
    
    slurm_parser.add_argument('--slurm-account', action='store', dest='slurm_account', type=str,
                                required=False, default=os.environ.get("SLURM_ACCOUNT",""),
                                help = "the slurm account used to submit jobs. default can be passed by SLURM_ACCOUNT environ.")
    
    return [common_parser, database_parser, slurm_parser]