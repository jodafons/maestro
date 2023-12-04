

import uvicorn, os, socket, shutil
from time import sleep
from fastapi import FastAPI, HTTPException
from maestro import models, schemas, Database, Schedule, Pilot, Server, Postman
from maestro import system_info as get_system_info
from maestro.models import Base
from loguru import logger


def run( args , launch_executor : bool=False ):

    # node information
    sys_info = get_system_info()

    # pilot server endpoints
    hostname  = sys_info['hostname']
    host      = sys_info['network']['ip_address']
    pilot_url = f"http://{host}:{args.pilot_port}"

    # mlflow server endpoints
    tracking_host     = host
    tracking_url      = f"http://{tracking_host}:{args.tracking_port}"


    db = Database(args.database_url)

    if args.database_recreate:
        logger.info("clean up the entire database and recreate it...")
        Base.metadata.drop_all(db.engine())
        Base.metadata.create_all(db.engine())
        logger.info("Database created...")
        if os.path.exists(args.tracking_location):
            logger.info("clean up tracking directory...")
            shutil.rmtree(args.tracking_location)
    else:
        logger.info("set the enviroment with the pilot current location at the network...")


    with db as session:
        # rewrite all environs into the database
        session.set_environ( "PILOT_SERVER_URL"    , pilot_url    )
        session.set_environ( "TRACKING_SERVER_URL" , tracking_url )
        session.set_environ( "DATABASE_SERVER_URL" , args.database_url )


    # services
    postman    = Postman(args.email_from, args.email_password)
    schedule   = Schedule(db, postman)
    pilot      = Pilot(pilot_url, schedule)

    # mlflow tracking server
    tracking   = Server( f"mlflow ui --port {args.tracking_port} --backend-store-uri {args.tracking_location}/mlflow  --artifacts-destination {args.tracking_location}/artifacts" )
    
    if launch_executor:
        executor = Server(f"maestro run executor --max_procs {args.max_procs} --device {args.device} --partition {args.partition} --executor-port {args.executor_port} --database-url {args.database_url}")


    # create master
    app      = FastAPI()

    @app.on_event("shutdown")
    async def shutdown_event():
        tracking.stop()
        if launch_executor:
            logger.info("stopping executor service...")
            executor.stop()
        pilot.stop()


    @app.on_event("startup")
    async def startup_event():
        tracking.start()
        if launch_executor:
            logger.info("starting executor service...")
            executor.start()
        pilot.start()


    @app.get("/pilot/ping")
    async def ping() -> schemas.Answer:
        return schemas.Answer( host=pilot.host, message="pong")


    @app.post("/pilot/join")
    async def join( req : schemas.Request ) -> schemas.Answer:
        pilot.join_as( req.host )
        return schemas.Answer( host = pilot.host, message="joined" )


    @app.get("/pilot/system_info") 
    async def system_info()  -> schemas.Answer:
        return schemas.Answer( host=pilot.host, metadata=pilot.system_info() )
        

    uvicorn.run(app, host=host, port=args.pilot_port, reload=False)


