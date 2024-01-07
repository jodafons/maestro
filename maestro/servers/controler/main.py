

import uvicorn, os, shutil
from fastapi import FastAPI
from maestro import schemas, Database, Pilot, Server, ControlPlane
from maestro import get_system_info
from maestro.models import Base
from loguru import logger


def run( args , launch_runner : bool=False ):

    # node information
    sys_info = get_system_info()

    # pilot server endpoints
    hostname  = sys_info['hostname']
    host      = sys_info['network']['ip_address']
    pilot_url = f"http://{host}:{args.master_port}"

    # mlflow server endpoints
    tracking_host     = host
    tracking_url      = f"http://{tracking_host}:{args.tracking_port}" if args.tracking_enable else ""


    db = Database(args.database_url)

    if args.database_recreate:
        logger.info("clean up the entire database and recreate it...")
        Base.metadata.drop_all(db.engine())
        Base.metadata.create_all(db.engine())
        logger.info("Database created...")
        if args.tracking_enable and os.path.exists(args.tracking_location):
            logger.info("clean up tracking directory...")
            shutil.rmtree(args.tracking_location)
    else:
        logger.info("set the enviroment with the pilot current location at the network...")


    with db as session:
        # rewrite all environs into the database
        session.set_environ( "PILOT_SERVER_URL"       , pilot_url           )
        session.set_environ( "TRACKING_SERVER_URL"    , tracking_url        )
        session.set_environ( "DATABASE_SERVER_URL"    , args.database_url   )
        session.set_environ( "POSTMAN_EMAIL_FROM"     , args.tracking_email_from     )
        session.set_environ( "POSTMAN_EMAIL_PASSWORD" , args.tracking_email_password )


    # services

    control_plane = ControlPlane( db )

    #postman    = Postman(args.email_from, args.email_password)
    pilot      = Pilot(pilot_url, db, control_plane)

    if args.tracking_enable:
        # mlflow tracking server
        tracking   = Server( f"mlflow ui --port {args.tracking_port} --host 0.0.0.0 --backend-store-uri {args.tracking_location}/mlflow  --artifacts-destination {args.tracking_location}/artifacts" )
    else:
        logger.warning("tracking service is disable")

    if args.max_procs > 0:
        logger.info('starting runner...')
        runner = Server(f"maestro run runner --max-procs {args.max_procs} --device {args.device} --partition {args.partition} --runner-port {args.runner_port} --database-url {args.database_url}")


    # create master
    app      = FastAPI()

    @app.on_event("shutdown")
    async def shutdown_event():
        
        if args.tracking_enable:
            tracking.stop()
        if args.max_procs > 1:
            logger.info("stopping runner service...")
            runner.stop()
        pilot.stop()


    @app.on_event("startup")
    async def startup_event():
        if args.tracking_enable:
            tracking.start()
        if args.max_procs > 1:
            logger.info("starting runner service...")
            runner.start()
        pilot.start()


    @app.get("/pilot/ping")
    async def ping() -> schemas.Answer:
        return schemas.Answer( host=pilot.host_url, message="pong")


    @app.post("/pilot/join")
    async def join( req : schemas.Request ) -> schemas.Answer:
        pilot.join_as( req.host )
        return schemas.Answer( host = pilot.host_url, message="joined" )



    uvicorn.run(app, host=host, port=args.master_port, reload=False)


