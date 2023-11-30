

import uvicorn, os, socket, shutil
from time import sleep
from fastapi import FastAPI, HTTPException
from maestror import models, schemas, Database, Schedule, Pilot, Server
from maestror import system_info as get_system_info
from maestror.models import Base
from loguru import logger


def run( 
            database_url        : str,
            database_recreate   : bool = False,
            port                : int = 5000 ,
            # tracking server
            tracking_location   : str = os.getcwd()+"/tracking",
            tracking_port       : int = 4000,
            # postman configuration
            email_from          : str=os.environ.get("POSTMAN_SERVER_EMAIL_FROM"    , ""),
            email_to            : str=os.environ.get("POSTMAN_SERVER_EMAIL_TO"      , ""),
            email_password      : str=os.environ.get("POSTMAN_SERVER_EMAIL_PASSWORD", ""),
        ):


    # node information
    sys_info = get_system_info()

    # pilot server endpoints
    hostname  = sys_info['hostname']
    host      = sys_info['network']['ip_address']
    pilot_url = f"http://{host}:{port}"

    # mlflow server endpoints
    tracking_host     = host
    tracking_url      = f"http://{tracking_host}:{tracking_port}"


    db = Database(database_url)

    if database_recreate:
        logger.info("clean up the entire database and recreate it...")
        Base.metadata.drop_all(db.engine())
        Base.metadata.create_all(db.engine())
        logger.info("Database created...")
        if os.path.exists(tracking_location):
            logger.info("clean up tracking directory...")
            shutil.rmtree(tracking_location)
    else:
        logger.info("set the enviroment with the pilot current location at the network...")


    with db as session:
        # rewrite all environs into the database
        session.set_environ( "PILOT_SERVER_URL"    , pilot_url    )
        session.set_environ( "TRACKING_SERVER_URL" , tracking_url )
        session.set_environ( "DATABASE_SERVER_URL" , database_url )


    # overwrite schedule external configurations
    from maestror.servers.control.schedule import schedule_args
    schedule_args.tracking_url      = tracking_url
    schedule_args.email_to          = email_to
    schedule_args.email_to          = email_to
    schedule_args.email_password    = email_password
    schedule                        = Schedule(db)


    # create MLFlow tracking server by cli 
    tracking = Server( f"mlflow ui --port {tracking_port} --backend-store-uri {tracking_location}/mlflow --host {tracking_host} --artifacts-destination {tracking_location}/artifacts" )
    
    # create master
    pilot    = Pilot(pilot_url, schedule)


    app      = FastAPI()

    @app.on_event("shutdown")
    async def shutdown_event():
        tracking.stop()
        pilot.stop()


    @app.on_event("startup")
    async def startup_event():
        tracking.start()
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
        

    uvicorn.run(app, host=host, port=port, reload=False)




if __name__ == "__main__":
    
    run(os.environ["DATABASE_SERVER_URL"], database_recreate=True)            
