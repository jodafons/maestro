#
# enviroments for configuration:
#
# POSTMAN_SERVER_EMAIL_FROM
# POSTMAN_SERVER_EMAIL_PASSWORD
# POSTMAN_SERVER_EMAIL_TO
# DATABASE_SERVER_RECREATE
# DATABASE_SERVER_HOST
# PILOT_SERVER_PORT
#

import uvicorn, os, socket, shutil
from time import sleep
from fastapi import FastAPI, HTTPException
from maestro import models, schemas, Database, Schedule, Pilot, Server, system_info
from maestro.models import Base
from loguru import logger

# node information
sys_info = system_info()


# pilot server endpoints
port              = int(os.environ.get("PILOT_SERVER_PORT", 5001 ))
hostname          = sys_info['hostname']
ip_address        = sys_info['network']['ip_address']
url               = f"http://{ip_address}:{port}"

# mlflow server endpoints
tracking_port     = int(os.environ.get("TRACKING_SERVER_PORT", 4000))
tracking_location = os.environ.get("TRACKING_SERVER_PATH", os.getcwd()+'/tracking')
tracking_host     = ip_address
tracking_url      = f"http://{tracking_host}:{tracking_port}"


# database endpoint
database_url     = os.environ["DATABASE_SERVER_URL"]


app      = FastAPI()
db       = Database(database_url)


if os.environ.get("DATABASE_SERVER_RECREATE", '')=='recreate':
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
    session.set_environ( "PILOT_SERVER_URL"    , url          )
    session.set_environ( "TRACKING_SERVER_URL" , tracking_url )
    session.set_environ( "DATABASE_SERVER_URL" , database_url )
    os.environ["TRACKING_SERVER_URL"] = tracking_url

# create MLFlow tracking server by cli 
tracking = Server( f"mlflow ui --port {tracking_port} --backend-store-uri {tracking_location} --host {tracking_host}" )
schedule = Schedule(db)
pilot    = Pilot(url, schedule)



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


if __name__ == "__main__":
    uvicorn.run("main:app", host='0.0.0.0', port=port, reload=False)
