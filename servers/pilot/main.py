
import sys, os, tempfile

from time import time, sleep
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any, List
from loguru import logger
from api.client_postgres import client_postgres
from api.client_mailing import client_mailing
from api.client_schedule import client_schedule


database_host = os.environ['DATABASE_SERVER_HOST']
mailing_host  = os.environ['MAILING_SERVER_HOST']
schedule_host = os.environ['SCHEDULE_SERVER_HOST']


app = FastAPI()


db       = client_postgres(database_host)
mailing  = client_mailing(mailing_host)
schedule = client_schedule(schedule_host)



#logger.info("Trying to connect to schedule service...")
#if schedule.is_alive():
#    logger.info("Schedule service online.")
#else:
#    logger.critical("Its not possible to connect with schedule service. Abort...")





@app.get("/pilot/is_alive")
async def is_alive() -> bool:
    return True


class Host(BaseModel):
    hostname : str
    dnsname  : str
    devices  : List


@app.get("/pilot/register")
async def register( host : Host ) -> bool:

    logger.into("Registering {host.hostname} from {host.dnsname} into the pilot.")
    
    return True



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
