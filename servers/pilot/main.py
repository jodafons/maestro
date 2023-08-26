
import sys, os, tempfile

from time import time, sleep
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any, List
from loguru import logger
from pilot import Pilot
from api.client_postgres import client_postgres
from api.client_mailing  import client_mailing
from api.client_schedule import client_schedule


database_host = os.environ['DATABASE_SERVER_HOST']
mailing_host  = os.environ['MAILING_SERVER_HOST']
schedule_host = os.environ['SCHEDULE_SERVER_HOST']


app      = FastAPI()
db       = client_postgres(database_host)
mailing  = client_mailing(mailing_host)
schedule = client_schedule(schedule_host)
pilot    = Pilot( db, schedule, mailing )

# Starting the pilot into a thread
pilot.start()



@app.get("/pilot/is_alive")
async def is_alive() -> bool:
    return True



class Host(BaseModel):
    me      : str
    device  : int

@app.post("/pilot/register")
async def register( host : Host ) -> bool:
    logger.info(f"Registering {host.me} with device {host.device} into the pilot.")
    return pilot.register( host.me, host.device )


@app.get("/pilot/reset")
async def reset() -> bool:
    logger.info(f"Registering {host.hostname} from {host.dnsname} into the pilot.")
    return True



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
