
import sys, os, tempfile

from time import time, sleep
from fastapi import FastAPI
from pydantic import BaseModel
from loguru import logger
from schedule import Schedule
from api.client_postgres import client_postgres
from api.client_mailing import client_mailing


database_host = os.environ['DATABASE_SERVER_HOST']
mailing_host  = os.environ['MAILING_SERVER_HOST']

app      = FastAPI()
db       = client_postgres(database_host)
mailing  = client_mailing(mailing_host)
schedule = Schedule(db, mailing)



@app.get("/schedule/is_alive")
async def is_alive() -> bool:
    return True

@app.get("/schedule/run")
async def run() -> bool:
    return schedule.run()




if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
