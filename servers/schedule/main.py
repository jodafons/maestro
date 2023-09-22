
import sys, os, tempfile

from time import time, sleep
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from loguru import logger
from schedule import Schedule

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

if bool(os.environ.get("DOCKER_IMAGE",False)):
    from api.clients import postgres
    from models import Base
else:
    from maestro.api.clients import postgres
    from maestro.models import Base


recreate = bool(os.environ.get("SCHEDULE_SERVER_RECREATE"    , ''))



if recreate:
    db = postgres(os.environ["DATABASE_SERVER_HOST"])
    logger.info("test model acivated. Clean up the entire database")
    Base.metadata.drop_all(db.engine())
    Base.metadata.create_all(db.engine())
    logger.info("Database created...")


app      = FastAPI()
schedule = Schedule(level=os.environ.get("SCHEDULE_LOGGER_LEVEL","INFO"))
#schedule.start()


@app.get("/schedule/ping")
async def ping():
    return {"message": "pong"}


@app.get("/schedule/stop") 
async def stop():
    schedule.stop()
    return {"message", "schedule was stopped by external signal."}


@app.get("/schedule/start") 
async def start():
    schedule.start()
    return {"message", "schedule was started by external signal."}


@app.post("/schedule/get_jobs/{partition}/{n}") 
async def get_jobs(partition : str, n: int):
    return schedule.get_jobs(partition, n)



@app.on_event("shutdown")
async def shutdown_event():
    schedule.stop()

@app.on_event("startup")
async def startup_event():
    schedule.start()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
