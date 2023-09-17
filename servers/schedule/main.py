
import sys, os, tempfile

from time import time, sleep
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from loguru import logger
from schedule import Schedule
from models import Base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

try:
    from api.postgres import postgres
except:
    from maestro.api.postgres import postgres



recreate = bool(os.environ.get("SCHEDULE_SERVER_RECREATE"    , ''))



if recreate:
    db = postgres(os.environ["DATABASE_SERVER_HOST"])
    logger.info("test model acivated. Clean up the entire database")
    Base.metadata.drop_all(db.engine())
    Base.metadata.create_all(db.engine())
    logger.info("Database created...")


app      = FastAPI()
schedule = Schedule(level=os.environ.get("SCHEDULE_LOGGER_LEVEL","INFO"))
schedule.start()


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


@app.post("/schedule/get/{k}") 
async def get(k: int):
    return schedule.get(k)



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
