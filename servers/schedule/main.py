
import sys, os, tempfile

from time import time, sleep
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from loguru import logger
from schedule import Schedule
from models import Base

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session



recreate = bool(os.environ.get("SCHEDULE_SERVER_RECREATE"    , ''))



if recreate:

    engine = create_engine(os.environ["DATABASE_SERVER_HOST"])
    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    logger.info("test model acivated. Clean up the entire database")
    session = session()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    session.commit()
    session.close()
    logger.info("Database created...")


app      = FastAPI()
schedule = Schedule()
schedule.start()


@app.get("/sedule/ping")
async def ping() -> bool:
    return True


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
