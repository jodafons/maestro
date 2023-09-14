
import sys, os, tempfile

from time import time, sleep
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from typing import Dict, Any, List
from loguru import logger

try:
    from pilot import Pilot
    from api.clients import Executor
except:
    from servers.pilot.pilot import Pilot
    from maestro.api.clients import Executor


app   = FastAPI()
pilot = Pilot()
# Starting the pilot into a thread
pilot.start()



@app.get("/pilot/ping")
async def ping() -> bool:
    return True


@app.post("/pilot/connect")
async def connect( executor : Executor ) -> bool:
    logger.info(f"Connecting {executor.hostname} with device {executor.device} into the pilot.")
    return pilot.connect( executor.hostname, executor.device )






if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
