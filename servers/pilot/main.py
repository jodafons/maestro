
import uvicorn, os

from fastapi import FastAPI, HTTPException

if bool(os.environ.get("DOCKER_IMAGE",False)):
    from pilot import Pilot
    from api.clients import Executor
else:
    from servers.pilot.pilot import Pilot
    from maestro.api.clients import Executor


app   = FastAPI()
pilot = Pilot(level = os.environ.get("PILOT_LOGGER_LEVEL","INFO"))
pilot.start()

@app.get("/pilot/ping")
async def ping():
    return {"message": "pong"}


@app.post("/pilot/join")
async def join( executor : Executor ):
    pilot.join_as( executor.host )
    return {"message": "join"}


@app.get("/pilot/stop") 
async def stop():
    pilot.stop()
    return {"message", "pilot was stopped by external signal."}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
