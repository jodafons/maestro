
import uvicorn, os

from fastapi import FastAPI, HTTPException

try:
    from pilot import Pilot
except:
    from servers.pilot.pilot import Pilot


app   = FastAPI()
pilot = Pilot(level = os.environ.get("PILOT_LOGGER_LEVEL","INFO"))
pilot.start()




@app.get("/pilot/ping")
async def ping():
    return {"message": "pong"}


@app.post("/pilot/append/{hostname}")
async def append( hostname : str ):
    if not pilot.append( hostname )
        raise HTTPException(status_code=404, detail=f"not possible to include executor as {hostname} into the pilot.")
    return {"message", f"executor as {hostname} was included into the pilot."}


@app.get("/pilot/stop") 
async def stop():
    pilot.stop()
    return {"message", "pilot was stopped by external signal."}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
