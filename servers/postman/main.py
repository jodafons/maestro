
import os, traceback
import uvicorn

from fastapi import FastAPI, HTTPException
from postman import Postman
    
if bool(os.environ.get("DOCKER_IMAGE",False)):
    from api.clients import Email
else:
    from maestro.api.clients import Email



app = FastAPI()


from_email = os.environ['POSTMAN_SERVER_EMAIL_FROM']
password   = os.environ['POSTMAN_SERVER_EMAIL_PASSWORD']
templates  = os.getcwd()+'/templates'
postman    = Postman(from_email, password, templates)



@app.post("/postman/send")
async def send(email : Email):
    try:
        postman.send( email.to , email.subject, email.body )
        return {"message", f"The messager was delivered."}
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      raise HTTPException(status_code=404, detail=f"Not possible to send the email.")


@app.get("/postman/ping")
async def ping():
    return {"message": "pong"}



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
    
