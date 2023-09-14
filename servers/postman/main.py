
import os, traceback
import uvicorn

from fastapi import FastAPI
from pydantic import BaseModel
from loguru import logger
from postman import Postman
    
try:
    from api.clients import Email
except:
    print(traceback.print_exc())

    from maestro.api.clients import Email



app = FastAPI()


from_email = os.environ['POSTMAN_SERVER_EMAIL_FROM']
password   = os.environ['POSTMAN_SERVER_EMAIL_PASSWORD']
templates  = os.getcwd()+'/templates'
postman    = Postman(from_email, password, templates)




@app.get("/postman/ping")
async def ping() -> bool:
    return True


@app.post("/postman/send")
async def send(email : Email) -> bool:
    try:
        logger.info(f'Sending email to {email.to}')
        postman.send( email.to , email.subject, email.body )
        return True
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      self.broken=True
      return False


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
    
