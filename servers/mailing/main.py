
import os, traceback
import uvicorn

from fastapi import FastAPI
from pydantic import BaseModel
from loguru import logger

from mailing.postman import Postman
    

class EmailRequest(BaseModel):
    to : str
    subject : str
    body : str



app = FastAPI()


from_email = os.environ['MAILING_SERVER_EMAIL_FROM']
password   = os.environ['MAILING_SERVER_EMAIL_PASSWORD']



templates = os.getcwd()+'/mailing/templates'
postman    = Postman(from_email, password, templates)




@app.get("/mailing/is_alive")
async def is_alive() -> bool:
    return True

@app.post("/mailing/send")
async def send(email : EmailRequest) -> bool:
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
    
