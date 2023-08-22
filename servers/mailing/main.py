
import os

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
postman    = Postman(from_email, password, '/app/mailing/templates')


@app.get("/mailing/status")
async def status() -> str:
    return "online"

@app.get("/mailing/send")
async def send(email : EmailRequest) -> int:
    postman.send( email.to , email.subject, email.body )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=9002, reload=True)
