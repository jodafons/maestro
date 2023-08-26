
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker
from models import Job
from loguru import logger
import traceback



class client_postgres:
  
  def __init__(self, host):

    self.is_alive = False
    try:
      logger.info(f"Setup database to {host}")
      # prepare database
      engine = create_engine(host)
      Session = sessionmaker(bind=engine)
      self.session = Session()
      self.is_alive = True
    except Exception as e:
      traceback.print_exc()
      logger.error("It is not possible to connect to the database.")

  def __del__(self):
    self.commit()
    self.session.close()


  def retrieve(self, job_id):
    try:
      return self.session.query(Job).filter( Job.id==job_id ).with_for_update().all()
    except Exception as e:
      logger.error(f"Not be able to get running from database. Return an empty list to the user.")
      traceback.print_exc()
      return None


  def commit(self):
    self.session.commit()

  