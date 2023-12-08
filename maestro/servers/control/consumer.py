
__all__ = []

import traceback, threading
from time import time, sleep
from maestro.enumerations import JobStatus
from maestro import Database, schemas, models

from loguru import logger



#
# A collection of slots
#
class Consumer(threading.Thread):

  def __init__(self, host_url            : str,
                     db                  : Database,
                     max_retry           : int=5,
                     ):
            
    threading.Thread.__init__(self)
    self.host_url  = host_url
    self.__stop    = threading.Event()
    self.db        = models.Database(db.host) 
    self.client    = schemas.client(self.host_url, "executor")
    self.max_retry = max_retry
    

  def configure(self):
    
    if self.client.ping():
      answer = self.client.try_request("system_info" , method="get")
      consumer = answer.metadata['consumer']
      self.partition = consumer['partition']
      self.device = consumer['device']
    else:
      logger.error(f"failed to retrieve the node configuration from {self.host_url}")
      return False


  def stop(self):
    logger.info("stopping service...")
    self.__stop.set()


  def run(self):
    while not self.__stop.isSet():
      sleep(0.5)
      self.loop()
     

  def loop(self):
      
    try:
      start = time()
      with self.db as session:

        if self.client.ping():
          self.retry = 0

          answer = self.client.try_request("system_info" , method="get")
          if answer.status:
            consumer = answer.metadata['consumer']
            n = consumer['max_procs'] - consumer['allocated']
            if n > 0:
              jobs = (session.query(models.Job).filter(models.Job.status==JobStatus.ASSIGNED)\
                                             .filter(models.Job.partition==self.partition)\
                                             .filter(models.Job.consumer==self.host_url)\
                                             .order_by(models.Job.id).limit(n).all()
              )
              logger.debug(f"getting {n} jobs from {self.partition} partition...")
              body = schemas.Request( host=self.host, metadata={"jobs":jobs} ) 
              if len(jobs)>0:
                if self.client.try_request(f'start_job', method='post', body=body.json()).status:
                  logger.debug(f'start job sent well to the consumer node.')
        else:
          self.retry += 1

      end = time()
      logger.info(f"server consumer toke {end-start} seconds...")

    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      self.retry+=1


    if self.retry > self.max_retry:
      self.stop()