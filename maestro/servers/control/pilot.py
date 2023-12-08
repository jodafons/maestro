
__all__ = ["Pilot"]

import threading
from time import time, sleep
from maestro.servers.control.schedule import Schedule
from maestro.servers.control.consumer import Consumer
from maestro import models
from loguru import logger



class Pilot( threading.Thread ):


  def __init__(self, 
               host               : str, 
               db                 : models.Database,
              ):

    threading.Thread.__init__(self)
    self.host      = host
    self.__stop    = threading.Event()
    self.__lock    = threading.Event()
    self.__lock.set()
    self.db = models.Database(db.host)
    self.tasks = {}
    self.consumers = {}

  def run(self):
    while not self.__stop.isSet():
      sleep(1)
      # NOTE wait to be set
      self.__lock.wait() 
      # NOTE: when set, we will need to wait to register until this loop is read
      self.__lock.clear()
      self.loop()
      # NOTE: allow external user to incluse nodes into the list
      self.__lock.set()


  def loop(self):

    start = time()
    # NOTE: only healthy nodes  

    # create a schedule for each new task
    with self.db as session:
      tasks = session().query(models.Task).all()
      for task in tasks:
        if task.id not in self.tasks.keys():
          logger.info(f"creating a new schedule for task {task.id}")
          schedule = Schedule(task.id, self.db)
          schedule.start()
          self.tasks[task.id] = schedule

        else:
          schedule = self.tasks[task.id]
          if not schedule.is_alive():
            self.tasks.pop(task.id, schedule)

    #
    # TODO: control plane
    #
    for host, consumer in self.consumers.items():
      if not consumer.is_alive():
        logger.info("consumer from host {host} is not alive... removing...")
        self.consumers.pop(host, consumer)

    end = time()
    logger.debug(f"the pilot run loop took {end-start} seconds.")

  

  def stop(self):
    
    logger.info("stopping pilot main loop")
    self.__stop.set()

    logger.info("stopping schedule service...")
    for schedule in self.tasks.values():
      schedule.stop()
    
    logger.info("stopping consumer service...")
    for consumer in self.consumers.values():
      consumer.stop()



  def join_as( self, host ) -> bool:

    if host not in self.consumers.keys():
      logger.info(f"join node {host} into the pilot.")
      consumer = Consumer(host, self.db)
      if consumer.configure():
        self.__lock.wait()
        self.__lock.clear()
        consumer.start()
        self.consumer[host] = consumer
        self.__lock.set()
        return True
          
    return False
    
 