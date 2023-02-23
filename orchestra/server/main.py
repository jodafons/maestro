

__all__ = ['Pilot']

from orchestra.database.models import Device
from orchestra.server.consumer import Consumer
from orchestra.server import Clock
from orchestra import INFO
import socket

SECONDS = 1


class Pilot:

  #
  # Constructor
  #
  def __init__(self, db, schedule, master=True):


    # create consumers
    hostname = socket.gethostname()
    if not master:
      print(INFO+"Pilot is running in slave mode...")
    else:
      print(INFO+"Pilot is running in master mode...")
    print(INFO+"Loading device from database...")
    devices = db.session().query(Device).filter(Device.host==hostname).all()
    self.consumers = [Consumer(device, db) for device in devices]
    self.schedule = schedule
    self.master = master
    self.tictac = Clock( 10*SECONDS )
    self.db = db


  def run(self):

    while True:
      if self.tictac():
        #print(INFO+'Run pilot...')
        if self.master:
          #print(INFO+'Scheduluing all jobs...')
          self.schedule.run()
        for consumer in self.consumers:
          n = consumer.size() - consumer.allocated()
          jobs_db = self.schedule.jobs(n)
          while consumer.available() and len(jobs_db) > 0:
            consumer.push_back(jobs_db.pop())
          consumer.run()
        
        self.tictac.reset()



if __name__ == '__main__':

    from .schedule import Schedule, compile
    from .database.client import client_postgres
    from .mailing.Postman import Postman
    import os



    postman = Postman( os.environ['ORCHESTRA_POSTMAIL_FROM'], 
                       os.environ['ORCHESTRA_POSTMAIL_TO'],
                       os.environ['ORCHESTRA_POSTMAIL_TOKEN'],
                       orch_path+'/mailing/templates')

    



    db = client_postgres( os.environ["ORCHESTRA_POSTGRES_HOST"] )

       

    schedule = Schedule(db, postman)
    compile(schedule)

    pilot = Pilot(db, schedule, master=True )
    pilot.run()
        





