

__all__ = ['Pilot']

from database.models import Device
from consumer import Consumer
import socket
from . import Clock, SECONDS

class Pilot:

  #
  # Constructor
  #
  def __init__(self, db, schedule, master=True):


    # create consumers
    hostname = socket.gethostname()
    devices = db.session().query(Device).filter(Device.host==hostname).all()
    self.consumers = [Consumer(device) for device in devices]
    self.schedule = schedule
    self.master = master
    self.tictac = Clock( 10*SECONDS )


  def run(self):

    while True:
      if self.tictac():
        if self.master:
          self.schedule.run()
        else:
          for consumer in self.consumers:
            jobs_db = self.schedule.jobs()
            while consumer.available() and len(jobs_db) > 0:
              consumer+=jobs_db.pop()
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
        





