
__all__ = ["PilotParser"]

import traceback, time, os, argparse, socket

from orchestra.server.main import Pilot
from orchestra.server.mailing import Postman
from orchestra.server.schedule import Schedule, compile
from orchestra.database import Device
from orchestra.api import DeviceParser
from orchestra import ERROR, INFO

class PilotParser:

  def __init__(self, db, args=None):

    self.__db = db
    if args:

      run_parser = argparse.ArgumentParser(description = 'Run pilot command lines.' , add_help = False)
      run_parser.add_argument('-m','--master', action='store_true',
               dest='master', required = False ,
               help = "This is a master branch. One hostname must be a master.")
      run_parser.add_argument('--gpus', action='store',
                     dest='gpus', required = False , default=0, type=int,
                     help = "The number of GPUs available for this host.")
      run_parser.add_argument('--cpus', action='store',
                     dest='cpus', required = False , default=1, type=int,
                     help = "The number of CPU slots available for this host.")
      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')
      subparser.add_parser('run', parents=[run_parser])
      args.add_parser( 'pilot', parents=[parent] )



  def compile( self, args ):
    if args.mode == 'pilot':
      if args.option == 'run':
        self.run( args.master, args.gpus, args.cpus)
      else:
        print("Not valid option.")



  def run( self, master, gpus, cpus, max_slots=10):
    
    from_email = os.environ["ORCHESTRA_EMAIL_FROM"]
    to_email   = os.environ["ORCHESTRA_EMAIL_TO"]
    password   = os.environ["ORCHESTRA_EMAIL_TOKEN"]
    basepath   = os.environ["ORCHESTRA_BASEPATH"]
    postman    = Postman( from_email, password , to_email, basepath+'/orchestra/server/mailing/templates')


    # remove all device for this host
    self.__db.session().query(Device).filter(Device.host==socket.gethostname()).delete()

    # device auto-creation
    device_api = DeviceParser(self.__db)
    for gpu in range(gpus):
      print (INFO+f"Creating GPU device with ID number {gpu}")
      device_api.create(socket.gethostname(), device=gpu, slots=max_slots, enabled=1)
    device_api.create(socket.gethostname(), device=-1, slots=max_slots, enabled=cpus)
    


    while True:
      try:
        #self.__db.reconnect()
        # create the postman
        schedule = Schedule(self.__db, postman)
        compile(schedule)
        # create the pilot
        pilot = Pilot(self.__db, schedule, master=master )
        pilot.run()
        
      except Exception as e:
        traceback.print_exc()
        message=traceback.format_exc()
        postman.send("[Cluster LPS] (ALARM) Orchestra stop",message)
        print(ERROR+message)
        time.sleep(10)



