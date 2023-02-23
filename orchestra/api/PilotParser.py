
__all__ = ["PilotParser"]

import traceback, time, os, argparse, socket
import multiprocessing
import torch

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
      parent = argparse.ArgumentParser(description = '',add_help = False)

      subparser = parent.add_subparsers(dest='option')
      subparser.add_parser('run', parents=[run_parser])
      args.add_parser( 'pilot', parents=[parent] )



  def compile( self, args ):
    if args.mode == 'pilot':
      if args.option == 'run':
        self.run( args.master )
      else:
        print("Not valid option.")



  def run( self, master ):
    
    from_email = os.environ["ORCHESTRA_EMAIL_FROM"]
    to_email   = os.environ["ORCHESTRA_EMAIL_TO"]
    password   = os.environ["ORCHESTRA_EMAIL_TOKEN"]
    basepath   = os.environ["ORCHESTRA_BASEPATH"]
    postman    = Postman( from_email, password , to_email, basepath+'/orchestra/server/mailing/templates')

    hostname = socket.gethostname()
    # remove all device for this host
    self.__db.session().query(Device).filter(Device.host==hostname).delete()

    # device auto-creation
    device_api = DeviceParser(self.__db)



    if torch.cuda.is_available():
      print(INFO+"Cuda is available")
      print(torch.cuda.device_count())
      for gpu in range(torch.cuda.device_count()):
        print (INFO+f"Creating GPU device with ID number {gpu}")
        device_api.create(hostname, device=gpu, slots=10, enabled=1)
    else:

      ncores = multiprocessing.cpu_count()
      print (INFO+f"Creating CPU device with {ncores} slots")
      device_api.create(hostname, device=-1, slots=ncores, enabled=int(0.7 * ncores))
    


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
        postman.send(f"[Cluster LPS] (ALARM) Orchestra ({hostname}) stop",message)
        print(ERROR+message)
        time.sleep(10)



