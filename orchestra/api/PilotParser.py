
__all__ = ["PilotParser"]

import traceback, time, os, argparse, socket

from orchestra.server.main import Pilot
from orchestra.server.mailing import Postman
from orchestra.server.schedule import Schedule, compile
from orchestra import ERROR

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



  def run( self, master):
    
    from_email = os.environ["ORCHESTRA_EMAIL_FROM"]
    to_email   = os.environ["ORCHESTRA_EMAIL_TO"]
    password   = os.environ["ORCHESTRA_EMAIL_TOKEN"]
    basepath   = os.environ["ORCHESTRA_BASEPATH"]
    postman    = Postman( from_email, password , to_email, basepath+'/orchestra/server/mailing/templates')


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



