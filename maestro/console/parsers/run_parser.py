
__all__ = ["run_parser"]

import glob, traceback, os, argparse, re
from loguru import logger
from maestro.models import Base, Database


class run_parser:

  def __init__(self , args):

    database_parser = argparse.ArgumentParser(description = '', add_help = False)

    database_parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                                 required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                                 help = "the database url used to store all tasks and jobs. default can be passed as environ in DATABASE_SERVER_URL")
        

    executor_parser = argparse.ArgumentParser(description = '', add_help = False)

    executor_parser.add_argument('--device', action='store', dest='device', type=int,
                                 required=False, default = -1,
                                 help = "gpu device number, if not used, default will be cpu as device.")

    executor_parser.add_argument('--binds', action='store', dest='binds', type=str,
                                 required=False, default = os.environ.get("EXECUTOR_SERVER_BINDS"   ,"{}"),
                                 help = "necessary binds to append into the singularity --binds param. e.g., '{'/mnt/cern_data':'path/to/cern/storage'}'. Default can be passed as environ in EXECUTOR_SERVER_BINDS")

    executor_parser.add_argument('--executor-port', action='store', dest='executor_port', type=int,
                                 required=False , default=6000,
                                 help = "the consumer port number")                           
                                                              
    executor_parser.add_argument('--partition', action='store', dest='partition', type=str,
                                 required=False, default='cpu',
                                 help = "the partition name")
                                              
    executor_parser.add_argument('--max_procs', action='store', dest='max_procs', type=int,
                                 required=False, default=os.cpu_count(),
                                 help = "the max number of processors in the partition.")
                                              

    pilot_parser = argparse.ArgumentParser(description = '', add_help = False)


    pilot_parser.add_argument('--pilot-port', action='store', dest='pilot_port', type=int,
                                 required=False , default=5000,
                                 help = "the pilot port number")                           

    pilot_parser.add_argument('--tracking-port', action='store', dest='tracking_port', type=int,
                                 required=False , default=4000,
                                 help = "the tracking port number")                           
    
    pilot_parser.add_argument('--tracking-location', action='store', dest='tracking_location', type=str,
                                 required=False , default= os.getcwd()+"/tracking",
                                 help = "the tracking location path into the storage")     

    pilot_parser.add_argument('--database-recreate', action='store_true', dest='database_recreate', 
                                 required=False , 
                                 help = "recreate the postgres SQL database and erase the tracking location")     
            
    pilot_parser.add_argument('--email-from', action='store', dest='email_from', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_FROM","") ,
                                 help = "the email server")
                                 
    pilot_parser.add_argument('--email-password', action='store', dest='email_password', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_PASSWORD","") ,
                                 help = "the email server password")
                                 

    parent    = argparse.ArgumentParser(description = '', add_help = False)
    subparser = parent.add_subparsers(dest='option')
    subparser.add_parser('executor'    , parents=[executor_parser, database_parser])
    subparser.add_parser('pilot'       , parents=[pilot_parser, database_parser])
    subparser.add_parser('all'         , parents=[pilot_parser, executor_parser, database_parser])

    args.add_parser( 'run', parents=[parent] )


  def parser( self, args ):
    if args.mode == 'run':
      if args.option == 'executor':
        self.executor(args)
      elif args.option == 'pilot':
        self.pilot(args)
      elif args.option == 'all':
        self.all(args)
      else:
        logger.error("Option not available.")


  def executor(self, args):
    from maestro.servers.executor.main import run
    run( args )

  def pilot(self, args):
    from maestro.servers.control.main import run
    run( args )

  def all(self, args):
    from maestro.servers.control.main import run
    run( args, launch_executor = True )




















