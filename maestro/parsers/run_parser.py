
__all__ = ["run_parser"]

import glob, traceback, os, argparse, re
from loguru import logger
from maestro.models import Base, Database

# import executor server contructor
from maestro.servers.executor.main import executor


class run_parser:

  def __init__(self , args):

 
    executor_parser = argparse.ArgumentParser(description = '', add_help = False)

    executor_parser.add_argument('--device', action='store', dest='device', type=int,
                                 required=False, default = -1,
                                 help = "gpu device number.")

    executor_parser.add_argument('--binds', action='store', dest='binds', type=str,
                                 required=False, default = os.environ.get("EXECUTOR_SERVER_BINDS"   ,"{}"),
                                 help = "binds")

    executor_parser.add_argument('--port', action='store', dest='port', type=int,
                                 required=False , default=5000,
                                 help = "port number")                           
                                    
    executor_parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                                 required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                                 help = "database url")
                                 
    executor_parser.add_argument('--partition', action='store', dest='partition', type=str,
                                 required=False, default='cpu',
                                 help = "partition name")
                                              



    control_parser = argparse.ArgumentParser(description = '', add_help = False)


    control_parser.add_argument('--port', action='store', dest='port', type=int,
                                 required=False , default=5001,
                                 help = "port number")                           

    control_parser.add_argument('--tracking-port', action='store', dest='tracking_port', type=int,
                                 required=False , default=4000,
                                 help = "tracking port number")                           
    
    control_parser.add_argument('--tracking-location', action='store', dest='tracking_location', type=str,
                                 required=False , default= os.getcwd()+"/tracking",
                                 help = "tracking location path")     

    control_parser.add_argument('--database-recreate', action='store_true', dest='database_recreate', 
                                 required=False , 
                                 help = "recreate the postgres SQL database")     

    control_parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                                 required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                                 help = "database url")
                                 
    control_parser.add_argument('--email-to', action='store', dest='email_to', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_TO","") ,
                                 help = "send email to...")

    control_parser.add_argument('--email-from', action='store', dest='email_from', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_FROM","") ,
                                 help = "send email from...")
                                 
    control_parser.add_argument('--email-password', action='store', dest='email_password', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_PASSWORD","") ,
                                 help = "send email password...")
                                 




    parent    = argparse.ArgumentParser(description = '', add_help = False)
    subparser = parent.add_subparsers(dest='option')
    subparser.add_parser('executor', parents=[executor_parser])
    subparser.add_parser('control' , parents=[control_parser])
    args.add_parser( 'run', parents=[parent] )


  def parser( self, args ):
    if args.mode == 'run':
      if args.option == 'executor':
        self.executor(args)
      if args.option == 'control':
        self.control(args)
      else:
        logger.error("Option not available.")


  def executor(self, args):
    from maestro.servers.executor.main import run

    run( args.database_url, 
         port        = args.port,
         device      = args.device,
         binds       = eval(args.binds), 
         partition   = args.partition,
        )

  def control(self, args):
    from maestro.servers.control.main import run
    run( args.database_url, 
         port               = args.port,
         tracking_port      = args.tracking_port,
         tracking_location  = args.tracking_location, 
         database_recreate  = args.database_recreate,
         email_from         = args.email_from,
         email_to           = args.email_to,
         email_password     = args.email_password,
        )




















