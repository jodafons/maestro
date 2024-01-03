
__all__ = ["run_parser"]

import glob, traceback, os, argparse, re
from loguru import logger
from maestro.models import Base, Database
from rich_argparse import RichHelpFormatter
from shutil import which
from time import sleep




class run_parser:

  def __init__(self , args):

    #
    # common
    #
    common_parser = argparse.ArgumentParser(description = '', add_help = False)

    common_parser.add_argument('--device', action='store', dest='device', type=int,
                               required=False, default = -1,
                               help = "gpu device number, if not used, default will be cpu as device.")

    common_parser.add_argument('--partition', action='store', dest='partition', type=str,
                               required=False, default='cpu',
                               help = "the partition name")
                                              
    common_parser.add_argument('--max-procs', action='store', dest='max_procs', type=int,
                               required=False, default=os.cpu_count(),
                               help = "the max number of processors in the partition.")


    #
    # database
    #
    database_parser = argparse.ArgumentParser(description = '', add_help = False)

    database_parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                                 required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                                 help = "the database url used to store all tasks and jobs. default can be passed as environ in DATABASE_SERVER_URL")
    database_parser.add_argument('--database-recreate', action='store_true', dest='database_recreate', 
                                 required=False , 
                                 help = "recreate the postgres SQL database and erase the tracking location")     
            



    #
    # executor
    #
    executor_parser = argparse.ArgumentParser(description = '', add_help = False)

    executor_parser.add_argument('--executor-port', action='store', dest='executor_port', type=int,
                                 required=False , default=6000,
                                 help = "the consumer port number")                           
                                                              

    #
    # pilot
    #
    pilot_parser = argparse.ArgumentParser(description = '', add_help = False)


    pilot_parser.add_argument('--pilot-port', action='store', dest='pilot_port', type=int,
                                 required=False , default=5000,
                                 help = "the pilot port number")                           
    pilot_parser.add_argument('--pilot-as-executor', action='store_true', dest='pilot_as_executor',
                                 required=False ,
                                 help = "add an executor into the pilot")                           

    #
    # tracking
    #
    tracking_parser = argparse.ArgumentParser(description = '', add_help = False)


    tracking_parser.add_argument('--tracking-port', action='store', dest='tracking_port', type=int,
                                 required=False , default=4000,
                                 help = "the tracking port number")                           
    
    tracking_parser.add_argument('--tracking-location', action='store', dest='tracking_location', type=str,
                                 required=False , default= os.getcwd()+"/tracking",
                                 help = "the tracking location path into the storage")     

    tracking_parser.add_argument('--tracking-enable', action='store_true', dest='tracking_enable', 
                                 required=False , 
                                 help = "enable the tracking service")     

     
    tracking_parser.add_argument('--email-from', action='store', dest='email_from', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_FROM","") ,
                                 help = "the email server")
                                 
    tracking_parser.add_argument('--email-password', action='store', dest='email_password', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_PASSWORD","") ,
                                 help = "the email server password")
                                 



    slurm_parser = argparse.ArgumentParser(description = '', add_help = False)

    slurm_parser.add_argument('--slurm-reservation', action='store', dest='slurm_reservation', type=str,
                              required=False, default=None,
                              help = "the slurm reservation name.")
                                 
    slurm_parser.add_argument('--slurm-partition', action='store', dest='slurm_partition', type=str,
                              required=False, default=None,
                              help = "the slurm partition name.")
                                 
    slurm_parser.add_argument('--slurm-nodes', action='store', dest='slurm_nodes', type=int,
                              required=False, default=1,
                              help = "the number of nodes to be allocated.")
                                 
    slurm_parser.add_argument('--slurm-name', action='store', dest='slurm_name', type=str,
                              required=False, default='maestro',
                              help = "the slurm task name.")
                                 
    slurm_parser.add_argument('--slurm-account', action='store', dest='slurm_account', type=str,
                              required=False, default=None,#os.getlogin(),
                              help = "the slurm account name.")
           
    slurm_parser.add_argument('--slurm-virtualenv', action='store', dest='slurm_virtualenv', type=str,
                              required=False, default=None,#os.getlogin(),
                              help = "the slurm account name.")
    slurm_parser.add_argument('--slurm-cancel', action='store_true', dest='slurm_cancel',
                              required=False,
                              help = "cancel all tasks.")
                 

    executor_args = [common_parser, executor_parser, database_parser]
    pilot_args    = [common_parser, pilot_parser, tracking_parser, database_parser]
    cluster_args  = [common_parser, pilot_parser, executor_parser, tracking_parser, database_parser, slurm_parser]


    parent    = argparse.ArgumentParser(description = '', add_help = False, formatter_class=RichHelpFormatter)
    
    subparser = parent.add_subparsers(dest='option')
    subparser.add_parser('executor'    , parents=executor_args, formatter_class=RichHelpFormatter, help='run as executor') 
    subparser.add_parser('pilot'       , parents=pilot_args   , formatter_class=RichHelpFormatter, help='run as server')
    subparser.add_parser('slurm'       , parents=cluster_args , formatter_class=RichHelpFormatter, help='run as server and executor into the slurm infrastructure.')

    args.add_parser( 'run', parents=[parent], formatter_class=RichHelpFormatter )


  def parser( self, args ):
    if args.mode == 'run':
      if args.option == 'executor':
        self.executor(args)
      elif args.option == 'pilot':
        self.pilot(args)
      elif args.option == 'slurm':
        self.slurm(args)
      elif args.option == "cancel":
        from maestro.console.parsers import scancel
        scancel()
      else:
        logger.error("Option not available.")


  def executor(self, args):
    from maestro.servers.executor.main import run
    run( args )


  def pilot(self, args):
    from maestro.servers.controler.main import run
    run( args, launch_executor= args.pilot_and_executor )


  def slurm(self, args):
    from maestro.console.parsers import srun, scancel
    if args.slurm_cancel:
      scancel()
    else:
      # cancel all tasks for clear all tasks
      scancel()
      nodes = args.slurm_nodes
      args.option='pilot'
      args.pilot_as_executor = True
      args.slurm_nodes = 1
      srun( args )
      if nodes > 1: # add executor as nodes into the slurm 
        args.slurm_nodes = nodes - 1
        args.pilot_and_executor = False
        args.option='executor'
        srun(args)


    
