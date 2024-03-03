
__all__ = ["run_parser"]

import os, argparse
from loguru import logger

from rich_argparse import RichHelpFormatter
from maestro.clients.slurm import cancel_all_jobs, Slurm

#
# run parser
#
class run_parser:

  def __init__(self , args):

    #
    # common
    #
    common_parser = argparse.ArgumentParser(description = '', add_help = False)

    common_parser.add_argument('--device', action='store', dest='device', type=str,
                               required=False, default = '-1',
                               help = "gpu device number, if not used, default will be cpu as device.")

    common_parser.add_argument('--partition', action='store', dest='partition', type=str,
                               required=False, default='cpu',
                               help = "the partition name")
                                              
    common_parser.add_argument('--max-procs', action='store', dest='max_procs', type=int,
                               required=True, 
                               help = "the max number of processors in the partition.")

    common_parser.add_argument('--message-level', action='store', dest='message_level', type=str,
                               required=False, default='INFO', 
                               help = "the server messagem output level.")

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
    # runner
    #
    runner_parser = argparse.ArgumentParser(description = '', add_help = False)

    runner_parser.add_argument('--runner-port', action='store', dest='runner_port', type=int,
                                 required=False , default=6000,
                                 help = "the consumer port number")                           
    
    # NOTE: disable boot discovery mode to allocate runners given the host devices.                                                              
    runner_parser.add_argument('--disable-boot-discovery', action='store_false', dest='boot_discovery',
                                 required=False , 
                                 help=argparse.SUPPRESS,)
                                                              

    #
    # master
    #
    master_parser = argparse.ArgumentParser(description = '', add_help = False)

    master_parser.add_argument('--master-port', action='store', dest='master_port', type=int,
                                 required=False , default=5000,
                                 help = "the master port number")     
                   

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
     
    tracking_parser.add_argument('--tracking-email-from', action='store', dest='tracking_email_from', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_FROM","") ,
                                 help = "the email server")
                                 
    tracking_parser.add_argument('--tracking-email-password', action='store', dest='tracking_email_password', type=str,
                                 required=False, default =  os.environ.get("POSTMAN_SERVER_EMAIL_PASSWORD","") ,
                                 help = "the email server password")
                                 



    slurm_parser = argparse.ArgumentParser(description = '', add_help = False)

    slurm_parser.add_argument('--slurm-reservation', action='store', dest='slurm_reservation', type=str,
                              required=False, default=None,
                              help = "the slurm reservation name.")
                                 
    slurm_parser.add_argument('--slurm-partition', action='store', dest='slurm_partition', type=str,
                              required=True, default=None,
                              help = "the slurm partition name.")
                                 
    slurm_parser.add_argument('--slurm-nodes', action='store', dest='slurm_nodes', type=int,
                              required=False, default=1,
                              help = "the number of nodes to be allocated.")
                                 
    slurm_parser.add_argument('--slurm-jobname', action='store', dest='slurm_jobname', type=str,
                              required=False, default='maestro',
                              help = "the slurm job name.")
                                 
    slurm_parser.add_argument('--slurm-account', action='store', dest='slurm_account', type=str,
                              required=True,
                              help = "the slurm account name.")
           
    slurm_parser.add_argument('--slurm-virtualenv', action='store', dest='slurm_virtualenv', type=str,
                              required=True,
                              help = "the slurm account name.")

    slurm_parser.add_argument('--slurm-cancel', action='store_true', dest='slurm_cancel',
                              required=False,
                              help = "cancel all tasks.")
    
    slurm_parser.add_argument('--slurm-dry-run', action='store_true', dest='slurm_dry_run',
                              required=False,
                              help = "dry run slurm commands.")
                     

    runner_args   = [common_parser, runner_parser, database_parser]
    master_args   = [common_parser, master_parser, runner_parser, tracking_parser, database_parser]
    cluster_args  = [common_parser, master_parser, runner_parser, tracking_parser, database_parser, slurm_parser]


    parent    = argparse.ArgumentParser(description = '', add_help = False, formatter_class=RichHelpFormatter)
    
    subparser = parent.add_subparsers(dest='option')
    subparser.add_parser('runner'    , parents=runner_args  , formatter_class=RichHelpFormatter, help='run as runner') 
    subparser.add_parser('master'    , parents=master_args  , formatter_class=RichHelpFormatter, help='run as server')
    subparser.add_parser('slurm'     , parents=cluster_args , formatter_class=RichHelpFormatter, help='run as server and runner into the slurm infrastructure.')

    args.add_parser( 'run', parents=[parent], formatter_class=RichHelpFormatter )


  def parser( self, args ):
    if args.mode == 'run':
      if args.option == 'runner':
        self.runner(args)
      elif args.option == 'master':
        self.master(args)
      elif args.option == 'slurm':
        self.slurm(args)
      else:
        logger.error("Option not available.")


  def runner(self, args):
    from maestro.servers.runner.main import run, boot_discovery
    if args.boot_discovery:
      boot_discovery(args)
    else:
      run( args )


  def master(self, args):
    from maestro.servers.controler.main import run
    run( args )


  def slurm( self, args ):

    if args.slurm_cancel:
      cancel_all_jobs(args.slurm_account, args.slurm_jobname)
    else:
      cancel_all_jobs(args.slurm_account, args.slurm_jobname)
      args.partition = args.slurm_partition
      launcher = Slurm( reservation=args.slurm_reservation, 
                        account=args.slurm_account,
                        jobname=args.slurm_jobname,
                        partition=args.slurm_partition,
                        virtualenv=args.slurm_virtualenv )

      launcher.jobname = args.slurm_jobname + '-master'
      launcher.run( args, master=True, dry_run=args.slurm_dry_run )

      for _ in range(args.slurm_nodes-1):
        launcher.jobname = args.slurm_jobname + '-runner'
        launcher.run(args, dry_run=args.slurm_dry_run)
