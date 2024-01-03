
__all__ = ["run_parser"]

import glob, traceback, os, argparse, re
from loguru import logger
from maestro.models import Base, Database
from rich_argparse import RichHelpFormatter
from shutil import which
from time import sleep

from maestro.console.parsers.slurm_parser import cancel_all_jobs



class Slurm:

  def __init__(self, reservation=None, 
                     account=None, 
                     jobname=None,
                     partition=None,
                     virtualenv=None,
                     filename='srun_entrypoint.sh'):

    self.virtualenv=virtualenv
    self.account=account
    self.reservation=reservation
    self.partition=partition
    self.jobname=jobname
    self.filename=filename



  def run(self,args, envs={}, master=False, dry_run=False):

    command = self.parser(args, master=master)

    with open(self.filename, 'w') as f:

      f.write(f"#!/bin/bash\n")
      f.write(f"#SBATCH --nodes=1\n")
      f.write(f"#SBATCH --ntasks-per-node=1\n")
      f.write(f"#SBATCH --exclusive\n")
      if self.account:
        f.write(f"#SBATCH --account={self.account}\n")
      if self.partition:
        f.write(f"#SBATCH --partition={self.partition}\n")
      if self.reservation:
        f.write(f"#SBATCH --reservation={self.reservation}\n")
      if self.jobname:
        f.write(f"#SBATCH --job-name={self.jobname}\n")
        f.write(f"#SBATCH --output={self.jobname}.job_%j.out\n")

      f.write(f"echo 'Node:' $SLURM_JOB_NODELIST\n")
      f.write(f"export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK\n")
      for key, value in envs.items():
        f.write(f"export {key}='{value}'\n")
      if self.virtualenv:
        f.write(f"source {self.virtualenv}/bin/activate\n")
      f.write(f"{command}\n")
      f.write(f"wait\n")

    print( open(self.filename,'r').read())
    if not dry_run:
      os.system( f'sbatch {self.filename}' )
      sleep(2)
      os.system(f'rm {self.filename}')



  #
  # parser arguments to command
  #
  def parser(self, args, master : bool = False):

    parameters = vars(args)
    command = f'maestro run ' + ('master' if master else 'runner')
    def to_argument(argument):
      return "--"+argument.replace('_','-')
    # command preparation
    for argument, value in parameters.items():
      # loop over all arguments
      if argument in ['mode','option']: # skip these keys
        continue
      if 'slurm' in argument: # skip slurm args by the way
        continue
      if not master: # for runner
        if argument in ['database_recreate']: # skip these args if runner
          continue
        if 'tracking_' in argument: # skip tracking args
          continue
        if 'master_' in argument: # skip master args
          continue
      # if store_true arguments and true append the key into the command
      if type(value)==bool:
        if value:
          command += f' {to_argument(argument)}'
      else:
        command += f" {to_argument(argument)}={value}"

    return command




#
# run parser
#
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
                               required=True, 
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
    # runner
    #
    runner_parser = argparse.ArgumentParser(description = '', add_help = False)

    runner_parser.add_argument('--runner-port', action='store', dest='runner_port', type=int,
                                 required=False , default=6000,
                                 help = "the consumer port number")                           
                                                              

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
    from maestro.servers.runner.main import run
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
      launcher.run( args, master=True )

      for _ in range(args.slurm_nodes-1):
        launcher.jobname = args.slurm_jobname + '-runner'
        launcher.run(args)
