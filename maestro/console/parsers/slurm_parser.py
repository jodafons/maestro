
__all__ = ["slurm_parser"]

import glob, traceback, os, argparse, re, subprocess
from loguru import logger
from maestro.models import Base, Database
from rich_argparse import RichHelpFormatter
from time import sleep




def get_jobs(username, jobname):
    command = "squeue -u %s -n %s -h | awk '{print $1}'" % (username, jobname)
    output = subprocess.check_output(command, shell=True)
    jobs = str(output)[2:-1].split('\\n')[:-1]
    return jobs


def cancel_all_jobs( account, jobname):
  jobs = get_jobs( account, jobname + '-master')
  jobs.extend( get_jobs(account, jobname + '-runner') )
  for job_id in jobs:
    logger.info("cancel job {job_id}...")
    os.system(f"scancel {job_id}")
  os.system("rm *.out")


class slurm_parser:

  def __init__(self, args):

      # Create Task

      cancel_parser = argparse.ArgumentParser(description = '', add_help = False)

      cancel_parser.add_argument('--account', action='store', dest='account', type=str,
                               required=True,
                               help = "the slurm account.")

      cancel_parser.add_argument('--jobname', action='store', dest='jobname', type=str,
                               required=False, default='maestro',
                               help = "the slurm job name.")

           
      parent    = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('cancel', parents=[cancel_parser], formatter_class=RichHelpFormatter)
      subparser.add_parser('list'  , parents=[], formatter_class=RichHelpFormatter)
      args.add_parser( 'slurm', parents=[parent] , formatter_class=RichHelpFormatter)



  def parser( self, args ):

    if args.mode == 'slurm':
      if args.option == 'cancel':
        self.cancel(args)
      elif args.option == 'list':
        self.list(args) 
      else:
        logger.error("Option not available.")


  
  def cancel(self,args):
    cancel_all_jobs(args.account, args.jobname)

  def list(self, args):
    pass























