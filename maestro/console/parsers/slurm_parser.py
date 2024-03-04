__all__ = ["slurm_parser"]

import os, argparse
from loguru import logger
from rich_argparse import RichHelpFormatter
from maestro.clients.slurm import cancel_all_jobs


#
# slurm
#


class slurm_parser:

  def __init__(self, args):

      # Create Task

      cancel_parser = argparse.ArgumentParser(description = '', add_help = False)

      cancel_parser.add_argument('--account', action='store', dest='account', type=str,
                               required=False, default=os.environ["USER"],
                               help = "the slurm account.")

      cancel_parser.add_argument('--jobname', action='store', dest='jobname', type=str,
                               required=False, default='maestro',
                               help = "the slurm job name.")

           
      parent    = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('cancel', parents=[cancel_parser], formatter_class=RichHelpFormatter)
      args.add_parser( 'slurm', parents=[parent] , formatter_class=RichHelpFormatter)



  def parser( self, args ):

    if args.mode == 'slurm':
      if args.option == 'cancel':
        self.cancel(args)
      else:
        logger.error("Option not available.")


  
  def cancel(self,args):
    cancel_all_jobs(args.account, args.jobname)
