
__all__ = []

import glob, traceback, os, argparse, re
from loguru import logger
from maestro.models import Base
from maestro.api.clients import postgres


def run( server: str, partition: str='cpu', device: int=-1 ):

  try:
    
    envs = copy()




  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False








class executor_parser:

  def __init__(self , host, args=None):

    self.db = postgres(host)
    if args:

      # Create Task
      run_parser   = argparse.ArgumentParser(description = '', add_help = False)
      
      parent    = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('run', parents=[create_run])
      args.add_parser( 'postgres_session', parents=[parent] )




  def parser( self, args ):

    if args.mode == 'executor':
      if args.option == 'run':
        self.run()
      else:
        logger.error("Option not available.")


  def run(self):
    return run(args.server, partition=args.partition, device=args.device)

 
  

















