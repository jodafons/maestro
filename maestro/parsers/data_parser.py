
__all__ = ["data_parser"]

import glob, traceback, os, argparse, re
from loguru import logger
from maestro.models import Base, Database



def create( db: Database ) -> bool:

  try:
    Base.metadata.create_all(db.engine())
    logger.info("Succefully created.")
    return True

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False



def delete( db: Database ) -> bool:
  try:
    Base.metadata.drop_all(db.engine())
    logger.info("Succefully deleted.")
    return True

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False


def recreate( db: Database) -> bool:

  if (not delete(db)):
    return False

  if (not create(db)):
    return False

  return True





class data_parser:

  def __init__(self , host, args=None):

    self.db = Database(host)
    if args:

      # Create Task
      create_parser   = argparse.ArgumentParser(description = '', add_help = False)
      recreate_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser   = argparse.ArgumentParser(description = '', add_help = False)

      parent    = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('recreate' , parents=[recreate_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      args.add_parser( 'data', parents=[parent] )




  def parser( self, args ):

    if args.mode == 'data':
      if args.option == 'create':
        self.create()
      elif args.option == 'recreate':
        self.recreate() 
      elif args.option == 'delete':
        self.delete()
      else:
        logger.error("Option not available.")


  def create(self):
    return create(self.db)

  def delete(self):
    return delete(self.db)
   
  def recreate(self):
    return recreate(self.db)
    
  

















