
__all__ = []

import glob, traceback, os, argparse, re

from sqlalchemy_utils import database_exists
from loguru import logger
from maestro.models import Base
from maestro.api.clients import database


def create( db: database ) -> bool:

  try:
    #if database_exists( db.engine().url ):
    #  logger.error("The dataabse exists into the server. Its not possible to create a database. Please recreate it or delete than create.")
    #  return False

    Base.metadata.create_all(db.engine())
    db.commit()

    logger.info("Succefully created.")
    return True

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False



def delete( db: database ) -> bool:
  try:
    #if not database_exists( db.engine().url ):
    #  logger.error("The dataabse dont exists into the server. Its not possible to delete something that dont exist into the server...")
    #  return False

    Base.metadata.drop_all(db.engine())
    db.commit()

    logger.info("Succefully deleted.")
    return True

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False


def recreate( db: database) -> bool:

  if (not delete(db)):
    return False

  if (not create(db)):
    return False
  #logger.info("Succefully recreated")
  return True





class database_parser:

  def __init__(self , host, args=None):

    self.db = database(host)
    if args:

      # Create Task
      create_parser   = argparse.ArgumentParser(description = '', add_help = False)
      recreate_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser   = argparse.ArgumentParser(description = '', add_help = False)



      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('recreate' , parents=[recreate_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      args.add_parser( 'database', parents=[parent] )




  def parser( self, args ):

    if args.mode == 'database':
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
    
  

















