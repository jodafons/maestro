
__all__ = ["init_parser"]

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





class init_parser:

  def __init__(self, args):

      # Create Task
      parser   = argparse.ArgumentParser(description = '', add_help = False)
  
      parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                          required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                          help = "database url")
           
      parent    = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[parser])
      subparser.add_parser('recreate' , parents=[parser])
      subparser.add_parser('delete', parents=[parser])
      args.add_parser( 'init', parents=[parent] )




  def parser( self, args ):

    if args.mode == 'init':
      if args.option == 'create':
        self.create(args)
      elif args.option == 'recreate':
        self.recreate(args) 
      elif args.option == 'delete':
        self.delete(args)
      else:
        logger.error("Option not available.")


  def create(self,args):
    db = Database(args.database_url)
    return create(db)

  def delete(self,args):
    db = Database(args.database_url)
    return delete(db)
   
  def recreate(self,args):
    db = Database(args.database_url)
    return recreate(db)
    





















