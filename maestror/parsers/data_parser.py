
__all__ = ["data_parser"]

import glob, traceback, os, argparse, re
from loguru import logger
from maestror.models import Base, Database



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

  def __init__(self, args):

      # Create Task
      create_parser   = argparse.ArgumentParser(description = '', add_help = False)
      recreate_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser   = argparse.ArgumentParser(description = '', add_help = False)


      create_parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                                   required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                                   help = "database url")

      recreate_parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                                   required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                                   help = "database url")

      delete_parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                                   required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                                   help = "database url")
                                 

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
    db = Database(args.database_url)
    return create(db)

  def delete(self):
    db = Database(args.database_url)
    return delete(db)
   
  def recreate(self):
    db = Database(args.database_url)
    return recreate(db)
    
  

















