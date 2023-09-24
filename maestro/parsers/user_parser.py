__all__ = ["user_parser"]


import os, argparse
from tqdm import tqdm
from uuid import uuid4
from loguru import logger
from maestro import models
from maestro.api.clients import postgres






class user_parser:

  def __init__(self , host, args=None):

    self.db = postgres(host)
    if args:

      # Create Task
      create_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser = argparse.ArgumentParser(description = '', add_help = False)
      token_parser  = argparse.ArgumentParser(description = '', add_help = False)
      list_parser   = argparse.ArgumentParser(description = '', add_help = False)


      create_parser.add_argument('-n','--name', action='store', dest='name', required=True,
                          help = "the user name.")
      create_parser.add_argument('-e','--email', action='store', dest='email', required = True,
                          help = "the email for contant.")
      create_parser.add_argument('--priority', action='store', dest='priority', required=False, default=1,
                          help = "the priority number", type=int)
      
      token_parser.add_argument('-n', '--name', action='store', dest='name', required=True, default='',
                    help = "the name of the user.", type=str)
     
      delete_parser.add_argument('-n', '--name', action='store', dest='name', required=True, default='',
                    help = "the name of the user.", type=str)
     
      
      list_parser = argparse.ArgumentParser(description = '', add_help = False)
      

      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('token' , parents=[token_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list'  , parents=[list_parser])
      args.add_parser( 'user', parents=[parent] )

  

  def parser( self, args ):

    if args.mode == 'user':

      if args.option == 'create':
        self.create( args.name, args.email, args.priority )

      elif args.option == 'token':
        self.token(args.name)
        
      elif args.option == 'delete':
        self.delete(args.name)
        
      elif args.option == 'list':
        self.list()
        
      else:
        logger.error("Option not available.")


  def create(self, name : str, email : str, priority : int = 1 ):
    with self.db as session:
      if session.get_user( name ):
        logger.error(f"the user {name} exist into the database.")
        return False
      user = models.User( name = name, email = email , priority = priority, token = uuid4())
      session().add(user)
      session.commit()
      logger.info(f"the user {name} new token {user.token}")
      return user.token


  def delete(self, name : str):
    with self.db as session:
      if not session.get_user( name ):
        logger.error(f"the user {name} not found into the database.")
        return False
      session().query(User).filter(User.name==name).delete() 
      session.commit()
      logger.info(f"the user {name} removed from the database.")
      return True



  def token(self, name : str):
    with self.db as session:
      if not session.get_user( name ):
        logger.error(f"the user {name} not found into the database.")
        return False
      user = session.get_user( name )
      user.token = uuid4()
      session.commit()
      logger.info(f"the user {name} new token {user.token}")
      return user.token


  def list(self):
    print('not implemented')
    return True

  

















