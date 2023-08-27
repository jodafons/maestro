

import glob, traceback, os, argparse, re

from sqlalchemy import and_, or_
from prettytable import PrettyTable
from tqdm import tqdm
from loguru import logger

from maestro.standalone.job import test_job
from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger
from maestro.models import Task, Job
from maestro.api.client_postgres import client_postgres

def convert_string_to_range(s):
     """
       convert 0-2,20 to [0,1,2,20]
     """
     return sum((i if len(i) == 1 else list(range(i[0], i[1]+1))
                for i in ([int(j) for j in i if j] for i in
                re.findall('(\d+),?(?:-(\d+))?', s))), [])

def create( db: client_postgres, basepath: str, taskname: str, inputfile: str,
            image: str, command: str, dry_run: bool=False, do_test=True,
            extension='.json') -> bool:

  if db.task(taskname) is not None:
    logger.critical("The task exist into the database. Abort.")

  if (not '%IN' in command):
    logger.critical("The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
  
  # task volume
  volume = basepath + '/' + taskname
  # create task volume
  if not dry_run:
    os.makedirs(volume, exist_ok=True)

  try:
    task_db = Task( id=db.generate_id(Task),
                    name=taskname,
                    volume=volume,
                    status=TaskStatus.REGISTERED,
                    action=TaskAction.WAITING)
    # check if input file is json
    files = list(glob.iglob(root_dir + '**/**.'+extension , recursive=True))


    offset = db.generate_id(Job)
    for idx, fpath in tqdm( enumerate(files) ,  desc= 'Creating... ', ncols=50):
      
      workarea = volume +'/'+ remove_extension( fpath.split('/')[-1] )
      envs = str({})
      job_db = Job(
                    id=offset+idx,
                    image=image,
                    command=command.replace('%IN',fpath),
                    workarea=workarea,
                    inputfile=fpath,
                    envs=envs,
                    status=JobStatus.REGISTERED
                  )
      task_db.jobs.append(job_db)


    # NOTE: Should we skip test here?
    if not do_test:
      db.session().add(task_db)
      if not dry_run:
        db.commit()
      logger.info( "Succefully created.")
      return True
   

    # NOTE: Test my job localy
    logger.info("Applying local test...")
    if test_job( task_db.jobs[0] ):
      db.session().add(task_db)
      if not dry_run:
        db.commit()  
      logger.info("Succefully created.")
    else:
      logger.error("Local test failed.")
      return False

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error.")
    return False



def kill( db: client_postgres, task_id: int ) -> bool:

  try:
    task = db.session().query(Task).filter(Task.id==task_id).first()
    if not task:
        logger.warning( f"The task with id ({task_id}) does not exist into the data base" )
        return False
    task.kill()
    db.commit()
    logger.info(f"Succefully kill.")
    return True
  except Exception as e:
    traceback.print_exc()
    logger.info("Unknown error.")
    return False



def retry( db: client_postgres, task_id: int ) -> bool:
  try:
    task = db.session().query(Task).filter(Task.id==task_id).first()
    if not task:
      logger.warning(f"The task with id ({task_id}) does not exist into the data base" )
      return False
    
    if task.completed():
      logger.info(f"The task with id ({task.status}) is in COMPLETED TaskStatus. Can not retry." )
      return False
    
    task.retry()
    self.__db.commit()
    logger.info(f"Succefully retry.")
    return True
  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False


def delete( db: client_postgres, task_id: int, force=False , remove=False) -> bool:

  try:
    # Get task by id
    task = db.session().query(Task).filter(Task.id==task_id).first()
    if not task:
      logger.warning(f"The task with id ({task_id}) does not exist into the data base" )
      return False

    # Check possible status before continue
    if force:
      db.session().query(Job).filter(Job.taskid==task_id).delete()
      db.session().query(Task).filter(Task.id==task_id).delete()
      db.commit()
    else:
      task.delete()
      
    logger.info("Succefully deleted.")
    return True
 

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False






class task_parser:

  def __init__(self , host, args=None):

    self.db = client_postgres(host)
    if args:

      # Create Task
      create_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser = argparse.ArgumentParser(description = '', add_help = False)
      retry_parser  = argparse.ArgumentParser(description = '', add_help = False)
      list_parser   = argparse.ArgumentParser(description = '', add_help = False)


      create_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                          help = "The task name to be append into the db.")
      create_parser.add_argument('-i','--inputfile', action='store',
                          dest='inputfile', required = True,
                          help = "The input config file that will be used to configure the job (sort and init).")
      create_parser.add_argument('--image', action='store', dest='image', required=True, default=False,
                          help = "The singularity sif image path.")
      create_parser.add_argument('--exec', action='store', dest='command', required=True,
                          help = "The exec command")
      create_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                          help = "Use this as debugger.")
      create_parser.add_argument('--do_test', action='store_true', dest='do_test', required=False, default=False,
                          help = "Do local test")
      

      delete_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                    help = "All task ids to be deleted", type=str)
      delete_parser.add_argument('--force', action='store_true', dest='force', required=False,
                    help = "Force delete.")

      retry_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                    help = "All task ids to be retried", type=str)

      kill_parser = argparse.ArgumentParser(description = '', add_help = False)
      kill_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                    help = "All task ids to be killed", type=str)


      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('retry' , parents=[retry_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list'  , parents=[list_parser])
      subparser.add_parser('kill'  , parents=[kill_parser])
      args.add_parser( 'task', parents=[parent] )

  

  def parser( self, args ):

    if args.mode == 'task':

      if args.option == 'create':
        self.create(os.getcwd(), args.taskname, args.inputfile,
                    args.image, args.command, dry_run=args.dry_run,
                    do_test=args.do_test, extension=args.extension)

      elif args.option == 'retry':
        self.retry(convert_string_to_range(args.id))
        
      elif args.option == 'delete':
        self.delete(convert_string_to_range(args.id), force=args.force)
        
      elif args.option == 'list':
        self.list()
        
      elif args.option == 'kill':
        self.kill(convert_string_to_range(args.id))
        
      else:
        logger.error("Option not available.")


  def create(self, basepath: str, taskname: str, inputfile: str,
                   image: str, command: str, dry_run: bool=False, do_test=True,
                   extension='.json' ):
    return create(self.db, basepath, taskname, inputfile, image, command, dry_run, do_test, extension)

  def kill(self, task_ids):
    for task_id in task_ids:
      kill(self.db, task_id)

  def delete(self, task_ids, force=False):
    for task_id in task_ids:
      delete(self.db, task_id, force=force)

  def retry(self, task_ids):
    for task_id in task_ids:
      retry(self.db, task_id)

  def list(self):
    print(self.db.resume())


  

















