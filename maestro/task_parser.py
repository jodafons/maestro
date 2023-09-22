__all__ = []


import glob, traceback, os, argparse, re

from tqdm import tqdm
from loguru import logger

from maestro.standalone.job import test_job
from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger
from maestro.models import Task, Job
from maestro.api.clients import postgres, postgres_session
from maestro.expand_folders import expand_folders

def convert_string_to_range(s):
     """
       convert 0-2,20 to [0,1,2,20]
     """
     return sum((i if len(i) == 1 else list(range(i[0], i[1]+1))
                for i in ([int(j) for j in i if j] for i in
                re.findall(r'(\d+),?(?:-(\d+))?', s))), [])

partitions = os.environ.get("EXECUTOR_AVAILABLE_PARTITIONS","").split(',')

def create( session: postgres_session, basepath: str, taskname: str, inputfile: str,
            image: str, command: str, email: str, dry_run: bool=False, do_test=True,
            extension='.json', binds="{}", partition="cpu") -> bool:




  if session.task(taskname) is not None:
    logger.error("The task exist into the database. Abort.")
    return None

  if (not '%IN' in command):
    logger.error("The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
    return None

  if (not partition in partitions):
    logger.error(f"Partition {partition} not available.")
    return None

  # task volume
  volume = basepath + '/' + taskname
  # create task volume
  if not dry_run:
    os.makedirs(volume, exist_ok=True)

  try:
    task_db = Task( id=session.generate_id(Task),
                    name=taskname,
                    volume=volume,
                    status=TaskStatus.REGISTERED,
                    trigger=TaskTrigger.WAITING,
                    contact=email)
    # check if input file is json
    files = expand_folders( inputfile )

    if len(files) == 0:
      logger.error(f"It is not possible to find jobs into {inputfile}... Please check and try again...")
      return None

    offset = session.generate_id(Job)
    for idx, fpath in tqdm( enumerate(files) ,  desc= 'Creating... ', ncols=50):
      
      extension = fpath.split('/')[-1].split('.')[-1]
      workarea = volume +'/'+ fpath.split('/')[-1].replace('.'+extension, '')
      envs = str({})
      job_db = Job(
                    id=offset+idx,
                    image=image,
                    command=command.replace('%IN',fpath),
                    workarea=workarea,
                    inputfile=fpath,
                    envs=envs,
                    binds=binds,
                    status=JobStatus.REGISTERED,
                    partition=partition
                  )
      task_db.jobs.append(job_db)


    # NOTE: Should we skip test here?
    if not do_test:
      logger.info("Skipping local test...")
      session().add(task_db)
      if not dry_run:
        logger.info("Commit task into the database.")
        session.commit()
      logger.info( "Succefully created.")
      return task_db.id
   

    # NOTE: Test my job localy
    logger.info("Applying local test...")
    if test_job( task_db.jobs[0] ):
      session().add(task_db)
      if not dry_run:
        logger.info("Commit task into the database.")
        session.commit()  
      logger.info("Succefully created.")
      return task_db.id
    else:
      logger.error("Local test failed.")
      return None

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error.")
    return None



def kill( session: postgres_session, task_id: int ) -> bool:

  try:
    task = session().query(Task).filter(Task.id==task_id).first()
    if not task:
        logger.warning( f"The task with id ({task_id}) does not exist into the data base" )
        return False
    task.kill()
    session.commit()
    logger.info(f"Succefully kill.")
    return True
  except Exception as e:
    traceback.print_exc()
    logger.info("Unknown error.")
    return False



def retry( session: postgres_session, task_id: int ) -> bool:
  try:
    task = session().query(Task).filter(Task.id==task_id).first()
    if not task:
      logger.warning(f"The task with id ({task_id}) does not exist into the data base" )
      return False
    
    if task.completed():
      logger.info(f"The task with id ({task.status}) is in COMPLETED TaskStatus. Can not retry." )
      return False
    
    task.retry()
    session.commit()
    logger.info(f"Succefully retry.")
    return True
  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False


def delete( session: postgres_session, task_id: int, force=False , remove=False) -> bool:

  try:
    # Get task by id
    task = session().query(Task).filter(Task.id==task_id).first()
    if not task:
      logger.warning(f"The task with id ({task_id}) does not exist into the data base" )
      return False


    if not force:
      task.delete()
      session.commit()
      while task.status != TaskStatus.REMOVED:
        logger.info(f"Waiting for schedule... Task with status {task.status}")
        sleep(2)
    
    session().query(Job).filter(Job.taskid==task_id).delete()
    session().query(Task).filter(Task.id==task_id).delete()
    session.commit()
    logger.info("Succefully deleted.")
    return True
 

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False






class task_parser:

  def __init__(self , host, args=None):

    self.db = postgres(host)
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
      create_parser.add_argument('--image', action='store', dest='image', required=False, default="",
                          help = "The singularity sif image path.")
      create_parser.add_argument('--exec', action='store', dest='command', required=True,
                          help = "The exec command")
      create_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                          help = "Use this as debugger.")
      create_parser.add_argument('--do_test', action='store_true', dest='do_test', required=False, default=False,
                          help = "Do local test")
      create_parser.add_argument('-e','--email', action='store', dest='email', required=True,
                          help = "The user email contact.")
      create_parser.add_argument('--binds', action='store', dest='binds', required=False, default="{}",
                          help = "image volume bindd like {'/home':'/home','/mnt/host_volume:'/mnt/image_volume'}")
      create_parser.add_argument('-p', '--partition',action='store', dest='partition', required=True,
                          help = f"The selected partitions. Availables: {partitions}")


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
                    args.image, args.command, args.email, dry_run=args.dry_run,
                    do_test=args.do_test, binds=args.binds, partition=args.partition)

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
                   image: str, command: str, email: str, dry_run: bool=False, do_test=True,
                   extension='.json', binds="{}", partition='cpu' ):

    with self.db as session:
      return create(session, basepath, taskname, inputfile, image, command, email, 
                    dry_run=dry_run, do_test=do_test, binds=binds, partition=partition)

  def kill(self, task_ids):
    with self.db as session:
      for task_id in task_ids:
        kill(session, task_id)

  def delete(self, task_ids, force=False):
    with self.db as session:
      for task_id in task_ids:
        delete(session, task_id, force=force)

  def retry(self, task_ids):
    with self.db as session:
      for task_id in task_ids:
        retry(session, task_id)

  def list(self):
    print('not implemented')
  #  print(self.db.resume())


  

















