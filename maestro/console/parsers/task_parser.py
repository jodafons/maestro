__all__ = ["task_parser"]


import traceback, os, argparse, re

from time import time, sleep
from mlflow.tracking import MlflowClient
from expand_folders import expand_folders
from tabulate import tabulate
from tqdm import tqdm
from loguru import logger

from maestro.servers.executor.consumer import Job as JobTest
from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger, job_status
from maestro.models import Task, Job, Database, Session
from maestro import schemas

from rich_argparse import RichHelpFormatter


def convert_string_to_range(s):
     """
       convert 0-2,20 to [0,1,2,20]
     """
     return sum((i if len(i) == 1 else list(range(i[0], i[1]+1))
                for i in ([int(j) for j in i if j] for i in
                re.findall(r'(\d+),?(?:-(\d+))?', s))), [])




def test_job( job_db, timeout : int=120 ):

    # NOTE: if inside of singularity, put image and empty.
    image = "" if os.environ['SINGULARITY_CONTAINER'] else job_db.image

    job = JobTest( job_id       = job_db.id, 
                   taskname     = job_db.task.name,
                   command      = job_db.command,
                   image        = image, 
                   workarea     = job_db.workarea,
                   device       = 0,
                   binds        = {},
                   testing      = True,
                   run_id       = "",
                   tracking_url = "" )

    start = time()
    while True:
        sleep(1)
        if job.status() == JobStatus.PENDING:
            if not job.run():
              return False
        elif job.status() == JobStatus.FAILED:
            return False
        elif (time() - start) > timeout:
          logger.info('testing timeout reached. approving...')
          job.kill()
          job_db.status=JobStatus.REGISTERED
          return True
        elif job.status() == JobStatus.RUNNING:
            continue
        elif job.status() == JobStatus.COMPLETED:
            job_db.status=JobStatus.REGISTERED
            return True
        else:
            continue


def create_tracking( tracking_url : str, task : Task ):

  # get tracking server
  try:
    if tracking_url != "":
      logger.info(f"tracking server from {tracking_url}")
      tracking      = MlflowClient( tracking_url )
      experiment_id = tracking.create_experiment( task.name )
      task.experiment_id = experiment_id
    return True
  except Exception as e:
    traceback.print_exc()
    return False



def create( session   : Session, 
            basepath  : str, 
            taskname  : str, 
            inputfile : str,
            image     : str, 
            virtualenv: str,
            command   : str, 
            dry_run   : bool=False, 
            extension : str='.json', 
            binds     : str="{}", 
            partition : str="cpu",
            contact_to: str="",
            parents   : list=[],
            envs      : str="{}",
            priority  : int=1,
          ) -> bool:
            
  

  server_url = session.get_environ("PILOT_SERVER_URL")
  server = schemas.client( server_url, 'pilot')
  if not server.ping():
    logger.error("The server is not online. please setup the server before launch it... ")
    return None
    
  tracking_url = session.get_environ("TRACKING_SERVER_URL")

  if session.get_task(taskname) is not None:
    logger.error("The task exist into the database. Abort.")
    return None

  if len(parents) > 0:
    for task_id in parents:
      if session.get_task(task_id) is None:
        logger.error(f"The parent task id {task_id} does not exist into the database. Abort")
        return None


  if (not '%IN' in command):
    logger.error("The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
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
                    parents=str(parents),
                    status=TaskStatus.REGISTERED,
                    trigger=TaskTrigger.WAITING,
                    contact_to=contact_to,
                    priority=priority )
                    
    # check if input file is json
    files = expand_folders( inputfile )

    if len(files) == 0:
      logger.error(f"It is not possible to find jobs into {inputfile}... Please check and try again...")
      return None

    offset = session.generate_id(Job)
    for idx, fpath in tqdm( enumerate(files) ,  desc= 'Creating... ', ncols=50):
      
      extension = fpath.split('/')[-1].split('.')[-1]
      job_name  = fpath.split('/')[-1].replace('.'+extension, '')
      workarea  = volume +'/'+ job_name

      job_db = Job(
                    name=job_name,
                    id=offset+idx,
                    image=image,
                    virtualenv=virtualenv,
                    command=command.replace('%IN',fpath),
                    workarea=workarea,
                    inputfile=fpath,
                    envs=envs,
                    binds=binds,
                    status=JobStatus.REGISTERED,
                    partition=partition,
                    run_id="",
                    priority=priority,
                  )

      task_db.jobs.append(job_db)


    if not test_job( task_db.jobs[0] ):
      logger.fatal("local test fail...")
      return None
   
    if dry_run:
      return task_db.id

    if create_tracking(tracking_url, task_db):
      session().add(task_db)
      session.commit()
      return task_db.id
    else:
      logger.error("some problem to create the experiment into the tracking server... abort")
      return None


  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error.")
    return None





class task_parser:

  def __init__(self, args):


    # Create Task
    create_parser = argparse.ArgumentParser(description = '', add_help = False)
    delete_parser = argparse.ArgumentParser(description = '', add_help = False)
    retry_parser  = argparse.ArgumentParser(description = '', add_help = False)
    list_parser   = argparse.ArgumentParser(description = '', add_help = False)
    kill_parser   = argparse.ArgumentParser(description = '', add_help = False)


    database_parser   = argparse.ArgumentParser(description = '', add_help = False)
    database_parser.add_argument('--database-url', action='store', dest='database_url', type=str,
                  required=False, default =  os.environ["DATABASE_SERVER_URL"] ,
                  help = "the database url endpoint.")


    create_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                        help = "The name of the task to be included into the maestro.")
    create_parser.add_argument('-i','--inputfile', action='store',
                        dest='inputfile', required = True,
                        help = "The input config file that will be used to configure the job.")
    create_parser.add_argument('--image', action='store', dest='image', required=False, default="",
                        help = "The singularity image path to be used during the job execution.")
    create_parser.add_argument('--virtualenv', action='store', dest='virtualenv', required=False, default="",
                        help = "The virtualenv path to be used during the job execution.")
    create_parser.add_argument('--exec', action='store', dest='command', required=True,
                        help = "The exec command to be used when the job start.")
    create_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                        help = "Only prepare the command but not launch into the database. Use this as debugger.")
    create_parser.add_argument('--binds', action='store', dest='binds', required=False, default="{}", type=str,
                        help = "image volume binds to be append during the singularaty command preparation. The format should be: {'/home':'/home','/mnt/host_volume:'/mnt/image_volume'}.")
    create_parser.add_argument('-p', '--partition',action='store', dest='partition', required=True,
                        help = f"The name of the partition where this task will be executed.")
    create_parser.add_argument('--contact_to', action='store', dest='contact_to', required=False, default="",
                        help = "The email contact used to send the task notification.")
    create_parser.add_argument('--parents', action='store', dest='parents', required=False, default='', type=str,
                        help = "The parent task id. Can be a list of ids (e.g, 0,1-3,5)")
    create_parser.add_argument('--envs', action='store', dest='envs', required=False, default="{}", type=str,
                        help = "Extra environs to be added into the process environ system during the job execution. The format should be: {'ENV':'VALUE', ...}.")
    create_parser.add_argument('--priority', action='store', dest='priority', required=False, default=1, type=int,
                        help = "the task priority value to give some execution priority into the queue.")


    delete_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                  help = "All task ids to be deleted", type=str)                     

    retry_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                              help = "All task ids to be retried", type=str)

    kill_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                             help = "All task ids to be killed", type=str)
                       

    parent = argparse.ArgumentParser(description = '', add_help = False, formatter_class=RichHelpFormatter)
    subparser = parent.add_subparsers(dest='option')
    # Datasets
    subparser.add_parser('create', parents=[create_parser, database_parser], formatter_class=RichHelpFormatter)
    subparser.add_parser('retry' , parents=[retry_parser, database_parser] , formatter_class=RichHelpFormatter)
    subparser.add_parser('delete', parents=[delete_parser, database_parser], formatter_class=RichHelpFormatter)
    subparser.add_parser('list'  , parents=[list_parser, database_parser]  , formatter_class=RichHelpFormatter)
    subparser.add_parser('kill'  , parents=[kill_parser, database_parser]  , formatter_class=RichHelpFormatter)
    args.add_parser( 'task', parents=[parent], formatter_class=RichHelpFormatter )

  

  def parser( self, args ):

    if args.mode == 'task':
      if args.option == 'create':
        self.create(os.getcwd(), args)
      elif args.option == 'retry':
        self.retry(convert_string_to_range(args.id_list), args)
      elif args.option == 'delete':
        self.delete(convert_string_to_range(args.id_list), args)   
      elif args.option == 'list':
        self.list(args)   
      elif args.option == 'kill':
        self.kill(convert_string_to_range(args.id_list), args)   
      else:
        logger.error("Option not available.")





  #
  # kill task
  #
  def kill(self, task_ids, args):
    try:
      db = Database(args.database_url)
      with db as session:
        for task_id in task_ids:
          # Get task by id
          task = session().query(Task).filter(Task.id==task_id).first()
          if not task:
            logger.warning(f"The task with id ({task_id}) does not exist into the data base" )
            continue
          task.kill()
          session.commit()
          logger.info("Succefully killed.")
          return True
    except Exception as e:
      traceback.print_exc()
      logger.error("Unknown error." )



  #
  # delete task
  #
  def delete(self, task_ids, args):
    try:
      db = Database(args.database_url)
      with db as session:
        for task_id in task_ids:
          # Get task by id
          task = session().query(Task).filter(Task.id==task_id).first()
          if not task:
            logger.warning(f"The task with id ({task_id}) does not exist into the data base" )
            continue
          task.delete()
          session.commit()
          logger.info("Succefully deleted.")
          return True
    except Exception as e:
      traceback.print_exc()
      logger.error("Unknown error." )


  #
  # retry task
  #
  def retry(self, task_ids, args):
    try:
      db = Database(args.database_url)
      with db as session:
        for task_id in task_ids:
          # Get task by id
          task = session().query(Task).filter(Task.id==task_id).first()
          if not task:
            logger.warning(f"The task with id ({task_id}) does not exist into the data base" )
            continue
          if task.completed():
            logger.info(f"The task with id ({task.status}) is in COMPLETED TaskStatus. Can not retry." )
            continue
          task.retry()
          session.commit()
          logger.info("Succefully retry.")
          return True
    except Exception as e:
      traceback.print_exc()
      logger.error("Unknown error." )


  def list(self, args):

    try: 
      db = Database(args.database_url)
      with db as session:
        tasks = session().query(Task).all()
        table = []
        for task in tasks:
          values        = task.count()
          row = [task.id, task.name]
          row.extend([values[status] for status in job_status])
          row.append(task.status)
          table.append(row)

        t = tabulate(table,  headers=[
                      'ID'        ,
                      'Task'      ,
                      'Registered',
                      'Assigned'  ,
                      'Pending'   ,
                      'Running'   ,
                      'Completed' ,
                      'Failed'    ,
                      'kill'      ,
                      'killed'    ,
                      'Broken'    ,
                      'Status'    ,
                      ],tablefmt="heavy_outline")
        print(t)

    except Exception as e:
      traceback.print_exc()
      logger.error("Unknown error." )



  def create(self, basepath: str, args ):
    db = Database(args.database_url)
    with db as session:
      return create(session, basepath, 
                    args.taskname, 
                    args.inputfile, 
                    args.image, 
                    args.virtualenv,
                    args.command, 
                    dry_run=args.dry_run, 
                    binds=args.binds, 
                    partition=args.partition,
                    contact_to=args.contact_to,
                    parents=convert_string_to_range(args.parents),
                    envs=args.envs,
                    priority=args.priority )























