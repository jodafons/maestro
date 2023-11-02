__all__ = ["task_parser"]


import glob, traceback, os, argparse, re
from time import sleep

from expand_folders import expand_folders
from tabulate import tabulate
from tqdm import tqdm
from loguru import logger

from maestro.servers.executor.consumer import Job as JobTest
from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger, job_status
from maestro.models import Task, Job, Database, Session


def convert_string_to_range(s):
     """
       convert 0-2,20 to [0,1,2,20]
     """
     return sum((i if len(i) == 1 else list(range(i[0], i[1]+1))
                for i in ([int(j) for j in i if j] for i in
                re.findall(r'(\d+),?(?:-(\d+))?', s))), [])



def test_job( job_db ):

    job = JobTest( job_id     = job_db.id, 
               taskname   = job_db.task.name,
               command    = job_db.command,
               image      = job_db.image, 
               workarea   = job_db.workarea,
               device     = -1,
               job_db     = job_db,
               extra_envs = {"JOB_LOCAL_TEST":'1'},
               binds      = {},
               dry_run    = False)


    while True:
        if job.status() == JobStatus.PENDING:
            if not job.run():
              return False
        elif job.status() == JobStatus.FAILED:
            return False
        elif job.status() == JobStatus.RUNNING:
            continue
        elif job.status() == JobStatus.COMPLETED:
            job_db.status=JobStatus.REGISTERED
            return True
        else:
            continue


partitions = os.environ.get("EXECUTOR_AVAILABLE_PARTITIONS","").split(',')

def create( session: Session, basepath: str, taskname: str, inputfile: str,
            image: str, command: str, dry_run: bool=False, do_test=True,
            extension='.json', binds="{}", partition="cpu") -> bool:




  if session.get_task(taskname) is not None:
    logger.error("The task exist into the database. Abort.")
    return None

  if (not '%IN' in command):
    logger.error("The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
    return None

  #if (not partition in partitions):
  #  logger.error(f"Partition {partition} not available.")
  #  return None

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
                    trigger=TaskTrigger.WAITING)
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



def kill( session: Session, task_id: int ) -> bool:

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



def retry( session: Session, task_id: int ) -> bool:
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


def delete( session: Session, task_id: int ) -> bool:

  try:
    # Get task by id
    task = session().query(Task).filter(Task.id==task_id).first()
    if not task:
      logger.warning(f"The task with id ({task_id}) does not exist into the data base" )
      return False

    task.delete()
    session.commit()
    while task.status != TaskStatus.REMOVED:
      logger.info(f"Waiting for schedule... Task with status {task.status}")
      sleep(2)
    logger.info("Succefully deleted.")
    return True
 

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return False


def list_tasks( session: Session ):

  try:

    tasks = session().query(Task).all()
    table = []
    for task in tasks:
      values        = task.count()
      row = [task.id, task.name]
      row.extend([values[status] for status in job_status])
      row.append(task.status)
      table.append(row)

    t = tabulate(table,  headers=[
                  'ID'    ,
                  'Task'  ,
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
    return t

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return "Not possible to show the table."


class task_parser:

  def __init__(self , host, args=None):

    self.db = Database(host)
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
                    args.image, args.command, dry_run=args.dry_run,
                    do_test=args.do_test, binds=args.binds, partition=args.partition)

      elif args.option == 'retry':
        self.retry(convert_string_to_range(args.id_list))
        
      elif args.option == 'delete':
        self.delete(convert_string_to_range(args.id_list), force=args.force)
        
      elif args.option == 'list':
        self.list()
        
      elif args.option == 'kill':
        self.kill(convert_string_to_range(args.id_list))
        
      else:
        logger.error("Option not available.")


  def create(self, basepath: str, taskname: str, inputfile: str,
                   image: str, command: str, dry_run: bool=False, do_test=True,
                   extension='.json', binds="{}", partition='cpu' ):

    with self.db as session:
      return create(session, basepath, taskname, inputfile, image, command, 
                    dry_run=dry_run, do_test=do_test, binds=binds, partition=partition)

  def kill(self, task_ids):
    with self.db as session:
      for task_id in task_ids:
        kill(session, task_id)

  def delete(self, task_ids, force=False):
    with self.db as session:
      for task_id in task_ids:
        delete(session, task_id)

  def retry(self, task_ids):
    with self.db as session:
      for task_id in task_ids:
        retry(session, task_id)

  def list(self):
    with self.db as session:
      print(list_tasks(session))
  

  

















