
__all__ = ["TaskParser"]


import glob, traceback, os, argparse

from sqlalchemy import and_, or_
from prettytable import PrettyTable
from tqdm import tqdm

from enumerations import JobStatus, TaskStatus, TaskTrigger
from models import Task, Job






 #
# Create the new task
#
def create( db: client_postgres, basepath: str, taskname: str, inputfile: str,
            image: str, command: str, dry_run: bool=False, do_test=True):

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
    files = expand_folders(inputfile)

    offset = db.generate_id(Job)
    for idx, fpath in tqdm( enumerate(files) ,  desc= 'Creating... ', ncols=100):
      
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


























class task_parser:


  def __init__(self , db, args=None):

    self.__db = db
    if args:

      # Create Task
      create_parser = argparse.ArgumentParser(description = '', add_help = False)


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

      create_parser.add_argument('--skip_test', action='store_true', dest='skip_test', required=False, default=False,
                          help = "Skip local test.")
      


      delete_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser.add_argument('--id', action='store', nargs='+', dest='id_list', required=False, default=None,
                    help = "All task ids to be removed", type=int)
      delete_parser.add_argument('--id_min', action='store',  dest='id_min', required=False,
                    help = "Down taks id limit to apply on the loop", type=int, default=None)
      delete_parser.add_argument('--id_max', action='store',  dest='id_max', required=False,
                    help = "Upper task id limit to apply on the loop", type=int, default=None)
      delete_parser.add_argument('--remove', action='store_true', dest='remove', required=False,
                    help = "Remove all files for this task into the storage. Beware when use this flag becouse you will lost your data too.")
      delete_parser.add_argument('--force', action='store_true', dest='force', required=False,
                    help = "Force delete.")


      list_parser = argparse.ArgumentParser(description = '', add_help = False)
      list_parser.add_argument('-a','--all', action='store_true', dest='all', required=False,
                    help = "List all tasks.")
      list_parser.add_argument('-i','--interactive', action='store_true', dest='interactive', required=False,
                    help = "List all tasks interactive mode.")


      retry_parser = argparse.ArgumentParser(description = '', add_help = False)
      retry_parser.add_argument('--id', action='store', nargs='+', dest='id_list', required=False, default=None,
                    help = "All task ids to be retry", type=int)
      retry_parser.add_argument('--id_min', action='store',  dest='id_min', required=False,
                    help = "Down taks id limit to apply on the loop", type=int, default=None)
      retry_parser.add_argument('--id_max', action='store',  dest='id_max', required=False,
                    help = "Upper task id limit to apply on the loop", type=int, default=None)


      kill_parser = argparse.ArgumentParser(description = '', add_help = False)
      kill_parser.add_argument('--id', action='store', nargs='+', dest='id_list', required=False, default=None,
                    help = "All task ids to be removed", type=int)
      kill_parser.add_argument('--id_min', action='store',  dest='id_min', required=False,
                    help = "Down taks id limit to apply on the loop", type=int, default=None)
      kill_parser.add_argument('--id_max', action='store',  dest='id_max', required=False,
                    help = "Upper task id limit to apply on the loop", type=int, default=None)





      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('retry' , parents=[retry_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list'  , parents=[list_parser])
      subparser.add_parser('kill'  , parents=[kill_parser])
      args.add_parser( 'task', parents=[parent] )




  def compile( self, args ):


    def get_task_ids( _args ):
      if _args.id_list:
        ids = _args.id_list
      elif _args.id_min and _args.id_max:
        ids= list( range( _args.id_min, _args.id_max+1 ) )
      return ids

    # Task CLI
    if args.mode == 'task':

      # create task
      if args.option == 'create':
        answer, message = self.create(os.getcwd(),
                                  args.taskname,
                                  args.inputfile,
                                  args.image,
                                  args.command,
                                  args.dry_run,
                                  args.skip_test)

        if not answer:
          print(ERROR+message)
        else:
          print(INFO+message)


      # retry option
      elif args.option == 'retry':
        answer, message = self.retry(get_task_ids(args))
        if not answer:
          print(ERROR+message)
        else:
          print(INFO+message)

      # delete option
      elif args.option == 'delete':
        answer, message = self.delete(get_task_ids(args), force=args.force)
        if not answer:
          print(ERROR+message)
        else:
          print(INFO+message)

      # list all tasks
      elif args.option == 'list':
        answer, message = self.list(args.all, args.interactive)
        if not answer:
          print(ERROR+message)
        else:
          print(message)

      # kill option
      elif args.option == 'kill':
        answer, message = self.kill(get_task_ids(args))
        if not answer:
          print(ERROR+answer)
        else:
          print(INFO+answer)

      else:
        print(ERROR+"option not available.")






  #
  # Create the new task
  #
  def create( self, basepath,
                    taskname,
                    inputfile,
                    image,
                    command,
                    dry_run=False,
                    skip_local_test=False,
                    ):

    if self.__db.task(taskname) is not None:
      return (False, "The task exist into the database. Abort.")

    if (not '%IN' in command):
      return (False,"The exec command must include '%IN' into the string. This will substitute to the configFile when start.")

    # task volume
    volume = basepath + '/' + taskname

    # create task volume
    if not dry_run:
      os.makedirs(volume, exist_ok=True)

    try:
      task_db = Task( id=self.__db.generate_id(Task),
                      name=taskname,
                      volume=volume,
                      status=TaskStatus.HOLD,
                      action=TaskAction.WAITING)


      # check if input file is json
      


      files = expand_folders(inputfile)
      offset = self.__db.generate_id(Job)
      for idx, fpath in tqdm( enumerate(files) ,  desc= 'Creating... ', ncols=100):
        
        workarea = volume +'/'+ remove_extension( fpath.split('/')[-1] )


        #
        # Get the environ
        #

        # FIXME: Use for future, we need to recover all envs and pass it to the job worker
        #envs = str({ 
        #          'PATH' : os.environ['PATH'],
        #          'PYTHONPATH' : os.environ['PYTHONPATH']
        #       })

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

      task_db.status = TaskStatus.REGISTERED

      if skip_local_test:
        self.__db.session().add(task_db)
        if not dry_run:
          self.__db.commit()
        return (True, "Succefully created.")

     
      if test_locally( task_db.jobs[0] ):
        self.__db.session().add(task_db)
        if not dry_run:
          self.__db.commit()  
        return (True, "Succefully created.")
      else:
        return (False, "Local test failed.")
     
    except Exception as e:
      traceback.print_exc()
      return (False, "Unknown error.")





  def delete( self, task_ids, force=False , remove=False):

    for task_id in task_ids:

      # Get task by id
      task = self.__db.session().query(Task).filter(Task.id==task_id).first()
      if not task:
        return (False, f"The task with id ({task_id}) does not exist into the data base" )
      
      # Check possible status before continue
      if not force:
        if not task.status in [TaskStatus.BROKEN, TaskStatus.KILLED, TaskStatus.FINALIZED, TaskStatus.COMPLETED]:
          return (False, f"The task with current status {task.status} can not be deleted. The task must be in completed, finalized, killed or broken status.")
      
      try:
        self.__db.session().query(Job).filter(Job.taskid==task_id).delete()
        self.__db.session().query(Task).filter(Task.id==task_id).delete()
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()


    return (True, "Succefully deleted.")





  def list( self, list_all=False, interactive=False):


    # helper function to print my large table
    def table( tasks, list_all ):
      t = PrettyTable([

                        'TaskID'    ,
                        'Taskname'  ,
                        'Registered',
                        'Assigned'  ,
                        'Testing'   ,
                        'Running'   ,
                        'Failed'    ,
                        'Completed' ,
                        'kill'      ,
                        'killed'    ,
                        'broken'    ,
                        'Status'     ,
                        ])

      def count( jobs ):
        status = [JobStatus.REGISTERED, JobStatus.ASSIGNED , JobStatus.TESTING, 
                  JobStatus.RUNNING   , JobStatus.COMPLETED, JobStatus.FAILED, 
                  JobStatus.KILL      , JobStatus.KILLED   , JobStatus.BROKEN]
        total = { str(key):0 for key in status }
        for job in jobs:
          for s in status:
            if job.status==s: total[s]+=1
        return total

      for task in tasks:
        jobs = task.jobs
        if not list_all and (task.status == TaskStatus.COMPLETED):
          continue

        total = count(jobs)
        registered    = total[ JobStatus.REGISTERED]
        assigned      = total[ JobStatus.ASSIGNED  ] 
        testing       = total[ JobStatus.TESTING   ] 
        running       = total[ JobStatus.RUNNING   ] 
        completed     = total[ JobStatus.COMPLETED ] 
        failed        = total[ JobStatus.FAILED    ] 
        kill          = total[ JobStatus.KILL      ] 
        killed        = total[ JobStatus.KILLED    ] 
        broken        = total[ JobStatus.BROKEN    ] 
        status        = task.status

        t.add_row(  [task.id, task.name, registered,  assigned, 
                     testing, running, failed, completed, kill, killed, broken, 
                     status] )
      return t



    if interactive:
        while True:
            time.sleep(10*SECONDS)
            os.system("clear")
            print(table(self.__db.tasks(), list_all))
    else:
        return (True, table(self.__db.tasks(), list_all))






  def kill( self, task_ids ):

    for task_id in task_ids:
      try:
        task = self.__db.session().query(Task).filter(Task.id==task_id).first()
        if not task:
            return (False, f"The task with id ({task_id}) does not exist into the data base")
        task.action = TaskAction.KILL
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()
        return (False, "Unknown error." )

    return (True, "Succefully killed.")




  def retry( self, task_ids ):

    for task_id in task_ids:
      try:
        task = self.__db.session().query(Task).filter(Task.id==task_id).first()
        if not task:
            return (False, f"The task with id ({task_id}) does not exist into the data base" )
        if task.status == TaskStatus.COMPLETED:
            return (False, f"The task with id ({task.status}) is in COMPLETED TaskStatus. Can not retry." )
        task.action = TaskAction.RETRY
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()
        return (False, "Unknown error." )

    return (True, "Succefully retry.")









