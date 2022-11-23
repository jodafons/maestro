
__all__ = ["TaskParser"]


import glob, traceback, os, argparse

from orchestra.database.models import Task,Job
from orchestra.status import JobStatus, TaskStatus, TaskAction
from orchestra.api import test_locally, remove_extension
from sqlalchemy import and_, or_
from prettytable import PrettyTable
from tqdm import tqdm
from curses import OK






#
# Task parser
#
class TaskParser:


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
      create_parser.add_argument('--exec', action='store', dest='execCommand', required=True,
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
      subparser.add_parser('retry', parents=[retry_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])
      subparser.add_parser('kill', parents=[kill_parser])
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
        ok , answer = self.create(os.getcwd(),
                                  args.taskname,
                                  args.inputfile,
                                  args.command,
                                  args.dry_run,
                                  args.skip_test)

        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)


      # retry option
      elif args.option == 'retry':
        ok, answer = self.retry(get_task_ids(args))
        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)

      # delete option
      elif args.option == 'delete':
        ok , answer = self.delete(get_task_ids(args), force=args.force)
        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)

      # list all tasks
      elif args.option == 'list':
        ok, answer = self.list(args.all, args.interactive)
        if not ok:
          MSG_FATAL(answer)
        else:
          print(answer)

      # kill option
      elif args.option == 'kill':
        ok, answer = self.kill(get_task_ids(args))
        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)

      else:
        MSG_FATAL("option not available.")






  #
  # Create the new task
  #
  def create( self, basepath,
                    taskname,
                    inputfile,
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
      os.makedir(volume, exist_ok=True)

    try:
      task_db = Task()
      task_db = Task( self.__db.generate_id(Task),
                      name=taskname,
                      volume=volume,
                      status=TaskStatus.HOLD,
                      action=TaskAction.WAITING)

      files = glob.glob(inputdir+'/*', recursive=True)

      offset = self.__db.generate_id(Job)
      for idx, inputfile in tqdm( enumerate(files) ,  desc= 'Creating... ', ncols=100):
        jobname = remove_extension( file.split('/')[-1] )
        job_db = Job(
                    id=offset+idx,
                    command=command.replace('%IN',file),
                    name = jobname,
                    inputfile=inputfile,
                    status=JobStatus.REGISTERED)
        task_db+=job_db

      task_db.status = TaskStatus.REGISTERED

      if skip_local_test:
        self.__db.session().add(task)
        if not dry_run
          self.__db.commit()
        return (True, "Succefully created.")


      if test_locally( task.jobs[0] ):
        self.__db.session().add(task)
        if not dry_run:
          self.__db.commit()  
        return (True, "Succefully created.")
      else:
        return (False, "Local test failed.")
     
    except Exception as e:
      traceback.print_exc()
      return (False, "Unknown error.")





  def delete( self, task_id_list, force=False , remove=False):


    for id in task_id_list:

      # Get task by id
      task = self.__db.session().query(Task).filter(Task.id==id).first()
      if not task:
        return (False, "The task with id (%d) does not exist into the data base"%id )
      
      # Check possible status before continue
      if not force:
        if not task.status in [TaskStatus.BROKEN, TaskStatus.KILLED, TaskStatus.FINALIZED, TaskStatus.COMPLETED]:
          return (False, "The task with current status %s can not be deleted. The task must be in COMPLETED, finalized, killed or broken TaskStatus."% task.getStatus() )
      
      volume = task.volume

      # remove all jobs that allow to this task
      try:
        self.__db.session().query(Job).filter(Job.taskid==id).delete()
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()


      # remove the task table
      try:
        self.__db.session().query(Task).filter(Task.id==id).delete()
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()


    return (True, "Succefully deleted.")





  def list( self, list_all=False, interactive=False):


    # helper function to print my large table
    def ptable( tasks, list_all ):
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
                        'State'     ,
                        ])

      def count( jobs ):
        states = [TaskStatus.REGISTERED, TaskStatus.ASSIGNED, TaskStatus.TESTING, 
                  TaskStatus.RUNNING   , TaskStatus.COMPLETED, TaskStatus.FAILED, 
                  TaskStatus.KILL      , TaskStatus.KILLED  , TaskStatus.BROKEN]

        total = { str(key):0 for key in states }
        for job in jobs:
          for s in states:
            if job.state==s: total[str(s)]+=1
        return total

      for task in tasks:
        jobs = task.jobs
        if not list_all and (task.state == TaskStatus.COMPLETED):
          continue
        total = count(jobs)
        registered    = total[ TaskStatus.REGISTERED]
        assigned      = total[ TaskStatus.ASSIGNED  ] 
        testing       = total[ TaskStatus.TESTING   ] 
        running       = total[ TaskStatus.RUNNING   ] 
        completed     = total[ TaskStatus.COMPLETED      ] 
        failed        = total[ TaskStatus.FAILED    ] 
        kill          = total[ TaskStatus.KILL      ] 
        killed        = total[ TaskStatus.KILLED    ] 
        broken        = total[ TaskStatus.BROKEN    ] 
        state         = task.state

        t.add_row(  [task.id, task.taskname, registered,  assigned, 
                     testing, running, failed,  COMPLETED, kill, killed, broken, 
                     state] )
      return t



    if interactive:
        from server.main import Clock
        from server import SECOND
        import os
        clock = Clock(10*SECOND)
        while True:
            if clock():
                tasks = self.__db.tasks()

                os.system("clear")
                print(ptable(tasks, list_all))
    else:
        tasks = self.__db.tasks()
        t = ptable(tasks, list_all)
        return (True, t)






  def kill( self, task_id_list ):

    for id in task_id_list:
      try:
        # Get task by id
        task = self.__db.session().query(Task).filter(Task.id==id).first()
        if not task:
            return (False, "The task with id (%d) does not exist into the data base"%id )
        # Send kill TaskStatus to the task
        task.TaskStatus = TaskStatus.KILL
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()
        return (False, "Unknown error." )

    return (True, "Succefully killed.")




  def retry( self, task_id_list ):

    for id in task_id_list:
      try:
        task = self.__db.session().query(Task).filter(Task.id==id).first()
        if not task:
            return (False, "The task with id (%d) does not exist into the data base"%id )
        
        if task.state == TaskStatus.COMPLETED:
            return (False, "The task with id (%d) is in COMPLETED TaskStatus. Can not retry."%id )

        task.TaskStatus = TaskStatus.RETRY
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()
        return (False, "Unknown error." )

    return (True, "Succefully retry.")









