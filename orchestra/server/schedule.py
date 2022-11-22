

__all__ = ["compile", "Schedule"]


from orchestra.server.database.models import Job
from orchestra.status import JobStatus, TaskAction

from sqlalchemy import and_

from orchestra.enums import *
from orchestra.database import *
from orchestra.utils import *
import traceback

from colorama import *
from colorama import init
init(autoreset=True)


INFO = Style.BRIGHT + Fore.GREEN
ERROR = Style.BRIGHT + Fore.RED

class Schedule:

  def __init__(self, db, postman):

    self.db = db
    self.postman = postman 
    self.states = []


  #
  # Add Transiction and JobStatus into the schedule machine
  #
  def transition( self, source, destination, chain ):
    if type(chain) is not list:
      chain=[chain]
    self.states.append( (source, chain, destination) )



  #
  # execute
  #
  def run(self):

    self.treat_running_jobs_not_alive()
    count = 0
    for task in self.__db.tasks():
      self.trigger(task)
      #if task.JobStatus == JobStatus.RUNNING:
      #  count+=1
      #if count>self.__max_running_tasks:
      #  break

    self.__db.commit()
    return True



  #
  # Execute the correct JobStatus machine for this task
  #
  def trigger(self, task):

    # Get the current JobStatus information
    current = task.JobStatus
    # Run all JobStatus triggers to find the correct transiction
    for source, chain, destination in self.states:
      # Check if the current JobStatus is equal than this JobStatus
      if source == current:
        passed = True
        # Execute all triggers into this JobStatus
        for hypo in chain:
          try:
            passed = hypo(self.db, task) 
          except Exception as e:
            print(e)
            traceback.print_exc()
            taskname = task.name
            print(ERROR + f"Exception raise in state {current} for this task {taskname}")
          if not passed:
            break
        if passed:
          task.status = destination
          break




  def get_jobs(self, njobs):
    try:
      jobs = self.__db.session().query(Job).filter(  Job.status==JobStatus.ASSIGNED  ).order_by(Job.id).limit(njobs).with_for_update().all()
      jobs.reverse()
      return jobs
    except Exception as e:
      traceback.print_exc()
      print(e)
      return []



  #
  # Get all running jobs into the job list
  #
  def get_all_running_jobs(self):
    return self.__db.session().query(Job).filter( and_( Job.JobStatus==JobStatus.RUNNING) ).with_for_update().all()



  def treat_running_jobs_not_alive(self):
    jobs = self.get_all_running_jobs()
    for job in jobs:
      if not job.is_alive():
        job.status = JobStatus.ASSIGNED





  #
  # Retry all jobs after the user sent the retry signal to the task db
  #
  def broken_all_jobs( self, task ):

    # Broken all jobs inside od the task
    for job in task.jobs:
      job.status = JobStatus.BROKEN
    task.action = TaskAction.WAITING
    return True
  

  #
  # Retry all jobs after the user sent the retry signal to the task db
  #
  def retry_all_jobs( self, task ):

    if task.action == TaskAction.RETRY:
      for job in task.jobs:
        job.status = JobStatus.REGISTERED
      task.action = TaskAction.WAITING
      return True
    else:
      return False


  #
  # Send kill JobStatus for all jobs after the user sent the kill singal to the task db
  #
  def kill_all_jobs( self, task ):
    if task.signal == TaskAction.KILL:
      for job in task.jobs:
        if job.status != JobStatus.RUNNING:
          job.status =  JobStatus.KILLED
        else:
          job.status = JobStatus.KILL
      task.action = TaskAction.WAITING
      return True
    else:
      return False


  #
  # Retry all jobs with failed JobStatus after the user sent the retry signal to the task db
  #
  def retry_all_failed_jobs( self, task ):

    for job in task.jobs:
      if job.status == JobStatus.FAILED:
        if job.retry < 3:
          job.retry+=1
          job.status =  JobStatus.ASSIGNED
    task.action = TaskAction.WAITING
    return True
 

  #
  # Check if all jobs into this task were killed
  #
  def all_jobs_were_killed( self, task ):

    total = len(self.db.session().query(Job).filter( Job.taskid==task.id ).all())
    if ( len(self.db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.JobStatus==JobStatus.KILLED ) ).all()) == total ):
      return True
    else:
      return False
  


  #
  # Check if the test job is completed
  #
  def test_job_pass( self, task ):

    # Get the first job from the list of jobs into this task
    job = task.jobs[0]
    if job.JobStatus == JobStatus.COMPLETED:
      return True
    else:
      return False


  #
  # Check if the test job still running
  #
  def test_job_still_running( self, task ):

    # Get the first job from the list of jobs into this task
    job = task.jobs[0]
    if job.status == JobStatus.RUNNING:
      return True
    else:
      return False



  #
  # Check if the test job fail
  #
  def test_job_fail( self, task ):

    # Get the first job from the list of jobs into this task
    job = task.jobs[0]
    if job.status == JobStatus.FAILED or job.status == JobStatus.BROKEN:
      return True
    else:
      return False
   

  #
  # Check if all jobs into this taks is in registered JobStatus
  #
  def all_jobs_are_registered( self, task ):

    total = len(self.__db.session().query(Job).filter( Job.taskid==task.id ).all())
    if len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.JobStatus==JobStatus.REGISTERED ) ).all()) == total:
      return True
    else:
      return False


  #
  # Assigned the first job in the list to test
  #
  def assigned_one_job_to_test( self, task ):

    job = task.jobs[0]
    job.JobStatus =  JobStatus.ASSIGNED
    return True



  #
  # Assigned all jobs
  #
  def assigned_all_jobs( self, task ):

    for job in task.jobs:
      if job.JobStatus != JobStatus.COMPLETED:
        job.JobStatus =  JobStatus.ASSIGNED
    return True




  #
  # Check if all jobs into this task are in completed status
  #
  def all_jobs_completed( self, task ):

    total = len(self.__db.session().query(Job).filter( Job.taskid==task.id ).all())
    if len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.JobStatus==JobStatus.COMPLETED ) ).all()) == total:
      return True
    else:
      return False




  #
  # Check if all jobs into this task ran
  #
  def all_jobs_ran( self, task ):
                                                                                                                    
    total = len(self.__db.session().query(Job).filter( Job.taskid==task.id ).all())
    total_completed = len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==JobStatus.COMPLETED ) ).all())
    total_failed = len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==JobStatus.FAILED ) ).all())
    if (total_completed + total_failed) == total:
      return True
    else:
      return False



  #
  # Check if all jobs into this task ran
  #
  def check_not_allow_job_JobStatus_in_running_JobStatus( self, task ):
    
    exist_registered_jobs = False
    for job in task.jobs:
      if job.JobStatus==JobStatus.REGISTERED or job.JobStatus==JobStatus.PENDING: 
        job.JobStatus = JobStatus.ASSIGNED
        exist_registered_jobs=True
    return exist_registered_jobs



  #
  # Set the timer
  #
  def start_timer(self, task):
    task.startTimer()
    return True




  #
  # Set delete signal
  #
  def send_delete_signal(self, task):
    task.action = TaskAction.DELETE
    return True


  #
  # Assigned task to removed JobStatus and remove all objects from the database and store
  #
  def remove_this_task(self, task):

    if task.action == TaskAction.DELETE:
      try:
        from orchestra.api import TaskParser
        helper = TaskParser(self.__db)
        helper.delete(task.taskname,False)
        return True
      except:
        task.action = TaskAction.WAITING
        task.status = JobStatus.REMOVED
        name = task.name
        print(ERROR + f"It's not possible to delete this task with name {name}")
        return False
    else:
      return False




  #
  #
  # Email notification
  #
  #


  def send_email_task_completed( self, task ):

    subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
    message = ("The task with name %s was assigned with COMPLETED JobStatus.")%(task.taskname)
    self.__postman.send(subject, message)
    return True



  def send_email_task_broken( self, task ):

    subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
    message = ("Your task with name %s was set to BROKEN JobStatus.")%(task.taskname)
    self.__postman.send(subject, message)
    return True
    


  def send_email_task_finalized( self, task ):

    subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
    message = ("The task with name %s was assigned with FINALIZED JobStatus.")%(task.taskname)
    self.__postman.send(subject, message)
    return True



  def send_email_task_killed( self, task ):

    subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
    message = ("The task with name %s was assigned with KILLED JobStatus.")%(task.taskname)
    self.__postman.send(subject, message)
    return True

















 
#
# Compile the JobStatus machine
#
def compile(schedule):

  # Create the JobStatus machine
  #schedule.transition( source=JobStatus.REGISTERED, destination=JobStatus.TESTING    , trigger=['all_jobs_are_registered', 'assigned_one_job_to_test']         )
  schedule.transition( source=JobStatus.REGISTERED, destination=JobStatus.TESTING    , trigger=['all_jobs_are_registered']         )
  #schedule.transition( source=JobStatus.TESTING   , destination=JobStatus.TESTING    , trigger='test_job_still_running'                                        )
  #schedule.transition( source=JobStatus.TESTING   , destination=JobStatus.BROKEN     , trigger=['test_job_fail','broken_all_jobs','send_email_task_broken']    )
  #schedule.transition( source=JobStatus.TESTING   , destination=JobStatus.RUNNING    , trigger=['test_job_pass','assigned_all_jobs']                           )
  schedule.transition( source=JobStatus.TESTING   , destination=JobStatus.RUNNING    , trigger=['assigned_all_jobs']                                           )
  schedule.transition( source=JobStatus.BROKEN    , destination=JobStatus.REGISTERED , trigger='retry_all_jobs'                                                )
  schedule.transition( source=JobStatus.RUNNING   , destination=JobStatus.COMPLETED       , trigger=['all_jobs_are_COMPLETED', 'send_email_task_COMPLETED']                   )
  schedule.transition( source=JobStatus.RUNNING   , destination=JobStatus.FINALIZED  , trigger=['all_jobs_ran','send_email_task_finalized']                    )
  schedule.transition( source=JobStatus.RUNNING   , destination=JobStatus.KILL       , trigger='kill_all_jobs'                                                 )
  schedule.transition( source=JobStatus.RUNNING   , destination=JobStatus.RUNNING    , trigger='check_not_allow_job_JobStatus_in_running_JobStatus'                    )
  schedule.transition( source=JobStatus.FINALIZED , destination=JobStatus.RUNNING    , trigger='retry_all_failed_jobs'                                         )
  schedule.transition( source=JobStatus.KILL      , destination=JobStatus.KILLED     , trigger=['all_jobs_were_killed','send_email_task_killed']               )
  schedule.transition( source=JobStatus.KILLED    , destination=JobStatus.REGISTERED , trigger='retry_all_jobs'                                                )
  schedule.transition( source=JobStatus.COMPLETED      , destination=JobStatus.REGISTERED , trigger='retry_all_jobs'                                                )

