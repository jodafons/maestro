

from database.models import Job
from sqlalchemy import and_
from loguru import logger

import traceback, time





class Schedule:

  def __init__(self, db, postman):
    self.db = db
    self.postman = postman 
    self.states = []


  def transition( self, source, dest, relationship ):
    if type(relationship) is not list:
      relationship=[relationship]
    self.states.append( (source, relationship, dest) )


  def run(self):

    self.treat_running_jobs_not_alive()

    for task in self.db.tasks():
      self.eval(task)

    self.db.commit()
    return True



  def eval(self, task):

    # Get the current JobStatus information
    current = task.status
    # Run all JobStatus triggers to find the correct transiction
    for source, relationship, dest in self.states:
      # Check if the current JobStatus is equal than this JobStatus
      if source == current:
        answer = True
        # Execute all triggers into this JobStatus
        for question in relationship:
          try:

            start = time.time()
            answer = getattr(self, question)(task) 
            end = time.time()
            print(INFO + f'{question} toke %1.4f seconds'%(end-start))
          except Exception as e:
            traceback.print_exc()
            print(ERROR + f"Exception raise in state {current} for this task {task.name}")
          if not answer:
            break
        if answer:
          task.status = dest
          break




  def jobs(self, njobs):
    try:
      jobs = self.db.session().query(Job).filter(  Job.status==JobStatus.ASSIGNED  ).order_by(Job.id).limit(njobs).with_for_update().all()
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
    #return self.db.session().query(Job).filter( and_( Job.status==JobStatus.RUNNING) ).with_for_update().all()
    return self.db.session().query(Job).filter( Job.status==JobStatus.RUNNING ).with_for_update().all()


  def get_jobs( api, task )



  def treat_running_jobs_not_alive(self):
    jobs = self.get_all_running_jobs()
    for job in jobs:
      if not job.is_alive():
        job.status = JobStatus.ASSIGNED





  #
  # Retry all jobs after the user sent the retry action to the task db
  #
  def broken_all_jobs( self, task ):

    # Broken all jobs inside od the task
    for job in task.jobs:
      job.status = JobStatus.BROKEN
    task.action = TaskAction.WAITING
    return True
  

  #
  # Retry all jobs after the user sent the retry action to the task db
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
  # Check if all jobs into this taks is in registered JobStatus
  #
  def all_jobs_are_registered( self, task ):

    total = len(self.db.session().query(Job).filter( Job.taskid==task.id ).all())
    if len(self.db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==JobStatus.REGISTERED ) ).all()) == total:
      return True
    else:
      return False


 

  

  #
  # Check if all jobs into this task ran
  #
  def all_jobs_ran( self, task ):
                                                                                                                    
    total = len(self.db.session().query(Job).filter( Job.taskid==task.id ).all())
    total_completed = len(self.db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==JobStatus.COMPLETED ) ).all())
    total_failed = len(self.db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==JobStatus.FAILED ) ).all())
    if (total_completed + total_failed) == total:
      return True
    else:
      return False


  #
  # Check if all jobs into this task ran
  #
  def check_not_allow_job_status_in_running_state( self, task ):
    
    exist_registered_jobs = False
    for job in task.jobs:
      if job.status==JobStatus.REGISTERED or job.status==JobStatus.PENDING: 
        job.status = JobStatus.ASSIGNED
        exist_registered_jobs=True
    return exist_registered_jobs







  




class Transition:
  def __init__(self, source=  , target=  , function=  ):


  def __call__(self , ):



def send_email( task ):
  """
  Send an email with the task status
  """
  status = task.status
  taskname = task.name
  email = task.email
  subject = f"[LPS Cluster] Notification for task id {status}"
  message = (f"The task with name {taskname} was assigned with {status} status.")
  logger.info(f"Sending email to {email}") 
  api.mailing().send(email, subject, message)
  return True


def assigned_all_jobs( task ):
  """
  Force all jobs with ASSIGNED status
  """
  logger.info("Assigne all jobs...")
  for job in task.jobs:
      if job.status != JobStatus.COMPLETED:
        job.status =  JobStatus.ASSIGNED
  return True


def test_job_fail( task ):
  """
    Check if the first job returns fail
  """
  job = task.jobs[0]
  return (job.status == JobStatus.FAILED) or (job.status == JobStatus.BROKEN)
    
 
def test_job_assigned( task ):
  """
    Assigned the fist job to test
  """
  task.jobs[0].JobStatus =  JobStatus.ASSIGNED
  return True


def test_job_completed( task ):
  """
    Check if the test job is completed
  """
  return task.jobs[0].status == JobStatus.COMPLETED


def test_job_running( task ):
  """
    Check if the test job still running
  """
  return task.jobs[0].status == JobStatus.RUNNING


def task_completed( task ):
  """
    Check if all jobs into the task are completed
  """
  return all([job.status==JobStatus.COMPLETED for job in task.jobs])
  

def task_killed( task ):
  """
    Check if all jobs into the task are killed
  """
  return all([job.status==JobStatus.KILLED for job in task.jobs])
  

def task_retry( task ):
  """
    Check if all jobs into the task are killed
  """
  for job in task.jobs:
    if job.status == JobStatus.FAILED:
      if job.retry < 10:
        job.retry+=1
        job.status = JobStatus.ASSIGNED
  task.action = TaskAction.WAITING
  return True
 


def kill_all_jobs( task ):
  """
    Check if all jobs into the task are killed
  """
  if task.action == TaskAction.KILL:
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
# Compile the JobStatus machine
#
def compile(schedule):

  #schedule.transition(source=TaskStatus.REGISTERED, dest=TaskStatus.TESTING    , relationship=['all_jobs_are_registered', 'assigned_one_job_to_test']         )
  schedule.transition( source=TaskStatus.REGISTERED, dest=TaskStatus.TESTING    , relationship=['all_jobs_are_registered']         )
  #schedule.transition(source=TaskStatus.TESTING   , dest=TaskStatus.TESTING    , relationship='test_job_still_running'                                        )
  #schedule.transition(source=TaskStatus.TESTING   , dest=TaskStatus.BROKEN     , relationship=['test_job_fail','broken_all_jobs','send_email_task_broken']    )
  #schedule.transition(source=TaskStatus.TESTING   , dest=TaskStatus.RUNNING    , relationship=['test_job_pass','assigned_all_jobs']                           )
  schedule.transition( source=TaskStatus.TESTING   , dest=TaskStatus.RUNNING    , relationship=['assigned_all_jobs']                                           )
  schedule.transition( source=TaskStatus.BROKEN    , dest=TaskStatus.REGISTERED , relationship='retry_all_jobs'                                                )
  schedule.transition( source=TaskStatus.RUNNING   , dest=TaskStatus.COMPLETED  , relationship=['all_jobs_are_completed', 'send_email_task_completed']         )
  schedule.transition( source=TaskStatus.RUNNING   , dest=TaskStatus.FINALIZED  , relationship=['all_jobs_ran']                                                )
  schedule.transition( source=TaskStatus.RUNNING   , dest=TaskStatus.KILL       , relationship='kill_all_jobs'                                                 )
  schedule.transition( source=TaskStatus.RUNNING   , dest=TaskStatus.RUNNING    , relationship='check_not_allow_job_status_in_running_state'                   )
  schedule.transition( source=TaskStatus.FINALIZED , dest=TaskStatus.RUNNING    , relationship='retry_all_failed_jobs'                                         )
  schedule.transition( source=TaskStatus.KILL      , dest=TaskStatus.KILLED     , relationship=['all_jobs_were_killed','send_email_task_killed']               )
  schedule.transition( source=TaskStatus.KILLED    , dest=TaskStatus.REGISTERED , relationship='retry_all_jobs'                                                )
  schedule.transition( source=TaskStatus.COMPLETED , dest=TaskStatus.REGISTERED , relationship='retry_all_jobs'                                                )

