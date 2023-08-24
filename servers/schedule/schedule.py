

from database.models import Job
from sqlalchemy import and_
from loguru import logger
import traceback, time



class Transition:
  def __init__(self, source , target , relationship ):
    self.source = source
    self.target = target
    self.relationship = relationship

  def __call__(self, task, **kwargs):   
    for func in self.relationship:
      if not func(task, **kwargs):
        return False
    return True



def send_email( task , **kwargs):
  """
  Send an email with the task status
  """
  status = task.status
  taskname = task.name
  email = task.email
  subject = f"[LPS Cluster] Notification for task id {status}"
  message = (f"The task with name {taskname} was assigned with {status} status.")
  if kwargs.get('mailing'):
    api = kwargs.get('mailing')
    logger.info(f"Sending email to {email}") 
    api.mailing().send(email, subject, message)
  return True


def test_job_fail( task , **kwargs):
  """
    Check if the first job returns fail
  """
  job = task.jobs[0]
  return (job.status == JobStatus.FAILED) or (job.status == JobStatus.BROKEN)
    
 
def test_job_assigned( task , **kwargs):
  """
    Assigned the fist job to test
  """
  task.jobs[0].JobStatus =  JobStatus.ASSIGNED
  return True


def test_job_running( task , **kwargs):
  """
    Check if the test job still running
  """
  return task.jobs[0].status == JobStatus.RUNNING


def test_job_completed( task , **kwargs):
  """
    Check if the test job is completed
  """
  return task.jobs[0].status == JobStatus.COMPLETED





def task_registered( task , **kwargs):
  """
    Check if all jobs into the task are registered
  """
  return all([job.status==JobStatus.REGISTERED for job in task.jobs])
  

def task_assigned( task , **kwargs):
  """
  Force all jobs with ASSIGNED status
  """
  logger.info("Assigne all jobs...")
  for job in task.jobs:
      if job.status != JobStatus.COMPLETED:
        job.status =  JobStatus.ASSIGNED
  return True


def task_completed( task , **kwargs):
  """
    Check if all jobs into the task are completed
  """
  return all([job.status==JobStatus.COMPLETED for job in task.jobs])
  

def task_running( task , **kwargs):
  """
    Check if any jobs into the task is in running state
  """
  return any([job.status==JobStatus.RUNNING] for job in task.jobs)


def task_finalized( task , **kwargs):
  """
    Check if all jobs into the task are completed or failed
  """
  completed = all([job.status==JobStatus.COMPLETED for job in task.jobs])
  failed    = all([job.status==JobStatus.FAILED for job in task.jobs])
  return (len(self.jobs) == (completed+failed))


def task_killed( task , **kwargs):
  """
    Check if all jobs into the task are killed
  """
  return all([job.status==JobStatus.KILLED for job in task.jobs])
  

def task_broken( task , **kwargs):
  """
    Broken all jobs inside of the task
  """
  for job in task.jobs:
    job.status = JobStatus.BROKEN
  return True



def trigger_task_kill( task , **kwargs):
  """
    Put all jobs to kill status when trigger
  """
  if task.trigger == TaskTrigger.KILL:
    for job in task.jobs:
      if job.status == JobStatus.RUNNING:
        job.status = JobStatus.KILL
      else:
        job.status = JobStatus.KILLED
    task.trigger = TaskTrigger.WAITING
    return True
  else:
    return False


def trigger_task_retry( task , **kwargs):
  """
    Move all jobs to registered when trigger is retry given by external order
  """
  if task.trigger == TaskTrigger.RETRY:
    for job in task.jobs:
      job.status = JobStatus.REGISTERED
    task.trigger = TaskTrigger.WAITING
    return True
  else:
    return False

 
def job_retry( task ):
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




#
# Schedule implementation
# 


class Schedule:

  def __init__(self, db, mailing):
    logger.info("Creating schedule...")
    self.database = database
    self.mailing = mailing
    self.compile()


  
  def run(self):

    logger.info("Treat jobs with status running but not alive into the executor.")
    self.treat_jobs_not_alive()

    for task in tqdm( self.database.tasks(), dect='Loop over tasks...'):
      self.pulse(task)

    logger.info("Commit all database changes.")
    self.database.commit()
    return True



  def pulse(self, task):

    # Run all JobStatus triggers to find the correct transiction
    for source, transition, target in self.states:
      # Check if the current JobStatus is equal than this JobStatus
      if source == task.status:
        answer = 
        try:
          answer = transition(task)
          if answer:
            logger.info(f"Moving task from {source} to {target} state.")
            task.status = target
            break
        except Exception as e:
          logger.error(f"Found a problem to execute the transition from {source} to {target} state.")
          traceback.print_exc()

       
  def get_n_assigned_jobs(self, njobs):
    try:
      jobs = db_api.session().query(Job).filter(  Job.status==JobStatus.ASSIGNED  ).order_by(Job.id).limit(njobs).with_for_update().all()
      jobs.reverse()
      return jobs
    except Exception as e:
      logger.error(f"Not be able to get {njobs} from database. Return an empty list to the user.")
      traceback.print_exc()
      return []


  def get_running_jobs(self):
    try:
      return self.db.session().query(Job).filter( Job.status==JobStatus.RUNNING ).with_for_update().all()
    except Exception as e:
      logger.error(f"Not be able to get running from database. Return an empty list to the user.")
      traceback.print_exc()
      return []

  
  def treat_jobs_not_alive(self):
    """
      Check if we have any dead job when start the schedule
    """
    jobs = self.get_running_jobs()
    for job in jobs:
      if not job.is_alive():
        job.status = JobStatus.ASSIGNED



  #
  # Compile the JobStatus machine
  #
  def compile(schedule):
    logger.info("Compiling all transitions...")
    self.states = [

      Transition( source=TaskStatus.REGISTERED, target=TaskStatus.TESTING    , relationship=[task_registered, test_job_assigned]        ) # ok
      Transition( source=TaskStatus.TESTING   , target=TaskStatus.TESTING    , relationship=[test_job_running]                          ) # ok                        )
      Transition( source=TaskStatus.TESTING   , target=TaskStatus.BROKEN     , relationship=[test_job_fail, task_broken, send_email]    ) # ok
      Transition( source=TaskStatus.TESTING   , target=TaskStatus.RUNNING    , relationship=[test_job_completed, task_assigned]         ) # ok
      Transition( source=TaskStatus.BROKEN    , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                        ) # ok
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.COMPLETED  , relationship=[task_completed, send_email]                ) # ok
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.FINALIZED  , relationship=[task_completed, send_email]                ) # ok
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.KILL       , relationship=[trigger_task_kill]                         )
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.RUNNING    , relationship=[task_running]                              ) # ok
      Transition( source=TaskStatus.FINALIZED , target=TaskStatus.RUNNING    , relationship='retry_all_failed_jobs'                                         )
      Transition( source=TaskStatus.KILL      , target=TaskStatus.KILLED     , relationship=[task_killed, send_email]                   ) # ok
      Transition( source=TaskStatus.KILLED    , target=TaskStatus.REGISTERED , relationship=[task_retry]                                                )
      Transition( source=TaskStatus.COMPLETED , target=TaskStatus.REGISTERED , relationship=[task_retry]                                                )

    ]

    logger.info(f"Schedule with a total of {len(self.states)} nodes into the graph.")

 