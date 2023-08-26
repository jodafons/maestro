
import traceback, time, os

from models import Task, Job
from sqlalchemy import and_
from loguru import logger
from enumerations import JobStatus, TaskStatus, TaskTrigger
from tqdm import tqdm



class Transition:

  def __init__(self, source: JobStatus , target: JobStatus , relationship: list ):
    self.source = source
    self.target = target
    self.relationship = relationship

  def __call__(self, task: Task, **kwargs) -> bool:   
    """
      Apply the transition for each function
    """
    for func in self.relationship:
      if not func(task, **kwargs):
        return False
    return True

#
# Transitions functions
#

def send_email( task: Task , **kwargs) -> bool:
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

#
# Job test
#

def test_job_fail( task: Task, **kwargs) -> bool:
  """
    Check if the first job returns fail
  """
  job = task.jobs[0]
  return (job.status == JobStatus.FAILED) or (job.status == JobStatus.BROKEN)
    
 
def test_job_assigned( task: Task , **kwargs) -> bool:
  """
    Assigned the fist job to test
  """
  task.jobs[0].JobStatus =  JobStatus.ASSIGNED
  return True


def test_job_running( task: Task , **kwargs) -> bool:
  """
    Check if the test job still running
  """
  return task.jobs[0].status == JobStatus.RUNNING


def test_job_completed( task: Task , **kwargs) -> bool:
  """
    Check if the test job is completed
  """
  return task.jobs[0].status == JobStatus.COMPLETED


#
# Task
#


def task_registered( task: Task , **kwargs) -> bool:
  """
    Check if all jobs into the task are registered
  """
  return all([job.status==JobStatus.REGISTERED for job in task.jobs])
  

def task_assigned( task: Task , **kwargs) -> bool:
  """
  Force all jobs with ASSIGNED status
  """
  for job in task.jobs:
      job.status =  JobStatus.ASSIGNED
  return True


def task_completed( task: Task , **kwargs) -> bool:
  """
    Check if all jobs into the task are completed
  """
  return all([job.status==JobStatus.COMPLETED for job in task.jobs])
  

def task_running( task: Task , **kwargs) -> bool:
  """
    Check if any jobs into the task is in assigned state
  """
  return any([job.status==JobStatus.ASSIGNED] for job in task.jobs)


def task_finalized( task: Task , **kwargs) -> bool:
  """
    Check if all jobs into the task are completed or failed
  """
  # NOTE: We have jobs waiting to be executed here. Task should be in running state  
  return (not task_running(task)) and (not all([job.status==JobStatus.COMPLETED for job in task.jobs]) )



def task_killed( task: Task , **kwargs) -> bool:
  """
    Check if all jobs into the task are killed
  """
  return all([job.status==JobStatus.KILLED for job in task.jobs])
  

def task_broken( task: Task , **kwargs) -> bool:
  """
    Broken all jobs inside of the task
  """
  for job in task.jobs:
    job.status = JobStatus.BROKEN
  return True


def task_retry( task: Task , **kwargs) -> bool:
  """
    Retry all jobs inside of the task with failed status
  """
  retry_jobs = 0
  for job in task.jobs:
    if job.status != JobStatus.COMPLETED:
      if job.retry < 5:
        job.status = JobStatus.ASSIGNED
        job.retry +=1
        retry_jobs +=1
  # NOTE: If we have jobs to retry we must keep the current state and dont finalized the task
  return not retry_jobs>0

#
# Triggers
#


def trigger_task_kill( task: Task , **kwargs) -> bool:
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


def trigger_task_retry( task: Task , **kwargs) -> bool:
  """
    Move all jobs to registered when trigger is retry given by external order
  """
  if task.trigger == TaskTrigger.RETRY:

    if task.status == TaskStatus.FINALIZED:
      for job in task.jobs:
        if (job.status != JobStatus.COMPLETED):
          job.status = JobStatus.ASSIGNED
          job.retry  = 0 
    elif (task.status == TaskStatus.KILLED) or (task.status == TaskStatus.BROKEN):
      for job in task.jobs:
        job.status = JobStatus.REGISTERED
    else:
      logger.error(f"Not expected task status ({task.status})into the task retry. Please check this!")
      return False
    
    task.trigger = TaskTrigger.WAITING
    return True
  else:
    return False

 
def job_retry( task: Task ):
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

  def __init__(self, db, mailing, extended_states : bool=False):
    logger.info("Creating schedule...")
    self.mailing = mailing
    self.db = db
    self.extended_states = extended_states
    self.compile()


  def run(self):

    logger.info("Treat jobs with status running but not alive into the executor.")
    self.treat_jobs_not_alive()

    for task in tqdm( self.db.tasks(), desc='Loop over tasks...'):
      
      # Run all JobStatus triggers to find the correct transiction
      for source, transition, target in self.states:
        # Check if the current JobStatus is equal than this JobStatus
        if source == task.status:
          try:
            answer = transition(task)
            if answer:
              logger.info(f"Moving task from {source} to {target} state.")
              task.status = target
              break
          except Exception as e:
            logger.error(f"Found a problem to execute the transition from {source} to {target} state.")
            traceback.print_exc()
            return False

    logger.info("Commit all database changes.")
    self.db.commit()
    return True


    

       
  def get_n_assigned_jobs(self, njobs):
    try:
      jobs = self.db.session().query(Job).filter(  Job.status==JobStatus.ASSIGNED  ).order_by(Job.id).limit(njobs).with_for_update().all()
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
  def compile(self):

    logger.info("Compiling all transitions...")
    self.states = [

      Transition( source=TaskStatus.BROKEN    , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                       ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.COMPLETED  , relationship=[task_completed, send_email]               ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.FINALIZED  , relationship=[task_finalized, task_retry, send_email]   ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.KILL       , relationship=[trigger_task_kill]                        ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.RUNNING    , relationship=[task_running]                             ),
      Transition( source=TaskStatus.FINALIZED , target=TaskStatus.RUNNING    , relationship=[trigger_task_retry]                       ),
      Transition( source=TaskStatus.KILL      , target=TaskStatus.KILLED     , relationship=[task_killed, send_email]                  ),
      Transition( source=TaskStatus.KILLED    , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                       ),
      Transition( source=TaskStatus.COMPLETED , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                       ),
    
    ]

  

    if self.extended_states:
      logger.info("Adding test states into the graph.")

      self.states.extend( [
          Transition( source=TaskStatus.REGISTERED, target=TaskStatus.TESTING    , relationship=[task_registered, test_job_assigned]        ),
          Transition( source=TaskStatus.TESTING   , target=TaskStatus.TESTING    , relationship=[test_job_running]                          ),
          Transition( source=TaskStatus.TESTING   , target=TaskStatus.BROKEN     , relationship=[test_job_fail, task_broken, send_email]    ), 
          Transition( source=TaskStatus.TESTING   , target=TaskStatus.RUNNING    , relationship=[test_job_completed, task_assigned]         ), 
        ] )
      
    else:
      logger.info("Bypassing the testing state in the graph")

      self.states.extend( 
        [
          Transition( source=TaskStatus.REGISTERED, target=TaskStatus.TESTING    , relationship=[task_registered]        ) ,
          Transition( source=TaskStatus.TESTING   , target=TaskStatus.RUNNING    , relationship=[task_assigned]          ) ,
        ]
      )


    logger.info(f"Schedule with a total of {len(self.states)} nodes into the graph.")

 


if __name__ == "__main__":
  

  host = os.environ['DATABASE_SERVER_HOST']
  from sqlalchemy import create_engine 
  from sqlalchemy.orm import sessionmaker
  from api.database import postgres_client, Base, Task, Job


  # prepare database
  engine = create_engine(host)
  Session = sessionmaker(bind=engine)
  session = Session()
  Base.metadata.drop_all(engine)
  Base.metadata.create_all(engine)
  session.commit()
  session.close()

  app = Schedule(host)
