
__all__ = ["Schedule", "schedule_args"]

import traceback, time, os, threading
from mlflow.tracking import MlflowClient
from sqlalchemy import and_, or_
from loguru import logger
from tqdm import tqdm
from time import sleep, time
from maestror.models import Task, Job
from maestror.enumerations import JobStatus, TaskStatus, TaskTrigger


#
# NOTE: Use this class to configure some parameters used in function
#
class schedule_args:
  tracking_url  : str=""
  from_email    : str=""
  password      : str=""
  to_email      : str=""



def update_status(job):
  client = MlflowClient( schedule_args.tracking_url )
  client.set_tag(job.run_id, "Status", job.status)

#
# Transitions functions
#

def send_email( task: Task ) -> bool:
  """
  Send an email with the task status
  """
  try:
    status = task.status
    taskname = task.name
    from_email = schedule_args.email_from
    password   = schedule_args.email_password
    to_email   = schedule_args.email_to
    postman    = Postman(from_email, password)
    subject    = f"[LPS Cluster] Notification for task id {status}"
    message    = (f"The task with name {taskname} was assigned with {status} status.")
    logger.debug(f"Sending email to {email}") 
    postman.send(to_email, subject, message)
  except:
    logger.error("not possible to send email to the responsible.")
    
  return True

#
# Job test
#

def test_job_fail( task: Task ) -> bool:
  """
    Check if the first job returns fail
  """
  job = task.jobs[0]
  return (job.status == JobStatus.FAILED) or (job.status == JobStatus.BROKEN)
    
 
def test_job_assigned( task: Task ) -> bool:
  """
    Assigned the fist job to test
  """
  logger.debug("test_job_assigned")
  task.jobs[0].status =  JobStatus.ASSIGNED
  update_status(task.jobs[0])   
  return True


def test_job_running( task: Task ) -> bool:
  """
    Check if the test job still running
  """
  logger.debug(f"Job test with status {task.jobs[0].status}...")
  return task.jobs[0].status == JobStatus.RUNNING


def test_job_completed( task: Task ) -> bool:
  """
    Check if the test job is completed
  """
  return task.jobs[0].status == JobStatus.COMPLETED


#
# Task
#


def task_registered( task: Task ) -> bool:
  """
    Check if all jobs into the task are registered
  """
  logger.debug("task_registered")
  return all([job.status==JobStatus.REGISTERED for job in task.jobs])
  

def task_assigned( task: Task ) -> bool:
  """
  Force all jobs with ASSIGNED status
  """
  logger.debug("task_assigned")
  for job in task.jobs:
      job.status =  JobStatus.ASSIGNED
      update_status(job)
  return True


def task_completed( task: Task ) -> bool:
  """
    Check if all jobs into the task are completed
  """
  logger.debug("task_completed")
  return all([job.status==JobStatus.COMPLETED for job in task.jobs])
  

def task_running( task: Task ) -> bool:
  """
    Check if any jobs into the task is in assigned state
  """
  logger.debug("task_running")
  return any([ ((job.status==JobStatus.ASSIGNED) or (job.status==JobStatus.RUNNING))  for job in task.jobs])


def task_finalized( task: Task ) -> bool:
  """
    Check if all jobs into the task are completed or failed
  """
  logger.debug("task_finalized")
  # NOTE: We have jobs waiting to be executed here. Task should be in running state  
  return (not task_running(task)) and (not all([job.status==JobStatus.COMPLETED for job in task.jobs]) )



def task_killed( task: Task ) -> bool:
  """
    Check if all jobs into the task are killed
  """
  logger.debug("task_killed")
  return all([job.status==JobStatus.KILLED for job in task.jobs])
  

def task_broken( task: Task ) -> bool:
  """
    Broken all jobs inside of the task
  """
  logger.debug("task_broken")
  return all([job.status==JobStatus.BROKEN for job in task.jobs])


def task_retry( task: Task ) -> bool:
  """
    Retry all jobs inside of the task with failed status
  """
  logger.debug("task_retry")
  retry_jobs = 0
  for job in task.jobs:
    if job.status != JobStatus.COMPLETED:
      if job.retry < 5:
        job.status = JobStatus.ASSIGNED
        job.retry +=1
        retry_jobs +=1
        update_status(job)

  # NOTE: If we have jobs to retry we must keep the current state and dont finalized the task
  return not retry_jobs>0



def task_removed( task: Task ):
  """
    Check if task removed
  """
  logger.debug("task_removed")
  return task.to_remove
  

def task_kill( task: Task ):
  """
    Kill all jobs
  """
  logger.info("task_kill")
  for job in task.jobs:
    if job.status == JobStatus.RUNNING:
      job.status = JobStatus.KILL
    else:
      job.status = JobStatus.KILLED
    update_status(job)

  return True


#
# Triggers
#


def trigger_task_kill( task: Task ) -> bool:
  """
    Put all jobs to kill status when trigger
  """
  logger.debug("trigger_task_kill")
  if task.trigger == TaskTrigger.KILL:
    task.trigger = TaskTrigger.WAITING
    return True
  else:
    return False


def trigger_task_retry( task: Task ) -> bool:
  """
    Move all jobs to registered when trigger is retry given by external order
  """
  logger.debug("trigger_task_retry")
  if task.trigger == TaskTrigger.RETRY:

    if task.status == TaskStatus.FINALIZED:
      for job in task.jobs:
        if (job.status != JobStatus.COMPLETED):
          job.status = JobStatus.ASSIGNED
          job.retry  = 0 
          update_status(job)

    elif (task.status == TaskStatus.KILLED) or (task.status == TaskStatus.BROKEN):
      for job in task.jobs:
        job.status = JobStatus.REGISTERED
        update_status(job)

    else:
      logger.error(f"Not expected task status ({task.status})into the task retry. Please check this!")
      return False
    
    task.trigger = TaskTrigger.WAITING
    return True
  else:
    return False



def trigger_task_delete( task: Task ) -> bool:
  """
    Put all jobs to kill status when trigger
  """
  logger.debug("trigger_task_delete")
  if task.trigger == TaskTrigger.DELETE:
    task.remove()
    if task.status == TaskStatus.RUNNING:
      task.kill()
    return True
  else:
    return False


#
# Transition
#

class Transition:

  def __init__(self, source: JobStatus , target: JobStatus , relationship: list ):
    self.source = source
    self.target = target
    self.relationship = relationship

  def __call__(self, task: Task) -> bool:   
    """
      Apply the transition for each function
    """
    for func in self.relationship:
      if not func(task):
        return False
    return True


#
# Schedule implementation
# 


class Schedule(threading.Thread):

  def __init__(self, db, extended_states : bool=True):
    threading.Thread.__init__(self)
    logger.info("Creating schedule...")
    self.extended_states = extended_states
    self.db              = db
    self.compile()
    self.__stop    = threading.Event()
    

  def stop(self):
    self.__stop.set()


  def run(self):
    while (not self.__stop.isSet()):
      sleep(10)
      self.loop()


  def loop(self):

    #
    # treat dead jobs
    #
    try:
      with self.db as session:
        logger.debug("Treat jobs with status running but not alive into the executor.")
        # NOTE: Check if we have some job with running but not alive. If yes, return it to assigne status
        jobs = session().query(Job).filter( or_(Job.status==JobStatus.RUNNING, Job.status==JobStatus.PENDING) ).with_for_update().all()
        for job in jobs:
          if not job.is_alive():
            job.status = JobStatus.ASSIGNED
            update_status(job)

        session.commit()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return False
      
    #
    # Update task states
    #
    with self.db as session:

      # NOTE: All tasks assigned to remove should not be returned by the database.
      tasks = session().query(Task).filter(Task.status!=TaskStatus.REMOVED).with_for_update().all()

      for task in tasks:

        logger.debug(f"task in {task.status} status.")
        # Run all JobStatus triggers to find the correct transiction
        for state in self.states:
          # Check if the current JobStatus is equal than this JobStatus
          if state.source == task.status:
            try:
              res = state(task)
              if res:
                logger.debug(f"Moving task from {state.source} to {state.target} state.")
                task.status = state.target
                break
            except Exception as e:
              logger.error(f"Found a problem to execute the transition from {state.source} to {state.target} state.")
              traceback.print_exc()
              return False
              
      session.commit()

    #
    # remove tasks
    #
    with self.db as session:
      tasks = session().query(Task).filter(Task.status==TaskStatus.REMOVED).all()
      for task in tasks:
        logger.info(f"delete task {task.id}...")
        session().query(Job).filter(Job.taskid==task.id).delete()
        session().query(Task).filter(Task.id==task.id).delete()
        session.commit()



    logger.debug("Commit all database changes.")
    return True


  def get_jobs(self, partition : str, n: int):
    with self.db as session:
      try:
        jobs = session().query(Job).filter(  Job.status==JobStatus.ASSIGNED  ).filter( Job.partition==partition).order_by(Job.id).limit(n).all()
        jobs.reverse()
        return [job.id for job in jobs]
      except Exception as e:
        logger.error(f"Not be able to get {njobs} from database. Return an empty list to the user.")
        traceback.print_exc()
        return []


  #
  # Compile the JobStatus machine
  #
  def compile(self):

    logger.info("Compiling all transitions...")
    self.states = [

      Transition( source=TaskStatus.BROKEN    , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                       ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.COMPLETED  , relationship=[task_completed, send_email]               ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.BROKEN     , relationship=[task_broken, send_email]                  ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.FINALIZED  , relationship=[task_finalized, task_retry, send_email]   ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.KILL       , relationship=[trigger_task_kill, task_kill]             ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.KILL       , relationship=[trigger_task_delete, task_kill]           ),      
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.RUNNING    , relationship=[task_running]                             ),
      Transition( source=TaskStatus.FINALIZED , target=TaskStatus.RUNNING    , relationship=[trigger_task_retry]                       ),
      Transition( source=TaskStatus.KILL      , target=TaskStatus.KILLED     , relationship=[task_killed, send_email]                  ),
      Transition( source=TaskStatus.KILLED    , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                       ),
      Transition( source=TaskStatus.COMPLETED , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                       ),
      
      # NOTE: removed by trigger or when the task is in running state and go to killed status
      Transition( source=TaskStatus.KILLED    , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.KILLED    , target=TaskStatus.REMOVED    , relationship=[task_removed]                             ),
      
      
      Transition( source=TaskStatus.COMPLETED , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.FINALIZED , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.BROKEN    , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.REGISTERED, target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.COMPLETED , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
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

 

