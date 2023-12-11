
__all__ = ["Schedule"]

import traceback, threading
from mlflow.tracking import MlflowClient
from sqlalchemy import or_, and_
from loguru import logger
from time import sleep
from maestro.servers.controler.postman import Postman
from maestro.models import Task, Job, Database
from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger


def update_status(app, job):
  if job.run_id != "":
    app.tracking.set_tag(job.run_id, "Status", job.status)

#
# Transitions functions
#

def send_email( app, task: Task ) -> bool:
  """
  Send an email with the task status
  """
  try:
    status = task.status
    taskname = task.name
    subject    = f"[LPS Cluster] Notification for task id {status}"
    message    = (f"The task with name {taskname} was assigned with {status} status.")
    logger.debug(f"Sending email to {task.to_email}") 
    app.postman.send(task.to_email, subject, message)
  except Exception as e:
    traceback.print_exc()
    logger.error("not possible to send email to the responsible.")
    
  return True

#
# Job test
#

def test_job_fail( app, task: Task ) -> bool:
  """
    Check if the first job returns fail
  """
  job = task.jobs[0]
  return (job.status == JobStatus.FAILED) or (job.status == JobStatus.BROKEN)
    
 
def test_job_assigned( app, task: Task ) -> bool:
  """
    Assigned the fist job to test
  """
  logger.debug("test_job_assigned")
  task.jobs[0].status =  JobStatus.ASSIGNED
  update_status(app, task.jobs[0])   
  return True


def test_job_running( app, task: Task ) -> bool:
  """
    Check if the test job still running
  """
  logger.debug(f"Job test with status {task.jobs[0].status}...")
  return task.jobs[0].status == JobStatus.RUNNING


def test_job_completed( app, task: Task ) -> bool:
  """
    Check if the test job is completed
  """
  return task.jobs[0].status == JobStatus.COMPLETED


#
# Task
#


def task_registered( app, task: Task ) -> bool:
  """
    Check if all jobs into the task are registered
  """
  logger.debug("task_registered")
  return all([job.status==JobStatus.REGISTERED for job in task.jobs])
  


def task_assigned( app, task: Task ) -> bool:
  """
  Force all jobs with ASSIGNED status
  """
  logger.debug("task_assigned")
  for job in task.jobs:
      job.status =  JobStatus.ASSIGNED
      update_status(app, job)
  return True


def task_completed( app, task: Task ) -> bool:
  """
    Check if all jobs into the task are completed
  """
  logger.debug("task_completed")
  return all([job.status==JobStatus.COMPLETED for job in task.jobs])
  

def task_running( app, task: Task ) -> bool:
  """
    Check if any jobs into the task is in assigned state
  """
  logger.debug("task_running")
  return any([ ((job.status==JobStatus.ASSIGNED) or (job.status==JobStatus.RUNNING))  for job in task.jobs])


def task_finalized( app, task: Task ) -> bool:
  """
    Check if all jobs into the task are completed or failed
  """
  logger.debug("task_finalized")
  # NOTE: We have jobs waiting to be executed here. Task should be in running state  
  return (not task_running(app, task)) and (not all([job.status==JobStatus.COMPLETED for job in task.jobs]) )



def task_killed( app, task: Task ) -> bool:
  """
    Check if all jobs into the task are killed
  """
  logger.debug("task_killed")
  return all([job.status==JobStatus.KILLED for job in task.jobs])
  

def task_broken( app, task: Task ) -> bool:
  """
    Broken all jobs inside of the task
  """
  logger.debug("task_broken")
  return all([job.status==JobStatus.BROKEN for job in task.jobs])


def task_retry( app, task: Task ) -> bool:
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
        update_status(app, job)

  # NOTE: If we have jobs to retry we must keep the current state and dont finalized the task
  return not retry_jobs>0



def task_removed( app, task: Task ):
  """
    Check if task removed
  """
  logger.debug("task_removed")
  return task.to_remove
  

def task_kill( app, task: Task ):
  """
    Kill all jobs
  """
  logger.info("task_kill")
  for job in task.jobs:
    if job.status == JobStatus.RUNNING:
      job.status = JobStatus.KILL
    else:
      job.status = JobStatus.KILLED
    update_status(app, job)

  return True


#
# Triggers
#


def trigger_task_kill( app, task: Task ) -> bool:
  """
    Put all jobs to kill status when trigger
  """
  logger.debug("trigger_task_kill")
  if task.trigger == TaskTrigger.KILL:
    task.trigger = TaskTrigger.WAITING
    return True
  else:
    return False


def trigger_task_retry( app, task: Task ) -> bool:
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
          update_status(app, job)

    elif (task.status == TaskStatus.KILLED) or (task.status == TaskStatus.BROKEN):
      for job in task.jobs:
        job.status = JobStatus.REGISTERED
        update_status(app, job)

    else:
      logger.error(f"Not expected task status ({task.status})into the task retry. Please check this!")
      return False
    
    task.trigger = TaskTrigger.WAITING
    return True
  else:
    return False



def trigger_task_delete( app, task: Task ) -> bool:
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

  def __call__(self, app, task: Task) -> bool:   
    """
      Apply the transition for each function
    """
    for func in self.relationship:
      if not func(app, task):
        return False
    return True


#
# Schedule implementation
# 
class Schedule(threading.Thread):

  def __init__(self, task_id, db):
    threading.Thread.__init__(self)
    logger.info("Creating schedule...")
    self.task_id  = task_id
    self.db       = Database(db.host)
    self.__stop   = threading.Event()
    with self.db as session:
      tracking_url  = session.get_environ( "TRACKING_SERVER_URL" )
      self.tracking = MlflowClient( tracking_url )
      email         = session.get_environ( "POSTMAN_EMAIL_FROM" )
      password      = session.get_environ( "POSTMAN_EMAIL_PASSWORD" ) 
      self.postman  = Postman(email,password)

    self.compile()

    
  def stop(self):
    logger.info("stopping service")
    self.__stop.set()


  def run(self):
    while (not self.__stop.isSet()):
      sleep(1)
      self.loop()


  def loop(self):

    try:
      with self.db as session:
        logger.debug("Treat jobs with status running but not alive into the executor.")
        # NOTE: Check if we have some job with running but not alive. If yes, return it to assigne status
        jobs = session().query(Job).filter( and_(Job.taskid==self.task_id, or_(Job.status==JobStatus.RUNNING, Job.status==JobStatus.PENDING)) ).with_for_update().all()
        for job in jobs:
          if not job.is_alive():
            job.status = JobStatus.ASSIGNED
            update_status(self, job)
        session.commit()
    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return False
      
    # Update task states
    try:
      with self.db as session:
        # NOTE: All tasks assigned to remove should not be returned by the database.
        #task = session().query(Task).filter(Task.status!=TaskStatus.REMOVED).with_for_update().all()
        task = session().query(Task).filter(Task.id==self.task_id).with_for_update().first()
        logger.debug(f"task in {task.status} status.")
        # Run all JobStatus triggers to find the correct transiction
        for state in self.states:
          # Check if the current JobStatus is equal than this JobStatus
          if state.source == task.status:
            try:
              res = state( self, task)
              if res:
                logger.debug(f"Moving task from {state.source} to {state.target} state.")
                task.status = state.target
                break
            except Exception as e:
              logger.error(f"Found a problem to execute the transition from {state.source} to {state.target} state.")
              traceback.print_exc()
              return False
        session.commit()

        if task.status == TaskStatus.REMOVED:
          logger.info("removing task with name {self.taskname} from database")
          session().query(Job).filter(Job.taskid==self.task_id).delete()
          session().query(Task).filter(Task.id==self.task_id).delete()
          session.commit()
          self.stop()

    except Exception as e:
      traceback.print_exc()
      logger.error(e)
      return False

    logger.debug("Commit all database changes.")
    return True


  #
  # Compile the JobStatus machine
  #
  def compile(self):

    logger.info("Compiling all transitions...")
    self.states = [

      Transition( source=TaskStatus.REGISTERED, target=TaskStatus.TESTING    , relationship=[task_registered, test_job_assigned]       ),
      Transition( source=TaskStatus.TESTING   , target=TaskStatus.TESTING    , relationship=[test_job_running]                         ),
      Transition( source=TaskStatus.TESTING   , target=TaskStatus.BROKEN     , relationship=[test_job_fail, task_broken, send_email]   ), 
      Transition( source=TaskStatus.TESTING   , target=TaskStatus.RUNNING    , relationship=[test_job_completed, task_assigned]        ), 
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
      Transition( source=TaskStatus.KILLED    , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.KILLED    , target=TaskStatus.REMOVED    , relationship=[task_removed]                             ),
      Transition( source=TaskStatus.COMPLETED , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.FINALIZED , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.BROKEN    , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.REGISTERED, target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
      Transition( source=TaskStatus.COMPLETED , target=TaskStatus.REMOVED    , relationship=[trigger_task_delete]                      ),
    ]

    logger.info(f"Schedule with a total of {len(self.states)} nodes into the graph.")

 

