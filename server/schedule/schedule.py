



__all__ = ["compile", "Schedule"]

from database import Job

from sqlalchemy import and_
import traceback
from . import *











def broken_all_jobs( task ):
    for job in task.jobs:
        job.state = BROKEN
    task.order = WAITING
    return True
   



#
# Retry all jobs after the user sent the retry
#
def retry_all_jobs( task ):

    if task.order == RETRY:
        for job in task.jobs:
            job.status = REGISTERED
        task.order = WAITING
        return True
    else:
        return False




#
# Retry all jobs with failed state after the user sent the retry signal to the task db
#
def retry_all_failed_jobs( task ):
    for job in task.jobs:
      if job.status == FAILED:
          if job.retry < 3:
              job.retry+=1
              job.status = ASSIGNED
    task.order = WAITING
    return True
   




#
# Send kill state for all jobs after the user sent the kill singal to the task db
#
def kill_all_jobs( task ):
  
    if task.order == KILL:
        for job in task.jobs:
            if job.status != RUNNING:
                job.status =  KILLED
            else:
                job.status = KILL
        task.order = WAITING
        return True
    else:
      return False
   



#
# Check if all jobs into this task were killed
#
def all_jobs_were_killed( task, db ):
    total = len(db.session.query(Job).filter( Job.taskid==task.id ).all())
    if ( len(db.session.query(Job).filter( and_ ( Job.taskid==task.id, Job.status==KILLED ) ).all()) == total ):
      return True
    else:
      return False
  



  #
  # Check if the test job is completed
  #
  def test_job_pass( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.jobs[0]
      if job.state == State.COMPLETED:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Check if the test job still running
  #
  def test_job_still_running( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.jobs[0]
      if job.state == State.RUNNING:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False


  #
  # Check if the test job fail
  #
  def test_job_fail( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.jobs[0]
      if job.state == State.FAILED or job.state == State.BROKEN:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False





  #
  # Assigned the first job in the list to test
  #
  def assigned_one_job_to_test( self, task ):

    try:
      job = task.jobs[0]
      job.state =  State.ASSIGNED
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Assigned all jobs
  #
  def assigned_all_jobs( self, task ):

    try:
      for job in task.jobs:
        if job.state != State.COMPLETED:
          job.state =  State.ASSIGNED
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



#
# Check if all jobs into this task are in COMPLETED state
def all_jobs_are_completed( task ):
    total = len(db.session().query(Job).filter( Job.taskid==task.id ).all())
    if len(db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.state==COMPLETED ) ).all()) == total:
        return True
    else:
        return False
 



  #
  # Check if all jobs into this task ran
  #
  def all_jobs_ran( self, task ):

    try:                                                                                                                                                                 
      total = len(self.__db.session().query(Job).filter( Job.taskid==task.id ).all())
      total_COMPLETED = len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.state==State.COMPLETED ) ).all())
      total_failed = len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.state==State.FAILED ) ).all())

      if (total_COMPLETED + total_failed) == total:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False


  #
  # Check if all jobs into this task ran
  #
  def check_not_allow_job_state_in_running_state( self, task ):

    try:
      exist_registered_jobs = False
      for job in task.jobs:
        if job.state==State.REGISTERED or job.state==State.PENDING: 
          job.state = State.ASSIGNED
          exist_registered_jobs=True

      return exist_registered_jobs
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False






  #
  # Set delete signal
  #
  def send_delete_signal(self, task):

    task.signal =Signal.DELETE
    return True


  #
  # Assigned task to removed state and remove all objects from the database and store
  #
  def remove_this_task(self, task):

    if task.signal == Signal.DELETE:
      try:
        from orchestra.parsers import TaskParser
        helper = TaskParser(self.__db)
        helper.delete(task.taskname,False)
        return True
      except Exception as e:
        task.signal =Signal.WAITING
        task.state = State.REMOVED
        MSG_ERROR(self, e)
        MSG_ERROR(self, "It's not possible to delete this task with name %s", task.taskname)
        return False
    else:
      return False




  #
  #
  # Email notification
  #
  #


  def send_email_task_COMPLETED( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
      message = ("The task with name %s was assigned with COMPLETED State.")%(task.taskname)
      self.__postman.send(subject, message)
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR(self, "It's not possible to send the email to %s", self.__postman.email)
      return True


  def send_email_task_broken( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
      message = ("Your task with name %s was set to BROKEN State.")%(task.taskname)
      self.__postman.send(subject, message)
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR(self, "It's not possible to send the email to %s", self.__postman.email)
      return True


  def send_email_task_finalized( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
      message = ("The task with name %s was assigned with FINALIZED State.")%(task.taskname)
      self.__postman.send(subject, message)
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR(self, "It's not possible to send the email to %s", self.__postman.email)
      return True


  def send_email_task_killed( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
      message = ("The task with name %s was assigned with KILLED State.")%(task.taskname)
      self.__postman.send(subject, message)
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR(self, "It's not possible to send the email to %s", self.__postman.email)
      return True







def kill_all_jobs(task):
    if task.order == KILL:
        for job in task.jobs:
            if job.status != RUNNING:
                job.status = KILLED
            else:
                job.status = KILL
        task.order = WAITING
        return True
    else:
        return False


def assigned_all_jobs( task ):
    for job in task.jobs:
        if job.status != COMPLETED:
          job.status =  ASSIGNED
    return True
 

def all_jobs_are_registered( task, db ):
    total = len(db.session().query(Job).filter( Job.taskid==task.id ).all())
    if len(db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==REGISTERED ) ).all()) == total:
        return True
    else:
        return False







def compile(schedule):

  # Create the state machine
  schedule.transition( source=REGISTERED, dest=TESTING    , func=all_jobs_are_registered                              )
  schedule.transition( source=TESTING   , dest=RUNNING    , func=assigned_all_jobs                                    )
  schedule.transition( source=BROKEN    , dest=REGISTERED , func=retry_all_jobs                                         )
  schedule.transition( source=RUNNING   , dest=COMPLETED  , func=[all_jobs_are_COMPLETED, send_email_task_COMPLETED]  )
  schedule.transition( source=RUNNING   , dest=FINALIZED  , func=[all_jobs_ran,send_email_task_finalized]             )
  schedule.transition( source=RUNNING   , dest=KILL       , func=kill_all_jobs                                          )
  schedule.transition( source=RUNNING   , dest=RUNNING    , func=check_not_allow_job_state_in_running_state             )
  schedule.transition( source=FINALIZED , dest=RUNNING    , func=retry_all_failed_jobs                                  )
  schedule.transition( source=KILL      , dest=KILLED     , func=[all_jobs_were_killed,send_email_task_killed]        )
  schedule.transition( source=KILLED    , dest=REGISTERED , func=retry_all_jobs                                         )
  schedule.transition( source=COMPLETED , dest=REGISTERED , func=retry_all_jobs                                         )

