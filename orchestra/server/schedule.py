

__all__ = ["compile", "Schedule"]


from orchestra.database.models import Job
from orchestra.status import JobStatus, TaskStatus, TaskAction
from orchestra import INFO, ERROR
from sqlalchemy import and_
import traceback



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
            answer = getattr(self, question)(task) 
          except Exception as e:
            print(e)
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



  def treat_running_jobs_not_alive(self):
    print('treat_running_jobs')
    jobs = self.get_all_running_jobs()
    print(len(jobs))
    for job in jobs:
      if not job.is_alive():
        print('job not alive')
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
  # Send kill JobStatus for all jobs after the user sent the kill singal to the task db
  #
  def kill_all_jobs( self, task ):
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
  # Retry all jobs with failed JobStatus after the user sent the retry action to the task db
  #
  def retry_all_failed_jobs( self, task ):

    for job in task.jobs:
      if job.status == JobStatus.FAILED:
        if job.retry < 10:
          job.retry+=1
          job.status =  JobStatus.ASSIGNED
    task.action = TaskAction.WAITING
    return True
 

  #
  # Check if all jobs into this task were killed
  #
  def all_jobs_were_killed( self, task ):

    total = len(self.db.session().query(Job).filter( Job.taskid==task.id ).all())
    if ( len(self.db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==JobStatus.KILLED ) ).all()) == total ):
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

    total = len(self.db.session().query(Job).filter( Job.taskid==task.id ).all())
    if len(self.db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==JobStatus.REGISTERED ) ).all()) == total:
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
      if job.status != JobStatus.COMPLETED:
        job.status =  JobStatus.ASSIGNED
    return True


  #
  # Check if all jobs into this task are in completed status
  #
  def all_jobs_are_completed( self, task ):

    total = len(self.db.session().query(Job).filter( Job.taskid==task.id ).all())
    if len(self.db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.status==JobStatus.COMPLETED ) ).all()) == total:
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


  #
  # Email notification
  #


  def send_email_task_completed( self, task ):

    subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
    message = (f"The task with name {task.name} was assigned with COMPLETED JobStatus.")
    self.postman.send(subject, message)
    return True



  def send_email_task_broken( self, task ):

    subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
    message = (f"Your task with name {task.name} was set to BROKEN JobStatus.")
    self.postman.send(subject, message)
    return True
    


  def send_email_task_finalized( self, task ):

    subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
    message = (f"The task with name {task.name} was assigned with FINALIZED JobStatus.")
    self.postman.send(subject, message)
    return True



  def send_email_task_killed( self, task ):

    subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
    message = (f"The task with name {task.name} was assigned with KILLED JobStatus.")
    self.postman.send(subject, message)
    return True

















 
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
  schedule.transition( source=TaskStatus.RUNNING   , dest=TaskStatus.FINALIZED  , relationship=['all_jobs_ran','send_email_task_finalized']                    )
  schedule.transition( source=TaskStatus.RUNNING   , dest=TaskStatus.KILL       , relationship='kill_all_jobs'                                                 )
  schedule.transition( source=TaskStatus.RUNNING   , dest=TaskStatus.RUNNING    , relationship='check_not_allow_job_status_in_running_state'                   )
  schedule.transition( source=TaskStatus.FINALIZED , dest=TaskStatus.RUNNING    , relationship='retry_all_failed_jobs'                                         )
  schedule.transition( source=TaskStatus.KILL      , dest=TaskStatus.KILLED     , relationship=['all_jobs_were_killed','send_email_task_killed']               )
  schedule.transition( source=TaskStatus.KILLED    , dest=TaskStatus.REGISTERED , relationship='retry_all_jobs'                                                )
  schedule.transition( source=TaskStatus.COMPLETED , dest=TaskStatus.REGISTERED , relationship='retry_all_jobs'                                                )

