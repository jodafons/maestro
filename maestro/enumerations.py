
#from enum import Enum


class JobStatus:

    REGISTERED = "Registered"
    TESTING    = "Testing"
    ASSIGNED   = "Assigned"
    RUNNING    = "Running"
    COMPLETED  = "Completed"
    BROKEN     = "Broken"
    FAILED     = "Failed"
    KILL       = "Kill"
    KILLED     = "Killed"
    UNKNOWN    = 'Unknown'



class TaskStatus:

    REGISTERED = "Registered"
    TESTING    = "Testing"
    RUNNING    = "Running"
    FINALIZED  = "Finalized"
    COMPLETED  = "Completed"
    KILL       = "Kill"
    KILLED     = "Killed"
    BROKEN     = "Broken"
    UNKNOWN    = 'Unknown'

#
# Task order
#
class TaskTrigger:
    RETRY      = "Retry"
    KILL       = "Kill"
    WAITING    = "Waiting"
    DELETE     = "Delete"



job_status = [JobStatus.REGISTERED, JobStatus.ASSIGNED , JobStatus.TESTING, 
              JobStatus.RUNNING   , JobStatus.COMPLETED, JobStatus.FAILED, 
              JobStatus.KILL      , JobStatus.KILLED   , JobStatus.BROKEN]