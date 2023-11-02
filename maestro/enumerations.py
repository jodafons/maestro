
#from enum import Enum
__all__ = ["JobStatus", "TaskStatus", "TaskTrigger", "job_status"]


class JobStatus:

    REGISTERED = "Registered"
    PENDING    = "Pending"
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
    REMOVED    = "Removed"
    UNKNOWN    = 'Unknown'

#
# Task order
#
class TaskTrigger:
    RETRY      = "Retry"
    KILL       = "Kill"
    WAITING    = "Waiting"
    DELETE     = "Delete"



job_status = [JobStatus.REGISTERED, JobStatus.ASSIGNED , JobStatus.PENDING, 
              JobStatus.RUNNING   , JobStatus.COMPLETED, JobStatus.FAILED, 
              JobStatus.KILL      , JobStatus.KILLED   , JobStatus.BROKEN]