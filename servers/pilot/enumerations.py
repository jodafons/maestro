
from enum import Enum


class JobStatus(Enum):

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



class TaskStatus(Enum):

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
class TaskTrigger(Enum):
    RETRY      = "Retry"
    KILL       = "Kill"
    WAITING    = "Waiting"
    DELETE     = "Delete"

