
from enum import Enum



class JobStatus(Enum):

    UNKNOWN    = 'Unknown'
    REGISTERED = "Registered"
    ASSIGNED   = "Assigned"
    TESTING    = "Testing"
    BROKEN     = "Broken"
    FAILED     = "Failed"
    KILL       = "Kill"
    KILLED     = "Killed"
    COMPLETED  = "Completed"
    PENDING    = "Pending"
    RUNNING    = "Running"