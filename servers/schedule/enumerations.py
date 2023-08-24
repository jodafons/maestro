
from enum import Enum


class TaskStatus:

    UNKNOWN    = 'Unknown'
    HOLD       = "Hold"
    HOLDED     = "Holded"
    BROKEN     = "Broken"
    FAILED     = "Failed"
    KILL       = "Kill"
    KILLED     = "Killed"
    COMPLETED  = "Completed"
    REGISTERED = "Registered"
    TESTING    = "Testing"
    ASSIGNED   = "Assigned"
    ACTIVATED  = "Activated"
    PENDING    = "Pending"
    STARTING   = "Starting"
    RUNNING    = "Running"
    FINALIZED  = "Finalized"
    REMOVED    = "Removed"


#
# Task order
#
class TaskAction:
    RETRY      = "Retry"
    KILL       = "Kill"
    WAITING    = "Waiting"
    DELETE     = "Delete"

