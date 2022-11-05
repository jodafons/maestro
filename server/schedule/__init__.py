

#__all__ = []
#from . import database
#__all__.extend(database.__all__)
#from .database import *
#from . import mailing
#__all__.extend(mailing.__all__)
#from .mailing import *


UNKNOWN     = 'Unknown'


#
# Task status
#
REGISTERED  = "Registered"
TESTING     = "Testing"
FINALIZED   = "Finalized"

#
# Job status
#
PENDING     = 'Pending'
ASSIGNED    = 'Assigned'
RUNNING     = 'Running'
COMPLETED   = 'Completed'
BROKEN      = 'Broken'
FAILED      = 'Failed'
KILL        = 'Kill'
KILLED      = 'Killed'


#
# Task order
#
WAITING     = "Waiting"
RETRY       = "Retry"
