__all__ = ["remove_extension","test_locally"]




from . import DeviceParser
__all__.extend( DeviceParser.__all__ )
from .DeviceParser import *

from . import PilotParser
__all__.extend( PilotParser.__all__ )
from .PilotParser import *


def remove_extension(f, extensions="json|h5|pic|gz|tgz|csv"):
  for ext in extensions.split("|"):
    if f.endswith('.'+ext):
      return f.replace('.'+ext, '')
  return f



def test_locally( job_db ):

  from orchestra.server.consumer import Job
  from orchestra.server import Slot
  from orchestra.status import JobStatus
  job = Job( job_db, Slot(), extra_envs={'ORCHESTRA_LOCAL_TEST':'1'})
  job.slot.enable()
  job.db().status = JobStatus.PENDING
  while True:
      if job.status() == JobStatus.PENDING:
          if not job.run():
            return False
      elif job.status() == JobStatus.FAILED:
          return False
      elif job.status() == JobStatus.RUNNING:
          continue
      elif job.status() == JobStatus.COMPLETED:
          job_db.status=JobStatus.REGISTERED
          return True
      else:
          continue


from . import TaskParser
__all__.extend( TaskParser.__all__ )
from .TaskParser import *



