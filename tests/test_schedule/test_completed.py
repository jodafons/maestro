

from maestro.task_parser import task_parser
from maestro.database_parser import database_parser
from maestro.api.clients import postgres
from maestro.enumerations import TaskStatus

from servers.schedule.schedule import Schedule
from servers.executor.consumer import Consumer


from time import sleep
import pytest
import unittest
import os, json, socket
import tempfile

program = """

import argparse
import sys,os
import traceback
import json
import time

parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()

parser.add_argument('-j','--job', action='store',
        dest='job', required = True,
            help = "The job config file.")

if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()
#print('Starting job...')
job  = json.load(open(args.job, 'r'))
sort = job['sort']
time.sleep(1)
#print('Finish job...')
"""

NUMBER_OF_JOBS       = 5
NUMBER_OF_SLOTS      = 4
HOSTNAME             = os.environ["HOSTNAME"]
DATABASE_HOST_SERVER = os.environ["DATABASE_SERVER_HOST"]
TASK_NAME            = 'test.server'
EMAIL                = 'jodafons@lps.ufrj.br'
IMAGE                = ""

class test_completed(unittest.TestCase):

    basepath      = tempfile.mkdtemp()

    @pytest.mark.order(1)
    def test_prepare_database(self):
        sleep(1)
        parser = database_parser( DATABASE_HOST_SERVER )
        assert parser.recreate()
        

    @pytest.mark.order(2)
    def test_prepare_jobs(self):

        # create jobs
        os.makedirs(self.basepath+'/jobs', exist_ok=True)

        for sort in range(NUMBER_OF_JOBS):
            d = {'sort': sort}
            o = self.basepath + '/jobs/job.sort_%d.json'%(sort)
            with open(o, 'w') as f:
                json.dump(d, f)

        # create program file
        with open(self.basepath+'/program.py','w') as f:
            f.write(program)

        assert True


    @pytest.mark.order(3)
    def test_create_task(self):

        parser = task_parser(DATABASE_HOST_SERVER)
        db     = postgres(DATABASE_HOST_SERVER)

        command  = "python {PATH}/program.py -j %IN".format(PATH=self.basepath)
        task_id = parser.create( self.basepath, TASK_NAME, self.basepath+'/jobs', IMAGE, command, EMAIL, do_test=False)
        
        assert task_id is not None

        with db as session:
            task = session.task(task_id)
            assert task is not None

            assert len(task.jobs) == NUMBER_OF_JOBS

     
    @pytest.mark.order(4)
    def test_run(self):

        db       = postgres(DATABASE_HOST_SERVER)
        executor = Consumer(slot_size=NUMBER_OF_SLOTS, level="DEBUG")
        schedule = Schedule(level='DEBUG')

        with db as session:
            task_id = session.task(TASK_NAME).id

        #
        # emulate pilot loop
        #
        while True:
        
            with db as session:
            
                status = session.task(task_id).status
              
                if status in [TaskStatus.COMPLETED, TaskStatus.BROKEN, TaskStatus.FINALIZED, TaskStatus.KILLED]:
                    break

                schedule.loop()
                sleep(2)
                executor.loop()
                if executor.full():
                    continue
                n = executor.size - len(executor)
                for job_id in schedule.get_jobs(n):
                    executor.start_job( job_id )

         
        with db as session:
            task = session.task(TASK_NAME)
            assert task.status == TaskStatus.COMPLETED 





if __name__ == "__main__":


    test = test_completed()
    test.test_prepare_database()
    test.test_prepare_jobs()
    test.test_create_task()
    test.test_run()
