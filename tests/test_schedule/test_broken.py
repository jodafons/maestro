

from maestro.task_parser import task_parser
from maestro.database_parser import database_parser
from maestro.api.client_postgres import client_postgres
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

time.sleep(5)
#print('Finish job...')
"""

NUMBER_OF_JOBS       = 5
NUMBER_OF_SLOTS      = 2
LOCAL_HOST           = os.environ["LOCAL_HOST"]
DATABASE_HOST_SERVER = f"postgresql://postgres:postgres@{LOCAL_HOST}:5432/postgres"
TASK_NAME            = 'test.server'
EMAIL                = 'jodafons@lps.ufrj.br'
IMAGE                = ""

class test_broken(unittest.TestCase):

    basepath      = tempfile.mkdtemp()

    @pytest.mark.order(1)
    def test_prepare_database(self):
        sleep(4)
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
        db = client_postgres(DATABASE_HOST_SERVER)

        # NOTE: This command will not work and must put the task in broken status (-j was move to -x to broke)
        command  = "python {PATH}/program.py -x %IN".format(PATH=self.basepath)
        task_id = parser.create( self.basepath, TASK_NAME, self.basepath+'/jobs', IMAGE, command, EMAIL, do_test=False)
        
        assert task_id is not None

        task = db.task(task_id)
        assert task is not None

        assert len(task.jobs) == NUMBER_OF_JOBS

     
    @pytest.mark.order(4)
    def test_run(self):

        db = client_postgres(DATABASE_HOST_SERVER)
        task = db.task(TASK_NAME)
        executor = Consumer("executor-server", db, size=NUMBER_OF_SLOTS)
        schedule = Schedule(db, level='DEBUG')


        #
        # emulate pilot loop
        #
        while db.task(task.id).status not in [TaskStatus.COMPLETED, TaskStatus.BROKEN, TaskStatus.FINALIZED, TaskStatus.KILLED]:

            schedule.run()
            sleep(2)
            executor.loop()
            if executor.full():
                continue
            n = executor.size - executor.allocated()
            for job in db.get_n_jobs(n):
                executor.start_job( job.id, job.task.name, job.command, job.image, self.basepath, device=-1, dry_run=True )

        task = db.task(TASK_NAME)
        assert task.status == TaskStatus.BROKEN




if __name__ == "__main__":


    test = test_broken()
    test.test_prepare_database()
    test.test_prepare_jobs()
    test.test_create_task()
    test.test_run()
