

from maestro.api.client_postgres import client_postgres
from maestro.task_parser import task_parser
from maestro.database_parser import database_parser
from time import sleep
import pytest
import unittest
import os, json, socket


program = """

import argparse
import sys,os
import traceback
import json
import time
import orchestra

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

NUMBER_OF_JOBS = 5


class test_basics(unittest.TestCase):

    local_host    = os.environ["LOCAL_HOST"]
    database_host = "postgresql://postgres:postgres@{LOCAL_HOST}:5432/postgres".format(LOCAL_HOST=os.environ["LOCAL_HOST"])
    basepath      = os.getcwd() + '/test_basics'
    taskname      = "test_basics"
    image         = ""


    @pytest.mark.order(1)
    def test_prepare_database(self):
        #parser = database_parser( self.database_host )
        #assert parser.recreate()
        pass

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

        parser = task_parser(self.database_host)
        
        command  = "python {PATH}/program.py -j %IN".format(PATH=self.basepath)

        assert parser.create( self.basepath, self.taskname, self.basepath+'/jobs', self.image, command, do_test=False)
        
        db = client_postgres(self.database_host)
        task = db.task(self.taskname)
        assert task is not None

        assert len(task.jobs) == NUMBER_OF_JOBS

     
    @pytest.mark.order(4)
    def test_run(self):
      pass




