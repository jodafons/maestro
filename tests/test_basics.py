

from orchestra.database.models import Job, Task, Device, Base
from orchestra.database import postgres_client
from orchestra.server.main import Pilot
from orchestra.server.consumer import Consumer
from orchestra.server.mailing import Postman
from orchestra.server.schedule import Schedule, compile
from orchestra.status import TaskStatus
from orchestra.api import TaskParser, DeviceParser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest
import unittest
import os, json, socket, time

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

    host = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
    basepath = os.getcwd() + '/test_basics'
    image = '/home/joao.pinto/public/images/tensorflow_2.10.0-gpu.sif'

    

    @pytest.mark.order(1)
    def test_prepare_database(self):

        engine = create_engine(self.host)
        Session = sessionmaker(bind=engine)
        session = Session()
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        session.commit()
        session.close()
        assert True


    @pytest.mark.order(2)
    def test_prepare_workarea(self):

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

        db = postgres_client(self.host)

        api = TaskParser(db)
        taskname = 'basics'
        command = "python {PATH}/program.py -j %IN".format(PATH=self.basepath)
        answer, message = api.create(self.basepath, taskname, self.basepath+'/jobs', self.image, command)        
        assert answer == True, message
        
        task = db.task(taskname)

        assert len(task.jobs) == NUMBER_OF_JOBS

        # delete task by hand
        db.session().query(Job).filter(Job.taskid==task.id).delete()
        db.session().query(Task).filter(Task.id==task.id).delete()
        db.commit()


    


    @pytest.mark.order(4)
    def test_delete(self):

        db = postgres_client(self.host)

        api = TaskParser(db)
        taskname = 'basics'
        command = "python {PATH}/program.py -j %IN".format(PATH=self.basepath)
        answer, message = api.create(self.basepath, taskname, self.basepath+'/jobs', self.image, command)        
        assert answer == True, message
        
        # delete task using the api
        task = db.task(taskname)
        answer, message = api.delete([task.id], force=True)
        assert answer == True, message
        
        # check if exsist into the database
        task = db.task(taskname)
        assert task == None




    @pytest.mark.order(5)
    def test_run(self):

        db = postgres_client(self.host)

        task_api = TaskParser(db)
        taskname = 'basics'
        command = "python {PATH}/program.py -j %IN".format(PATH=self.basepath)
        answer, message = task_api.create(self.basepath, taskname, self.basepath+'/jobs', self.image, command)        
        assert answer == True, message
        

        # create device for this local test
        device_api = DeviceParser(db)
        device_api.create(socket.gethostname(), device=-1, slots=10, enabled=10)


        # create postman
        postman = Postman( os.environ['ORCHESTRA_EMAIL_FROM'], 
                           os.environ['ORCHESTRA_EMAIL_TOKEN'],
                           os.environ['ORCHESTRA_EMAIL_TO'],
                           os.environ["ORCHESTRA_BASEPATH"]+'/orchestra/server/mailing/templates')

        # create schedule
        schedule = Schedule(db, postman)
        compile(schedule)

        # create pilot (force to create the device)
        app = Pilot(db, schedule, master=False)

        tic = time.time()
        now = tic
        while (now - tic) < 30:
            app.schedule.run()
            for consumer in app.consumers:
                n = consumer.size() - consumer.allocated()
                jobs_db = app.schedule.jobs(n)
                while consumer.available() and (len(jobs_db) > 0):
                    consumer.push_back(jobs_db.pop())
                consumer.run()
            now = time.time()
        

        task = db.task(taskname)
        assert task.status == TaskStatus.COMPLETED, f"Task fail with status {task.status}"
        
        answer, message = task_api.delete([task.id], force=True)
        assert answer == True, message
        

        for consumer in app.consumers:
            db.session().query(Device).filter(Device.id==consumer.device_db.id).delete()
        db.commit()


    @pytest.mark.order(6)
    def test_delete_workarea(self):
        os.system('rm -rf {}'.format(self.basepath))

    @pytest.mark.order(7)
    def test_delete_database(self):
        engine = create_engine(self.host)
        Session = sessionmaker(bind=engine)
        session = Session()
        Base.metadata.drop_all(engine)






