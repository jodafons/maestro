

__all__ = []

import os
import requests
import threading
import traceback

from loguru          import logger
from time            import sleep, time
from qio             import Partition
from qio             import schemas, TaskStatus, JobStatus, RunnerStatus
from qio.io          import get_io_service
from qio.db          import models, get_db_service
from qio.slurm       import get_slurm_service
from qio.manager     import get_manager_service
from .task           import TaskScheduler

#
# vanilla scheduler operating as a first in first out (FIFO) queue
#
class SchedulerFIFO(threading.Thread):

    def __init__(
        self,  
    ):
        threading.Thread.__init__(self)
        self.tasks   = {}
        self.__lock  = threading.Lock()
        self.__stop  = threading.Event()
        

    def stop(self):
        self.__stop.set()
        while self.is_alive():
            sleep(1)
        for task in self.tasks.values():
            task.stop()



    def run(self):

        self.treat_tasks()

        while (not self.__stop.isSet()):
            sleep(10)
            self.__lock.acquire()
            self.loop()
            self.__lock.release()


    def loop(self):

        start = time()
        logger.debug("⌛ schedule loop...")
        self.prepare_tasks()
        self.keep_tasks_alive()
        self.queue_jobs( disable_resources_checker=True )


    def keep_tasks_alive(self):
        start=time()
        self.tasks = {task_id:task_scheduler for task_id, task_scheduler in self.tasks.items() if task_scheduler.is_alive()}
        logger.debug(f"keep tasks alive in {time()-start} seconds")


    def prepare_tasks(self):
        start=time()
        db_service = get_db_service()
        manager = get_manager_service()
        logger.debug("checking pre-registered tasks...")
        tasks_to_be_registered = []
        
        with db_service() as session:
            tasks_db = session.query( models.Task ).filter(models.Task.status==TaskStatus.PRE_REGISTERED).all()
            tasks_to_be_registered.extend([task_db.task_id for task_db in tasks_db])
            
        for task_id in tasks_to_be_registered:
            user_id = db_service.task(task_id).fetch_owner()
            sc = manager.task(user_id).run_task_group( task_id )
            if sc.isFailure():
                logger.warning(sc.reason())
            
            
        with db_service() as session:
            tasks = session.query(models.Task).filter_by(status=TaskStatus.REGISTERED).all()
            for task in tasks:
                if task.task_id not in self.tasks.keys():
                    scheduler = TaskScheduler( task.task_id )
                    self.tasks[task.task_id] = scheduler
                    scheduler.start()

        logger.debug(f"prepare tasks in {time()-start} seconds")


    def queue_jobs(self, partition : Partition, disable_resources_checker : bool=False):

        start=time()
        db_service    = get_db_service()
        io_service    = get_io_service()
        backend       = get_backend_service()
        logger.debug(f"🚀 checking {partition.value} partition...")
        procs=10
        try:
  
            with db_service() as session:
                jobs = (session.query(models.Job)\
                               .filter(models.Job.status==JobStatus.ASSIGNED)\
                               #.filter(models.Job.partition==partition)\
                               .filter(models.Job.sjob_id==-1)\
                               .order_by(models.Job.priority.desc())\
                               .order_by(models.Job.id).limit(procs).all() )
                job_ids = [job_db.job_id for job_db in jobs]
            
            for job_id in job_ids:
                with db_service() as session:
                    job_db = session.query(models.Job).filter(models.Job.job_id==job_id).one()
                    
                    envs       = {}
                    workarea   = io_service.job(job_id).mkdir()
                    envs.update(job_db.get_envs())
                    envs["JOB_ID"]               = job_id
                    envs["EXPERIMENT_WORKAREA"]  = workarea
                    envs["CUDA_VISIBLE_ORDER"]   = "PCI_BUS_ID"
                    envs["TF_CPP_MIN_LOG_LEVEL"] = "3"
                    #envs["CUDA_VISIBLE_DEVICES"] = "-1" if self.device<0 else str(self.device)
                    virtualenv                   = os.environ["VIRTUAL_ENV"]
                    command  = f"cd {workarea}\n"
                    command += f". {virtualenv}/bin/activate\n"
                    command += f"{job_db.command}\n"
                    job_name = f"{job_db.job_type}.{job_id}"
                    ok, job = backend.run( command    = job_db.command,
                                           cpus       = job_db.reserved_cpu_number,
                                           mem        = job_db.reserved_sys_memory_mb,
                                           partition  = job_db.partition,
                                           jobname    = job_name ,
                                           workarea   = workarea,
                                           envs       = envs,
                                           virtualenv = virtualenv)
                    if ok:
                        job_db.sjob_id = job['job_id']
                        job_db.sjob_state = slurm_service.status(job_db.sjob_id)
                        job_db.ping()
                        session.commit()
        except:
            logger.error("🚨 unknown error!")
            traceback.print_exc()

        logger.debug(f"queue jobs in {time()-start}")
        

    def treat_tasks( self ):

        db_service = get_db_service()
       
        with db_service() as session:
            # treat jobs with kill state
            jobs = session.query( models.Job ).filter_by(status=JobStatus.KILL).all()
            for job in jobs:
                if job.status==JobStatus.KILL:
                    job.status=JobStatus.KILLED
            session.commit()

        with db_service() as session:
            # treat jobs with kill state
            jobs = session.query( models.Job ).filter_by(status=JobStatus.RUNNING).all()
            for job in jobs:
                job.status=JobStatus.ASSIGNED
            session.commit()

        with db_service() as session:
            tasks = session.query( models.Task ).filter(models.Task.status!=TaskStatus.COMPLETED).all()
            for task in tasks:
                if task.task_id not in self.tasks.keys():
                    logger.debug(f"recoverning task {task.task_id} with last status {task.status}...")
                    scheduler=TaskScheduler(task.task_id)
                    self.tasks[task.task_id]=scheduler
                    scheduler.start()
     
         

         