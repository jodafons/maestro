
from . import *
from colorama import *
from colorama import init
init(autoreset=True)



class Dispacher:

    def __init__(self):
        self.__api = Slave()


    def run(self):

        for job in self.__db.jobs(status=ASSIGNED):
            self.__api.create(job)

        jobs = self.__api.jobs()
        deactivate_jobs = []

        for job in jobs:

            job_id = job['job.id']
            job_db = self.__db.job(job_id)
            status = job['job.status']

            if status == ASSIGNED:
   
                if( self.__api.run(job_id) ):
                    job_db.status = RUNNING
                    print(Style.BRIGHT + Fore.GREEN + "Job %d enter into RUNNING state."%job_id)
                else:
                    job_db.status = BROKEN
                    deactivate_jobs.append(job_id)
                    print(Style.BRIGHT + Fore.RED + "Job %d enter into BROKEN state."%job_id)

            elif status == RUNNING:
                job_db.ping()
                continue

            elif status == FAILED:
                job_db.status = FAILED
                deactivate_jobs.append(job_id)
                print(Style.BRIGHT + Fore.RED + "Job %d enter into FAILED state."%job.id)

            elif status == COMPLETED:
                job_db.status = COMPLETED
                deactivate_jobs.append(job_id)
                print(Style.BRIGHT + Fore.BLUE + "Job %d enter into COMPLETED state."%job.id)

            elif status == KILLED:
                job_db.status = KILLED
                deactivate_jobs.append(job_id)
                print(Style.BRIGHT + Fore.RED + "Job %d enter into KILLED state."%job.id)

 
        for job_id in deactivate_jobs:
            if not self.__api.remove(job_id):
                print(Style.BRIGHT + Fore.RED + "Job {job_id} does not exist into the queue" )





