

import os, time, subprocess, threading, psutil
from termios import PENDIN
from colorama import *
from colorama import init
init(autoreset=True)


#
# Job states
#

UNKNOWN     = 'Unknown'
PENDING     = 'Pending'
ASSIGNED    = 'Assigned'
RUNNING     = 'Running'
COMPLETED   = 'Completed'
BROKEN      = 'Broken'
FAILED      = 'Failed'
KILL        = 'Kill'
KILLED      = 'Killed'


SECONDS     = 1
MAX_NUMBER_OF_JOBS = 50 # half of max number of connection into postgres database




#
# timer
#
class Clock(threading.Thread):

    def __init__(self,proc, tic=time.time()):
        threading.Thread.__init__(self)
        self.proc = proc
        self.__stop = threading.Event()
        self.__tic = tic
        self.__toc = None

    def is_alive(self):
        return True if self.proc.poll() is None else False

    def run(self):
        while not self.__stop.isSet():
            if not self.is_alive():
                self.__toc = time.time()
                self.__stop.set()

    def stop(self):
        self.__stop.set()

    def resume(self):
        if not self.__toc:
            return time.time() - self.__tic
        else:
            return self.__toc - self.__tic











#
# Job
#
class Job:

    def __init__(self, command, workarea, job_id, device=-1, quite=False):

        self.command = command

        # job control
        self.__killed = False
        self.__pending = True
        self.__assigned = False
        self.__clock = None
        self.__proc_stat = None
        self.pid = -1
        self.device = device
        self.id = job_id
        self.quite = quite

        # setup func internal envs
        self.__env = os.environ.copy()
        # where all python files will be executed
        self.workarea = workarea

        # setting GPU identification
        self.__env["CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"
        self.__env["CUDA_VISIBLE_DEVICES"]=str(device)
        self.__env["TF_FORCE_GPU_ALLOW_GROWTH"] = 'true'

        # setting job identification
        self.__env['JOB_WORKAREA'] = self.workarea
        self.__env['JOB_DATABASE_HOST'] = self.__env['ORCHESTRA_DATABASE_HOST']
        self.__env['JOB_DATABASE_KEY']  = self.id
 

    def run(self):

        command = "cd {workarea} && ".format(workarea=self.workarea)
        command += self.command
        if self.quite:
            command+= " > {log}".format(log=self.workarea+'/output.log') 
        print(Style.BRIGHT + Fore.GREEN + command)
        tic = time.time()
        self.proc = subprocess.Popen(command,env=self.__env, shell=True)
        self.__clock = Clock(self.proc, tic)
        self.__clock.start()
        self.__proc_stat = psutil.Process(self.proc.pid)
        self.pid = self.proc.pid
        self.__killed = False
        self.__assigned = False
        self.__pending = False
        return True


    def memory(self):
        return self.__proc_stat.memory_info().rss/(1024*1024) if self.is_alive() else 0




    def is_alive(self):
        return True if self.proc.poll() is None else False



    def kill(self):
        if self.is_alive():
            children = self.__proc_stat.children(recursive=True)
            for child in children:
                p=psutil.Process(child.pid)
                p.kill()
            self.proc.kill()
            self.__killed = True
            return True
        else:
            return False


    def resume(self):
        return self.__clock.resume() if self.__clock else 0.0


    def status(self):
        if self.__killed:
            return KILLED
        elif self.__pending:
            return PENDING
        elif self.__assigned:
            return ASSIGNED
        elif self.is_alive():
            return RUNNING
        else:
            if self.proc.returncode > 0:
                return FAILED
            else:
                return COMPLETED

    
    def activate(self):
        self.__pending=False
        self.__assigned=True



    def summary(self):
        return {
                'job.id'        : self.id,
                'job.status'    : self.status(),
                'job.time'      : self.resume(), 
                'job.device'    : self.device,
                'job.workarea'  : self.workarea,
                }



