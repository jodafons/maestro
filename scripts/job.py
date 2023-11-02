#!/usr/bin/env python3

from time import sleep
import argparse, traceback, os, sys, subprocess, psutil
import wandb

parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()

parser.add_argument('-c','--command', action='store', 
    dest='command', required = True,
    help = "bash command.")


import sys,os
if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()


try:
    env = os.environ.copy()
    task_name = env["SINGULARITYENV_JOB_TASKNAME"]
    job_name  = env["SINGULARITYENV_JOB_NAME"]
    job_id    = env["SINGULARITYENV_JOB_ID"]
    use_wandb = True if env["SINGULARITYENV_JOB_USE_WANDB"]=="true" else False
    
    if use_wandb:
        wandb.init(project=task_name,name=job_name,id=job_id )
    
    proc = subprocess.Popen(args.command, env=env, shell=True)
    sleep(1) # NOTE: wait for 2 seconds to check if the proc really start.
    proc_stat = psutil.Process(proc.pid)
    def is_alive(proc):
      return True if (proc and proc.poll() is None) else False
    while is_alive(proc):
        sleep(1)
    sys.exit(0)

except  Exception as e:
    traceback.print_exc()
    sys.exit(1)