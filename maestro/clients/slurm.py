__all__ = ['get_all_jobs', 
           'cancel_all_jobs', 
           'Slurm']

import os,subprocess
from loguru import logger
from time import sleep


def get_all_jobs( username : str, jobname : str ):
    command = "squeue -u %s -n %s -h | awk '{print $1}'" % (username, jobname)
    output = subprocess.check_output(command, shell=True)
    jobs = str(output)[2:-1].split('\\n')[:-1]
    return jobs


def cancel_all_jobs( account : str, jobname : str):

  jobs = get_all_jobs( account, jobname + '-master')
  jobs.extend( get_all_jobs(account, jobname + '-runner') )
  for job_id in jobs:
    logger.info(f"cancel job {job_id}...")
    os.system(f"scancel {job_id}")
  os.system("rm *.out")



class Slurm:

  def __init__(self, reservation=None, 
                     account=None, 
                     jobname=None,
                     partition=None,
                     virtualenv=None,
                     filename='srun_entrypoint.sh'):

    self.virtualenv=virtualenv
    self.account=account
    self.reservation=reservation
    self.partition=partition
    self.jobname=jobname
    self.filename=filename



  def run(self,args, envs={}, master=False, dry_run=False):

    command = self.parser(args, master=master)

    with open(self.filename, 'w') as f:

      f.write(f"#!/bin/bash\n")
      f.write(f"#SBATCH --nodes=1\n")
      f.write(f"#SBATCH --ntasks-per-node=1\n")
      f.write(f"#SBATCH --exclusive\n")
      f.write(f"#SBATCH --cpus-per-task=8\n")
      if self.account:
        f.write(f"#SBATCH --account={self.account}\n")
      if self.partition:
        f.write(f"#SBATCH --partition={self.partition}\n")
      if self.reservation:
        f.write(f"#SBATCH --reservation={self.reservation}\n")
      if self.jobname:
        f.write(f"#SBATCH --job-name={self.jobname}\n")
        f.write(f"#SBATCH --output={self.jobname}.job_%j.out\n")

  
      for key, value in envs.items():
        f.write(f"export {key}='{value}'\n")
      if self.virtualenv:
        f.write(f"source {self.virtualenv}/bin/activate\n")
      f.write(f"export LOGURO_LEVEL='INFO'\n")
      f.write(f"echo Node: $SLURM_JOB_NODELIST\n")
      f.write(f"export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK\n")
      f.write(f"echo OMP_NUM_HTREADS: $SLURM_CPUS_PER_TASK\n")

      f.write(f"{command}\n")
      f.write(f"wait\n")

    print( open(self.filename,'r').read())
    if not dry_run:
      os.system( f'sbatch {self.filename}' )
      sleep(2)
      os.system(f'rm {self.filename}')



  #
  # parser arguments to command
  #
  def parser(self, args, master : bool = False):

    parameters = vars(args)
    command = f'maestro run ' + ('master' if master else 'runner')
    def to_argument(argument):
      return "--"+argument.replace('_','-')
    # command preparation
    for argument, value in parameters.items():
      # loop over all arguments
      if argument in ['mode','option']: # skip these keys
        continue
      if 'slurm' in argument: # skip slurm args by the way
        continue
      if 'boot' in argument: # skip boot command since this is hidden for external users
        continue
      if not master: # for runner
        if argument in ['database_recreate']: # skip these args if runner
          continue
        if 'tracking_' in argument: # skip tracking args
          continue
        if 'master_' in argument: # skip master args
          continue
      # if store_true arguments and true append the key into the command
      if type(value)==bool:
        if value:
          command += f' {to_argument(argument)}'
      else:
        command += f" {to_argument(argument)}={value}"

    return command
