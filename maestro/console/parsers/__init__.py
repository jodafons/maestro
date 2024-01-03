__all__ = ["srun","scancel"]

import os
from loguru import logger
from time import sleep


script_sbatch = """#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output={jobname}.job_%j.out
#SBATCH --ntasks-per-node=1
#SBATCH --job-name={jobname}
#SBATCH --exclusive
#SBATCH --account={account}
#SBATCH --ntasks=1
#SBATCH --partition={partition}
#SBATCH --reservation={reservation}

echo 'Node:' $SLURM_JOB_NODELIST
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export DATABASE_SERVER_URL='{database_url}'
source {virtualenv}/bin/activate
{command}
wait
"""

#
# slurm integration
#
def srun( args, dry_run : bool=False ):

  args.partition  = args.slurm_partition
  params = vars(args)
  mode = params.pop('mode')
  option = params.pop('option')
  command = f'maestro {mode} {option}'

  # command preparation
  for key, value in params.items():
    # remove all slurm args
    if 'slurm' in key:
      continue
    # if bool and true append the key into the command
    if type(value)==bool and not value:
      continue
    # if value is str shoul add ''
    value_str = f"'{value}'" if type(value) == str else f"{value}"
    command += f" --{key.replace('_','-')}={value_str}"
    command = command.replace('=True','').replace('=False','')

  if not args.slurm_virtualenv:
    logger.error("maestro virtual env must be passed by --slurm-virtualenv as parameter.")
    return False
  if not args.slurm_partition:
    logger.error("slurm partition must be assigned by --slurm-partition as parameter.")
    return False
  if not args.slurm_account:
    logger.error("slurm account name must be passed by --slurm-account as parameter.")
    return False
    
  job_name = args.slurm_name + '-' +option
  # prepare script
  with open(f'sbatch_{job_name}.sh','w') as f:
    cmd = script_sbatch.format( 
                          jobname=job_name,
                          account=args.slurm_account,
                          partition=args.slurm_partition,
                          reservation=args.slurm_reservation,
                          database_url=args.database_url,
                          virtualenv=args.slurm_virtualenv,
                          command=command
                        )
    print(cmd)
    f.write(cmd)

  for _ in range( args.slurm_nodes ):
    if not dry_run:
        os.system(f'sbatch sbatch_{job_name}.sh')
  sleep(5)
  if not dry_run:
    os.system(f'rm sbatch_{job_name}.sh')

  return True

def scancel():
  options = ['pilot','executor','all']
  for op in options:
    command = "squeue -u $USER -n maestro-%s -h | awk '{print $1}' | xargs scancel && rm *.out"%(op)
    os.system(command)



from . import init_parser
__all__.extend( init_parser.__all__ )
from .init_parser import *

from . import task_parser
__all__.extend( task_parser.__all__ )
from .task_parser import *

from . import run_parser
__all__.extend( run_parser.__all__ )
from .run_parser import *

