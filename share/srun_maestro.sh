#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=orchestra-executor
#SBATCH --exclusive
#SBATCH --account=joao.pinto
#SBATCH --ntasks=1

#echo $SLURM_JOB_NODELIST
#export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

export MAESTRO_LOGPLACE=$pwd

echo "creating workdir..."

WORKDIR=$(mktemp -d)
echo $WORKDIR
cd $WORKDIR

echo "clonning..."
git clone https://github.com/jodafons/orchestra-server.git && cd orchestra-server
source dev_envs.sh

echo "building..."
make build_local


export LOGURO_LEVEL="INFO"
make node 
