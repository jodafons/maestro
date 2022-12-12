#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --partition=gpu-large
#SBATCH --job-name=orchestra-executor
#SBATCH --exclusive
#SBATCH --account=joao.pinto
#SBATCH --reservation=joao.pinto_2
#SBATCH --ntasks=1

echo $SLURM_JOB_NODELIST
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

srun ./run_master.sh 1 0
wait