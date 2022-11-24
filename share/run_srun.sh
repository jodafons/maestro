#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --partition=gpu
#SBATCH --job-name=orchestra-executor
#SBATCH --exclusive
#SBATCH --account=joao.pinto


echo $SLURM_JOB_NODELIST
nodeset -e $SLURM_JOB_NODELIST


export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export IMG=/home/jodafons/public/images/orchestra_latest.sif

srun -N 1 -n 1 -c $SLURM_CPUS_PER_TASK --gpus 2 singularity run --nv --writable-tmpfs $IMG pilot run &

wait