
RESERVATION=my_resservation

sbatch --reservation $RESERVATION --partition gpu-large srun_maestro.sh 1 1 1
sbatch --reservation $RESERVATION --partition gpu-large srun_maestro.sh 0 1 1
sbatch --reservation $RESERVATION --partition gpu-large srun_maestro.sh 0 1 1
sbatch --reservation $RESERVATION --partition gpu-large srun_maestro.sh 0 1 1

