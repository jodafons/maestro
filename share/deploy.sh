
RESERVATION=joao.pinto_3

sbatch --reservation $RESERVATION --partition gpu-large srun_master.sh
sbatch --reservation $RESERVATION --partition gpu-large srun_slave.sh
sbatch --reservation $RESERVATION --partition gpu-large srun_slave.sh
sbatch --reservation $RESERVATION --partition gpu-large srun_slave.sh

sbatch --partition gpu srun_slave.sh
sbatch --partition gpu srun_slave.sh
sbatch --partition gpu srun_slave.sh
sbatch --partition gpu srun_slave.sh
