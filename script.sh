#!/bin/bash
# Created by the University of Melbourne job script generator for SLURM
# Mon Apr 06 2020 20:15:04 GMT+1000 (Australian Eastern Standard Time)

# Partition for the job:
#SBATCH --partition=physical

# The name of the job:
#SBATCH --job-name="CCC_test_"

# Maximum number of tasks/CPU cores used by the job:
#SBATCH --nodes=2
#SBATCH --ntasks=8



# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=0-0:30:00

# check that the script is launched with sbatch
if [ "x$SLURM_JOB_ID" == "x" ]; then
   echo "You need to submit your job to the queuing system with sbatch"
   exit 1
fi

# Run the job from the directory where it was launched (default)
module load Python/3.7.3-spartan_gcc-8.1.0
mpirun -n 8 python 2_test.py

