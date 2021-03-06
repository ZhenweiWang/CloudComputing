#!/bin/bash
#SBATCH --time=01:00:00
#SBATCH --mem 10240
#SBATCH --begin 05:30
#SBATCH --nodes=2
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
module load Python/3.4.3-goolf-2015a
srun play_100M.py
