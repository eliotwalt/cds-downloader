#!/bin/bash

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --config) config="$2"; shift ;;
        --host_config) host_config="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# ensure that all options are set
if [ -z "$config" ] || [ -z "$host_config" ]; then
    echo "Please provide a config and host config file";
    exit 1
fi

# count jobs
source $config
num_jobs=$((${#SINGLE_LEVEL_VARIABLES[@]} + ${#MULTI_LEVEL_VARIABLES[@]}))

# get the number of cpus per task
max_cpus=20
cpus_per_task=$(( max_cpus < ${#YEARS[@]} ? max_cpus : ${#YEARS[@]} ))

echo "Launching $num_jobs jobs with $cpus_per_task cpus per task"

# lauch jobs
sbatch --cpus-per-task=$cpus_per_task \
       --mem=16G \
       --time=48:00:00 \
       --partition=staging \
       --output=./logs/%A_%a.out \
       --error=./logs/%A_%a.out \
       --job-name=dl_era5_cds \
       --array=0-$((num_jobs-1)) \
       --constraint=scratch-node \
       --wrap="./env/modules.sh && source ./env/venv/bin/activate && ./slurm/job.sh --config $config --host_config $host_config"
