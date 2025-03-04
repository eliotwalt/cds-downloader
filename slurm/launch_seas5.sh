#!/bin/bash
MAX_CPUS=3
MAX_CONCURRENT_JOBS=4

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
cpus_per_task=$(( MAX_CPUS < ${#YEARS[@]} ? MAX_CPUS : ${#YEARS[@]} ))

echo "Launching $num_jobs jobs with $cpus_per_task cpus per task"

# lauch jobs
sbatch --cpus-per-task=$cpus_per_task \
       --mem=32G \
       --time=120:00:00 \
       --partition=staging \
       --output=./logs/seas5/%A_%a.out \
       --error=./logs/seas5/%A_%a.out \
       --job-name=dl_seas5_cds \
       --array=0-$((num_jobs-1))%${MAX_CONCURRENT_JOBS} \
       --wrap="source ./env/modules.sh && source ./env/venv/bin/activate && ./slurm/job_seas5.sh --config $config --host_config $host_config"
