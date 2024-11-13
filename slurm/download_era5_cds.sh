#!/bin/bash
host_config=./configs/hosts/snellius.yaml

# create getops like options: --config --host_config
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

# load the config file
./env/modules.sh 
source ./env/venv/bin/activate

# count the number of jobs
source $config
num_jobs=$(python ./src/arrays.py \
    --years ${YEARS[@]} \
    --single_variables ${SINGLE_LEVEL_VARIABLES[@]} \
    --multi_variables ${MULTI_LEVEL_VARIABLES[@]} \
    --levels ${LEVELS[@]} \
    --strategy $ARRAY_STRATEGY \
    --n_years $N_YEARS)
echo "Scheduling $num_jobs jobs"

# lauch 
sbatch --cpus_per_task=1 \
       --mem=16G \
       --time=48:00:00 \
       --partition=staging \
       --output=./logs/%A_%a.out \
       --error=./logs/%A_%a.out \
       --job-name=dl_era5_cds \
       --array=0-$((num_jobs-1)) \
       --wrap="./slurm/download_era5_cds_job.sh --config $config --host_config $host_config"

