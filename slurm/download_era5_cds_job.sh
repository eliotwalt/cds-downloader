#!/bin/bash

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

echo "Slurm array task id: $SLURM_ARRAY_TASK_ID"

# load the config file
./env/modules.sh 
source ./env/venv/bin/activate

# get the job specifications
source $config
output=$(python ./src/arrays.py \
    --years ${YEARS[@]} \
    --single_variables ${SINGLE_LEVEL_VARIABLES[@]} \
    --multi_variables ${MULTI_LEVEL_VARIABLES[@]} \
    --levels ${LEVELS[@]} \
    --strategy $ARRAY_STRATEGY \
    --index $SLURM_ARRAY_TASK_ID \
    --n_years $N_YEARS)

# Extract years, variables, and levels from the output tuple
IFS=',' read -r years variables levels <<< "$output"
years=($years)
variables=($variables)
levels=($levels)

echo "Job specs:"
echo " * years: ${years[@]}"
echo " * variables: ${variables[@]}"
echo " * levels: ${levels[@]}"

# create path
min_year=${years[0]}
max_year=${years[0]}
for y in "${years[@]}"; do 
    (( y < min_year )) && min_year=$y
    (( y > max )) && max_year=$y
done
filename="era5_cds-${variables}-${min_year}-${max_year}-${FREQUENCY}-${RESOLUTION}"

# tmp path
tmp_path=$TMP_DIR/$filename.nc
#mkdir -p `basename $tmp_path`

# final path
source $host_config
final_path="${DATA_ROOT_DIR}/${OUTPUT_DIR}/${min_year}-${max_year}-${FREQUENCY}-${RESOLUTION}/$filename.zarr"
#mkdir -p `basename $final_path`

echo "Paths:"
echo " * tmp_path: $tmp_path"
echo " * final_path: $final_path"


# download
python ./src/download_era5_cds.py \
    --years ${years[@]} \
    --single_variables ${variables[@]} \
    --levels ${levels[@]} \
    --frequency $FREQUENCY \
    --resolution $RESOLUTION \
    --path $tmp_path \
    --config $config \
    --host_config $host_config

# download
# path=$(python ./src/download_era5_cds.py \
#     --config $config --host_config $host_config | tail -n 1)
# echo $path

