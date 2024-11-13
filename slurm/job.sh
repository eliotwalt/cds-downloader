#!/bin/bash

CUMULATIVE_VARIABLES=(
    "total_column_water_vapour" 
    "total_column_cloud_ice_water" 
    "total_column_cloud_liquid_water" 
    "total_column_rain_water" 
    "total_column_snow_water" 
    "total_column_graupel_water" 
    "total_precipitation"
    "total_precipitation_6hr"
    "total_precipitation_3hr"
    "total_precipitation_1hr"
    "total_precipitation_12hr"
)

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

# get configs
source $config
source $host_config

# get min/max years
min_year=${YEARS[0]}
max_year=${YEARS[0]}
for y in "${YEARS[@]}"; do 
    (( y < min_year )) && min_year=$y
    (( y > max )) && max_year=$y
done

# get job options
echo "job options:"
if [ $SLURM_ARRAY_TASK_ID -lt ${#SINGLE_LEVEL_VARIABLES[@]} ]; then
    variable=${SINGLE_LEVEL_VARIABLES[$SLURM_ARRAY_TASK_ID]}
    options="--variables $variable "
    echo " * variable: $variable"
else
    variable=${MULTI_LEVEL_VARIABLES[$((SLURM_ARRAY_TASK_ID - ${#SINGLE_LEVEL_VARIABLES[@]}))]} 
    options="--variables $variable "
    options+="--levels ${LEVELS[@]} "
    echo " * variable: $variable"
    echo " * levels: ${LEVELS[@]}"
fi
options+="--months ${MONTHS[@]} " && echo " * months: ${MONTHS[@]}"
options+="--days ${DAYS[@]} " && echo " * days: ${DAYS[@]}"
options+="--times ${TIMES[@]} " && echo " * times: ${TIMES[@]}"
options+="--area ${AREA[@]} " && echo " * area: ${AREA[@]}"
options+="--format ${FORMAT} " && echo " * format: ${FORMAT}"

# get paths 
filename="era5_cds-${variable}-${min_year}-${max_year}-${FREQUENCY}-${RESOLUTION}"
# tmp path
tmp_path=$TMPDIR/$filename.nc
# final path
final_path="${DATA_ROOT_DIR}/${OUTPUT_DIR}/${min_year}-${max_year}-${FREQUENCY}-${RESOLUTION}/$filename.zarr"

mkdir -p `dirname $tmp_path`
mkdir -p `dirname $final_path`

echo "Paths:"
echo " * tmp path: $tmp_path"
echo " * final path: $final_path"

# get the number of cpu for the job (cpu_per_tasks) from slurm env var
n_cpus=$SLURM_CPUS_PER_TASK
echo " * n_cpus: $n_cpus"

# download the data
tmp_paths=()
for i in $(seq 0 $n_cpus ${#YEARS[@]}); do
    for j in $(seq 0 $((n_cpus - 1))); do
        (
            # get the year
            index=$((i+j)) && y=${YEARS[$index]}

            # get sub path
            sub_filename=era5_cds-${variable}-${y}-${FREQUENCY}-${RESOLUTION}
            sub_tmp_path=$TMP_DIR/$sub_filename

            # copy options and add arguments
            sub_options=$options
            sub_options+="--years $y "
            sub_options+="--path $sub_tmp_path.orig.nc "

            # launch download
            python ./src/download_era5_cds.py $sub_options

            # aggregate
            if [ -z "$AGGREGATE" ]; then
                mv $sub_tmp_path.orig.nc $sub_tmp_path.nc
            else
                if  [ "$AGGREGATE" == "daily" ]; then
                    if [[ " ${CUMULATIVE_VARIABLES[@]} " =~ " ${variable} " ]]; then # cumulative variables
                        cdo -b F32 daysum $sub_tmp_path.orig.nc $sub_tmp_path.nc
                    else
                        cdo -b F32 daymean $sub_tmp_path.orig.nc $sub_tmp_path.nc
                    fi
                elif [ "$AGGREGATE" == "monthly" ]; then
                    if [[ " ${CUMULATIVE_VARIABLES[@]} " =~ " ${variable} " ]]; then # cumulative variables
                        cdo -b F32 monsum $sub_tmp_path.orig.nc $sub_tmp_path.nc
                    else
                        cdo -b F32 monmean $sub_tmp_path.orig.nc $sub_tmp_path.nc
                    fi
                elif [ "$AGGREGATE" == "yearly" ]; then
                    if [[ " ${CUMULATIVE_VARIABLES[@]} " =~ " ${variable} " ]]; then # cumulative variables
                        cdo -b F32 yearsum $sub_tmp_path.orig.nc $sub_tmp_path.nc
                    else
                        cdo -b F32 yearmean $sub_tmp_path.orig.nc $sub_tmp_path.nc
                    fi
                else
                    echo "Unknown aggregation method: $AGGREGATE"
                    exit 1
                fi
            fi
            rm $sub_tmp_path.orig.nc

            # append to tmp_paths
            tmp_paths+=($sub_tmp_path.nc)
        )&
    done
    wait
done

# mergetime and delete
cdo mergetime ${tmp_paths[@]} $tmp_path && rm ${tmp_paths[@]}

# compress and delete
python ./src/convert_zarr.py --input_file $tmp_path --output_file $final_path && rm $tmp_path


