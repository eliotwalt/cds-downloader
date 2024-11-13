#!/bin/bash

source ./env/modules.sh

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
if [ -z "$OUT_FREQUENCY" ]; then
    FILE_FREQUENCY=$BASE_FREQUENCY
else
    FILE_FREQUENCY=$OUT_FREQUENCY
    if [ -z "$AGGREGATION" ]; then
        echo "No aggregation method specified while output frequency is set. Please specify an aggregation method."
        exit 1
    fi
fi

if [ -z "$OUT_RESOLUTION" ]; then
    FILE_RESOLUTION=$BASE_RESOLUTION
else
    FILE_RESOLUTION=$OUT_RESOLUTION
    if [ -z "$REGRID_GRID" ] || [ -z "$REGRID_CDO_FN" ]; then
        echo "Regrid method and grid not both specified while output resolution is set. Please specify a regrid method and grid."
        exit 1
    fi
fi

filename="era5_cds-${variable}-${min_year}-${max_year}-${FILE_FREQUENCY}-${FILE_RESOLUTION}"
# tmp path
tmp_path=$TMPDIR/$filename.nc
# final path
final_path="${DATA_ROOT_DIR}/${OUTPUT_DIR}/${min_year}-${max_year}-${FILE_FREQUENCY}-${FILE_RESOLUTION}/$filename.zarr"

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
for i in $(seq 0 $n_cpus $(( ${#YEARS[@]} - 1 ))); do
    for j in $(seq 0 $((n_cpus - 1))); do
        index=$((i+j))
        if [ $index -ge ${#YEARS[@]} ]; then
            break
        fi
        (
            # get the year
            y=${YEARS[$index]}

            # get sub path
            sub_filename=era5_cds-${variable}-${y}-${FILE_FREQUENCY}-${FILE_RESOLUTION}
            sub_tmp_path=$TMPDIR/$sub_filename

            # copy options and add arguments
            sub_options=$options
            sub_options+="--years $y "
            sub_options+="--path $sub_tmp_path.orig.nc "

            # launch download
            python ./src/download_era5_cds.py $sub_options

            echo "Successfully downloaded $sub_tmp_path.orig.nc"

            # cdo pipeline
            cdo_pipeline=" -b F32 "

            # aggregate
            if [ -z "$AGGREGATION" ]; then
                echo "No aggregation method specified"
            else
                # check 
                if [[ "$AGGREGATION" != "day" && "$AGGREGATION" != "mon" && "$AGGREGATION" != "year" ]]; then
                    echo "Unknown aggregation method: $AGGREGATION"
                    exit 1
                fi
                agg=$AGGREGATION

                # check if cumulative variable
                if [[ " ${CUMULATIVE_VARIABLES[@]} " =~ " ${variable} " ]]; then # cumulative variables
                    agg+="sum"
                else
                    agg+="mean"
                fi

                # add to cdo pipeline
                cdo_pipeline+=" -$agg "
            fi

            # regrid
            if [ -z "$REGRID_GRID" ] || [ -z "$REGRID_CDO_FN" ] ; then
                echo "No regrid method and grid specified"
            else
                # TODO: check validity but too many possibilities so not implemented
                # add to cdo pipeline
                cdo_pipeline+=" -$REGRID_CDO_FN,$REGRID_GRID "
            fi

            # apply cdo pipeline
            echo "Applying cdo pipeline: $cdo_pipeline"
            cdo $cdo_pipeline $sub_tmp_path.orig.nc $sub_tmp_path.nc

            # check if cdo was successful
            cdo sinfo $sub_tmp_path.nc

            # remove original file
            rm $sub_tmp_path.orig.nc

            # append to tmp_paths
            tmp_paths+=($sub_tmp_path.nc)
        )&
    done
    wait
    echo "Done with year $y"
done

# mergetime and delete
echo "Merging files: ${tmp_paths[@]}"
cdo mergetime ${tmp_paths[@]} $tmp_path && rm ${tmp_paths[@]}

# compress and delete
echo "Saving to zarr: $final_path"
python ./src/convert_zarr.py --input_file $tmp_path --output_file $final_path && rm $tmp_path


