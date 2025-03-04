#!/bin/bash

set -euo pipefail
trap 'echo "An unexpected error occurred. Exiting..."; exit 1' ERR

source ./env/modules.sh

YEARS_PER_REQUEST=1
MONTHS_PER_REQUEST=2

if [ "$YEARS_PER_REQUEST" -ne 1 ]; then
    echo "Error: YEARS_PER_REQUEST must be 1"
    exit 1
fi

CUMULATIVE_VARIABLES=(
   "10m_wind_gust_since_previous_post_processing"
    "eastward_turbulent_surface_stress"
    "evaporation"
    "maximum_2m_temperature_in_the_last_24_hours"
    "minimum_2m_temperature_in_the_last_24_hours"
    "northward_turbulent_surface_stress"
    "runoff"
    "snowfall"
    "sub_surface_runoff"
    "surface_latent_heat_flux"
    "surface_net_solar_radiation"
    "surface_net_thermal_radiation"
    "surface_runoff"
    "surface_sensible_heat_flux"
    "surface_solar_radiation_downwards"
    "surface_thermal_radiation_downwards"
    "toa_incident_solar_radiation"
    "top_net_solar_radiation"
    "top_net_thermal_radiation"
    "total_precipitation"
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
    (( y > max_year )) && max_year=$y
done

# get min/max months
min_month=${MONTHS[0]}
max_month=${MONTHS[0]}
for m in "${MONTHS[@]}"; do 
    # remove leading zeros (e.g. "01" -> 1)
    m=$((10#$m))
    (( m < min_month )) && min_month=$m
    (( m > max_month )) && max_month=$m
done
min_month=$(printf "%02d" $min_month) && max_month=$(printf "%02d" $max_month)

# get job options
echo "job options:"
if [ $SLURM_ARRAY_TASK_ID -lt ${#SINGLE_LEVEL_VARIABLES[@]} ]; then
    variable=${SINGLE_LEVEL_VARIABLES[$SLURM_ARRAY_TASK_ID]}
    options="--variables $variable "
    echo " * variable: $variable"
else
    variable=${MULTI_LEVEL_VARIABLES[$((SLURM_ARRAY_TASK_ID - ${#SINGLE_LEVEL_VARIABLES[@]}))]} 
    options="--variable $variable "
    options+="--levels ${LEVELS[@]} "
    echo " * variable: $variable"
    echo " * levels: ${LEVELS[@]}"
fi
options+="--days ${DAYS[@]} " && echo " * days: ${DAYS[@]}"
options+="--lead_time_start ${LEAD_TIME_START} " && echo " * lead_time_start: ${LEAD_TIME_START}"
options+="--lead_time_end ${LEAD_TIME_END} " && echo " * lead_time_end: ${LEAD_TIME_END}"
options+="--lead_time_freq ${LEAD_TIME_FREQ} " && echo " * lead_time_freq: ${LEAD_TIME_FREQ}"
options+="--area ${AREA[@]} " && echo " * area: ${AREA[@]}"
options+="--format ${FORMAT} " && echo " * format: ${FORMAT}"

# get paths 
if [ -z "$OUT_RESOLUTION" ]; then
    FILE_RESOLUTION=$BASE_RESOLUTION
else
    FILE_RESOLUTION=$OUT_RESOLUTION
    if [ -z "$REGRID_GRID" ] || [ -z "$REGRID_CDO_FN" ]; then
        echo "Regrid method and grid not both specified while output resolution is set. Please specify a regrid method and grid."
        exit 1
    fi
fi

filename="seas5_cds_${variable}-${min_year}_${min_month}-${max_year}_${max_month}-${LEAD_TIME_FREQ}-${AREA_NAME}-${FILE_RESOLUTION}"
# tmp path
merged_tmp_path=$SCRATCH_DATA_DIR/$filename.nc
# final path
final_path="${DATA_ROOT_DIR}/${OUTPUT_DIR}/${min_year}-${max_year}-${LEAD_TIME_FREQ}-${FILE_RESOLUTION}/$filename.zarr"

mkdir -p `dirname $merged_tmp_path`
mkdir -p `dirname $final_path`

echo "Paths:"
echo " * tmp path: $merged_tmp_path"
echo " * final path: $final_path"

# get cdo pipeline
# cdo pipeline
cdo_pipeline=" -b F32 "

# aggregate: NO AGGREGATION FOR SEAS5 because not trivial with "valid_time" and "lead_time"
# and the data is low spatial resolution so not that big of a deal

# regrid
if [ -z "$REGRID_GRID" ] || [ -z "$REGRID_CDO_FN" ] ; then
    echo "No regrid method and grid specified"
else
    # TODO: check validity but too many possibilities so not implemented
    # add to cdo pipeline
    cdo_pipeline+=" -$REGRID_CDO_FN,$REGRID_GRID "
fi

# get the number of cpu for the job (cpu_per_tasks) from slurm env var
n_cpus=$SLURM_CPUS_PER_TASK
echo " * n_cpus: $n_cpus"

# precompute the tmp paths
tmp_paths=()
echo "Precomputing tmp paths:" 
for y in $(seq ${YEARS[0]} $YEARS_PER_REQUEST ${YEARS[-1]}); do
    y_start=$y
    y_end=$((y_start + YEARS_PER_REQUEST - 1))
    if [ $y_end -ge ${YEARS[-1]} ]; then
        y_end=${YEARS[-1]}
    fi
    for m in $(seq 1 $MONTHS_PER_REQUEST 12); do
        m_start=$m
        m_end=$((m_start + MONTHS_PER_REQUEST - 1))
        if [ $m_end -ge 12 ]; then
            m_end=12
        fi
        m_start=$(printf "%02d" $m_start) && m_end=$(printf "%02d" $m_end)
        sub_filename=seas5_cds_${variable}-${y_start}_${m_start}-${y_end}_${m_end}-${LEAD_TIME_FREQ}-${AREA_NAME}-${FILE_RESOLUTION}.nc
        sub_tmp_path=$SCRATCH_DATA_DIR/$sub_filename
        tmp_paths+=($sub_tmp_path)
        echo " * ${y_start}_${m_start}-${y_end}_${m_end}: $sub_tmp_path"
    done
done

# download data
for i in $(seq 0 $((YEARS_PER_REQUEST * n_cpus)) $(( ${#YEARS[@]} - 1 ))); do
    for j in $(seq 0 $((n_cpus - 1))); do
        index=$((i + j * YEARS_PER_REQUEST))
        if [ $index -ge ${#YEARS[@]} ]; then
            break
        fi
        (
            # get the start and end year
            y_start=${YEARS[$index]}
            y_end=$((y_start + YEARS_PER_REQUEST - 1))
            if [ $y_end -ge ${YEARS[-1]} ]; then
                y_end=${YEARS[-1]}
            fi
            
            for m in $(seq 1 $MONTHS_PER_REQUEST 12); do
                m_start=$m
                m_end=$((m_start + MONTHS_PER_REQUEST - 1))
                if [ $m_end -ge 12 ]; then
                    m_end="12"
                fi
                m_start=$(printf "%02d" $m_start) && m_end=$(printf "%02d" $m_end)
                
                # get sub path
                sub_tmp_path=${tmp_paths[$((index * 12 / MONTHS_PER_REQUEST + (m - 1) / MONTHS_PER_REQUEST))]}.tmp
                tmp_path=${tmp_paths[$((index * 12 / MONTHS_PER_REQUEST + (m - 1) / MONTHS_PER_REQUEST))]}

                echo "Downloading years $y_start to $y_end and months $m_start to $m_end"

                # get all years and months in a string
                all_years=""
                for y in $(seq $y_start 1 $y_end); do
                    all_years+="$y "
                done
                all_months=""
                for m in $(seq $m_start 1 $m_end); do
                    m=$(printf "%02d" $m)
                    all_months+="$m "
                done

                # copy options and add arguments
                sub_options=$options
                sub_options+="--years $all_years "
                sub_options+="--months $all_months "
                sub_options+="--path $sub_tmp_path "

                # launch download
                python ./src/download_seas5_cds.py $sub_options

                echo "Successfully downloaded $sub_tmp_path"

                # apply cdo pipeline
                # empty pipeline
                if [ "$cdo_pipeline" == " -b F32 " ]; then
                    echo "No cdo pipeline specified, moving file to $tmp_path"
                    mv $sub_tmp_path $tmp_path
                else
                    echo "Applying cdo pipeline: $cdo_pipeline"
                    cdo $cdo_pipeline $sub_tmp_path $tmp_path # nc.tmp -> .nc
                    echo "Successfully applied cdo pipeline to $tmp_path"
                fi

                # check if the dataset is readable successful
                echo $(python -c "import xarray as xr ; ds=xr.open_dataset('$tmp_path', engine='netcdf4') ; print(str(list(ds.coords.keys()))+','+str(list(ds.data_vars.keys())))")

                # remove original file if exists
                if [ -f $sub_tmp_path ]; then
                    rm $sub_tmp_path
                fi
            done
        )&
    done
    wait
    echo "Done with years $y_start to $y_end and months $m_start to $m_end"
done

# merge, compress, delete
echo "Merging and saving to zarr: $final_path"
python ./src/convert_zarr.py --input ${tmp_paths[@]} --output $final_path && rm ${tmp_paths[@]}

echo "Done."