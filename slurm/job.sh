#!/bin/bash

source ./env/modules.sh

CUMULATIVE_VARIABLES=(
   "lspf" "large_scale_precipitation_fraction"
   "uvb" "downward_uv_radiation_at_the_surface"
   "bld" "boundary_layer_dissipation"
   "sshf" "surface_sensible_heat_flux"
   "slhf" "surface_latent_heat_flux"
   "ssrd" "surface_solar_radiation_downwards"
   "strd" "surface_thermal_radiation_downwards"
   "ssr" "surface_net_solar_radiation"
   "str" "surface_net_thermal_radiation"
   "tsr" "top_net_solar_radiation"
   "ttr" "top_net_thermal_radiation"
   "ewss" "eastward_turbulent_surface_stress"
   "nsss" "northward_turbulent_surface_stress"
   "lgws" "eastward_gravity_wave_surface_stress"
   "mgws" "northward_gravity_wave_surface_stress"
   "gwd" "gravity_wave_dissipation"
   "tsrc" "top_net_solar_radiation_clear_sky"
   "ttrc" "top_net_thermal_radiation_clear_sky"
   "ssrc" "surface_net_solar_radiation_clear_sky"
   "strc" "surface_net_thermal_radiation_clear_sky"
   "tisr" "toa_incident_solar_radiation"
   "vimd" "vertically_integrated_moisture_divergence"
   "fdir" "total_sky_direct_solar_radiation_at_surface"
   "cdir" "clear_sky_direct_solar_radiation_at_surface"
   "ssrdc" "surface_solar_radiation_downward_clear_sky"
   "strdc" "surface_thermal_radiation_downward_clear_sky"
   "sro" "surface_runoff"
   "ssro" "sub_surface_runoff"
   "es" "snow_evaporation"
   "smlt" "snowmelt"
   "lsp" "large_scale_precipitation"
   "cp" "convective_precipitation"
   "sf" "snowfall"
   "e" "evaporation"
   "ro" "runoff"
   "tp" "total_precipitation"
   "csf" "convective_snowfall"
   "lsf" "large_scale_snowfall"
   "pev" "potential_evaporation"
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

filename="era5_cds_${variable}-${min_year}-${max_year}-${FILE_FREQUENCY}-${AREA_NAME}-${FILE_RESOLUTION}"
# tmp path
tmp_path=$SCRATCH_DATA_DIR/$filename.nc
# final path
final_path="${DATA_ROOT_DIR}/${OUTPUT_DIR}/${min_year}-${max_year}-${FILE_FREQUENCY}-${FILE_RESOLUTION}/$filename.zarr"

mkdir -p `dirname $tmp_path`
mkdir -p `dirname $final_path`

echo "Paths:"
echo " * tmp path: $tmp_path"
echo " * final path: $final_path"

# get cdo pipeline
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

    # cumulative variables take sum, others mean
    if [[ " ${CUMULATIVE_VARIABLES[@]} " =~ " ${variable} " ]]; then # cumulative variables
        agg+="sum"
    else
        agg+="mean"
    fi

    # add aggregation arguments
    if [ -z "$AGGREGATION_ARGS" ]; then
        echo "No aggregation arguments specified"
    else
        agg+=",$AGGREGATION_ARGS"
    fi

    # cumultative variables requires -1 sec shift
    if [[ " ${CUMULATIVE_VARIABLES[@]} " =~ " ${variable} " ]]; then
        cdo_pipeline+=" -shifttime,-1sec "
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

# get the number of cpu for the job (cpu_per_tasks) from slurm env var
n_cpus=$SLURM_CPUS_PER_TASK
echo " * n_cpus: $n_cpus"

# precompute the tmp paths
tmp_paths=()
for y in "${YEARS[@]}"; do
    sub_filename=era5_cds_${variable}-${y}-${FILE_FREQUENCY}-${AREA_NAME}-${FILE_RESOLUTION}.nc
    sub_tmp_path=$SCRATCH_DATA_DIR/$sub_filename
    tmp_paths+=($sub_tmp_path)
done
echo "Tmp files: ${tmp_paths[@]}"

# download data
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
            sub_tmp_path=${tmp_paths[$index]}.tmp # .nc.tmp

            echo "Downloading $sub_tmp_path"

            # copy options and add arguments
            sub_options=$options
            sub_options+="--years $y "
            sub_options+="--path $sub_tmp_path "

            # launch download
            python ./src/download_era5_cds.py $sub_options

            echo "Successfully downloaded $sub_tmp_path"

            # apply cdo pipeline
            echo "Applying cdo pipeline: $cdo_pipeline"
            cdo $cdo_pipeline $sub_tmp_path ${tmp_paths[$index]} # nc.tmp -> .nc

            # check if cdo was successful
            cdo sinfo ${tmp_paths[$index]}

            # remove original file
            rm $sub_tmp_path
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

echo "Done."