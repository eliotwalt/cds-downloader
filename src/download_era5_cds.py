###
# to download multiple years of a certain variable, u and v components in this case, from ERA5
###

from typing import Union, Optional, Any
import cdsapi
import argparse
from datetime import datetime
import sys
import os
import yaml

import logging

from arrays import n_years_single_var_all_levels, single_year_single_var_all_levels

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

SINGLE_LEVEL_ERA5 = 'reanalysis-era5-single-levels'
PRESSURE_LEVELS_ERA5 = 'reanalysis-era5-pressure-levels'
ALL_DAYS = [
    '01', '02', '03',
    '04', '05', '06',
    '07', '08', '09',
    '10', '11', '12',
    '13', '14', '15',
    '16', '17', '18',
    '19', '20', '21',
    '22', '23', '24',
    '25', '26', '27',
    '28', '29', '30',
    '31',
]
ALL_MONTHS = [
    '01', '02', '03',
    '04', '05', '06',
    '07', '08', '09',
    '10', '11', '12',
]
HOURLY_TIMES = [
    '00:00', '01:00', '02:00',
    '03:00', '04:00', '05:00',
    '06:00', '07:00', '08:00',
    '09:00', '10:00', '11:00',
    '12:00', '13:00', '14:00',
    '15:00', '16:00', '17:00',
    '18:00', '19:00', '20:00',
    '21:00', '22:00', '23:00',
]

def as_list(x: Union[Any, list[Any]]) -> list[Any]:
    if not isinstance(x, list): x = list(x)
    return x

def download(
    path: str,
    years: Union[str, list[str]],
    variables: Union[str, list[str]],
    months: Union[str, list[str]],
    days: Union[str, list[str]],
    times: Union[str, list[str]],
    format: str,
    levels: Optional[Union[int, list[int]]]=None,
    area: Optional[tuple[int, int, int, int]]=None,
    **kwargs
):
    c = cdsapi.Client()
    
    logger.info("Building request arguments...")
    # build request arguments
    request_kwargs = {
        'product_type': ['reanalysis'],
        'year': as_list(years),
        'variable': as_list(variables),
        'month': as_list(months),
        'day': as_list(days),
        'time': as_list(times),
        'format': format 
    }
    
    # add area
    if area is not None:
        request_kwargs["area"] = area
        
    # add level and select resource
    # select cds resource
    if levels is not None:
        level = as_list(levels)
        request_kwargs["level"] = levels
        cds_resource = PRESSURE_LEVELS_ERA5
    else:
        cds_resource = SINGLE_LEVEL_ERA5
        
    # print request arguments
    msg = "cds request:\n"
    msg += f" * resource: {cds_resource}\n"
    for k, v in request_kwargs.items():
        msg += f" * {k}: {v}\n"
    logger.info(msg)
    
    c.retrieve(
        cds_resource,
        request_kwargs,
        target=path
    )
    
    return path

if __name__ == "__main__":
    # p = argparse.ArgumentParser()
    # p.add_argument("--config", type=str, help="path to the configuration file.", required=True)
    # p.add_argument("--host_config", type=str, help="path to the host configuration file.", required=True)
    # args = p.parse_args()
    
    p = argparse.ArgumentParser(description="Download ERA5 data from CDS.")
    p.add_argument("--path", type=str, required=True, help="Path to save the downloaded data.")
    p.add_argument("--years", type=str, nargs='+', required=True, help="Years to download data for.")
    p.add_argument("--variables", type=str, nargs='+', required=True, help="Variables to download.")
    p.add_argument("--months", type=str, nargs='+', default=ALL_MONTHS, help="Months to download data for.")
    p.add_argument("--days", type=str, nargs='+', default=ALL_DAYS, help="Days to download data for.")
    p.add_argument("--times", type=str, nargs='+', default=HOURLY_TIMES, help="Times to download data for.")
    p.add_argument("--format", type=str, default="netcdf", help="Format of the downloaded data.")
    p.add_argument("--levels", type=int, nargs='+', help="Pressure levels to download data for.")
    p.add_argument("--area", type=float, nargs=4, help="Geographical area to download data for (lat0, lon0, lat1, lon1).")
    args = p.parse_args()

    # check no more than 1 var
    if len(args.variables) > 1:
        raise NotImplementedError("Only one variable can be downloaded at a time.")
    
    # # check if we are in a slurm array
    # index = os.environ.get("SLURM_ARRAY_TASK_ID", None)
    # if index is not None:
    #     index = int(index)
    #     logger.info(f"Running job {index}.")
    #     # get array strategy
    #     array_strategy = config.get("array_strategy")
    #     assert array_strategy, "array_strategy must be specified in the config."
    #     array_strategy_arguments = config.get("array_strategy_arguments", {})
    #     # retrieve job subconfig
    #     logger.info(f"Retrieving job configuration for array strategy: {array_strategy}.")
    #     if array_strategy == "single_year_single_var_all_levels":
    #         config = single_year_single_var_all_levels(config, index=index, **array_strategy_arguments)
    #     elif array_strategy == "n_years_single_var_all_levels":
    #         config = n_years_single_var_all_levels(config, index=index, **array_strategy_arguments)
    #     else:
    #         raise ValueError(f"Unknown array strategy: {array_strategy}.")
        
    # # change output directory to path
    # fn = f"era5_cds_{'-'.join(config['variables'])}-{min(config['years'])}-{max(config['years'])}.nc"
    # config["path"] = os.path.join(config["output_dir"], fn)
        
    # check that year is in 1940-now.year
    for y in args.years:
        now = datetime.now().year
        if int(y) < 1940 or int(y) > now:
            logger.error(f"Year must be between 1940 and {now}.")
            sys.exit()
            
    # check that area is valid (i.e. latitudes in [-90, 90] and longitudes in [-180, 180])
    area = [float(x) for x in args.area]
    if area[0] < -90 or area[0] > 90 or area[2] < -90 or area[2] > 90:
        logger.error("Latitude must be between -90 and 90.")
        sys.exit()
    if area[1] < -180 or area[1] > 180 or area[3] < -180 or area[3] > 180:
        logger.error("Longitude must be between -180 and 180.")
        sys.exit()
    
    path = download(**vars(args))
    sys.exit(0)