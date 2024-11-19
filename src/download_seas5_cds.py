from typing import Union, Optional, Any
import cdsapi
import argparse
import pandas as pd
from datetime import datetime
import sys
import os
import yaml

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def generate_lead_times_hours(start: pd.Timedelta, end: pd.Timedelta, freq: pd.Timedelta) -> list[str]:
    # Convert to hours
    start_h = int(start.total_seconds() / 3600)
    end_h = int(end.total_seconds() / 3600)
    freq_h = int(freq.total_seconds() / 3600)
    
    # Get lead times
    lead_times = []
    for h in range(start_h, end_h+1, freq_h):
        lead_times.append(f"{h:03}")
    return lead_times

def as_list(x: Union[Any, list[Any]]) -> list[Any]:
    if not isinstance(x, list): x = list(x)
    x = [str(i) for i in x]
    return x

def pdtimedelta(x: str) -> pd.Timedelta:
    try:
        return pd.Timedelta(x)
    except:
        raise argparse.ArgumentTypeError(f"Invalid timedelta: {x}")

SINGLE_LEVEL_ERA5 = 'seasonal-original-single-levels'
PRESSURE_LEVELS_ERA5 = 'seasonal-original-pressure-levels'
ALL_DAYS = ["01"]
ALL_MONTHS = [
    '01', '02', '03',
    '04', '05', '06',
    '07', '08', '09',
    '10', '11', '12',
]
SEAS5_FREQ = pd.Timedelta("6h")

def download(
    path: str,
    years: list[str],
    variables: list[str],
    months: list[str],
    days: list[str],
    lead_times: list[str],
    format: str,
    levels: Optional[list[int]] = None,
    area: Optional[list[float]] = None,
    **kwargs
):
    c = cdsapi.Client()
    
    logger.info("Building request arguments")
    
    request_kwargs = {
        "originating_centre": "ecmwf",
        "system":  "51",
        "variable": as_list(variables),
        "year": as_list(years),
        "month": as_list(months),
        "day": as_list(days),
        "leadtime_hour": lead_times,
        "data_format": format,
    }
    
    # add area
    if area is not None:
        request_kwargs["area"] = area
        
    # add level and select resource
    # select cds resource
    if levels is not None:
        levels = as_list(levels)
        request_kwargs["pressure_level"] = levels
        cds_resource = PRESSURE_LEVELS_ERA5
    else:
        cds_resource = SINGLE_LEVEL_ERA5
        
    # print request arguments
    msg = "cds request:\n"
    msg += f" * resource: {cds_resource}\n"
    for k, v in request_kwargs.items():
        msg += f" * {k}: {v}\n"
    msg += f" * path: {path}"
    logger.info(msg)
        
    c.retrieve(
        cds_resource,
        request_kwargs,
        target=path
    )
    
    return path

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Download ERA5 data from CDS.")
    p.add_argument("--path", type=str, required=True, help="Path to save the downloaded data.")
    p.add_argument("--years", type=str, nargs='+', required=True, help="Years to download data for.")
    p.add_argument("--lead_time_start", type=pdtimedelta, required=True, help="Start of the lead time.")
    p.add_argument("--lead_time_end", type=pdtimedelta, required=True, help="End of the lead time.")
    p.add_argument("--variables", type=str, nargs='+', required=True, help="Variables to download.")
    p.add_argument("--area", type=float, nargs=4, help="Geographical area to download data for (lat0, lon0, lat1, lon1).", required=False)
    # defaults
    p.add_argument("--lead_time_freq", type=pdtimedelta, default=SEAS5_FREQ, help="Frequency of the lead times.")
    p.add_argument("--months", type=str, nargs='+', default=ALL_MONTHS, help="Months to download data for.")
    p.add_argument("--days", type=str, nargs='+', default=ALL_DAYS, help="Days to download data for.", choices=ALL_DAYS)
    p.add_argument("--format", type=str, default="netcdf", help="Format of the downloaded data.")
    p.add_argument("--levels", type=int, nargs='+', help="Pressure levels to download data for.")
    args = p.parse_args()
    
    # check no more than 1 var
    if len(args.variables) > 1:
        raise NotImplementedError("Only one variable can be downloaded at a time.")
        
    # check that year is in 1940-now.year
    for y in args.years:
        now = datetime.now().year
        if int(y) < 1981 or int(y) > now:
            logger.error(f"Year must be between 1940 and {now}.")
            sys.exit()
            
    # check that area is valid (i.e. latitudes in [-90, 90] and longitudes in [-180, 180])
    if args.area is not None:
        area = [float(x) for x in args.area]
        if area[0] < -90 or area[0] > 90 or area[2] < -90 or area[2] > 90:
            logger.error("Latitude must be between -90 and 90.")
            sys.exit()
        if area[1] < -180 or area[1] > 180 or area[3] < -180 or area[3] > 180:
            logger.error("Longitude must be between -180 and 180.")
            sys.exit()
        
    # generate lead times    
    lead_times = generate_lead_times_hours(args.lead_time_start, 
                                           args.lead_time_end, 
                                           args.lead_time_freq)
    
    kwargs = vars(args)
    kwargs["lead_times"] = lead_times
    
    path = download(**vars(args))
    sys.exit(0)
