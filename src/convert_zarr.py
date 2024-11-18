import argparse
import xarray as xr
import os
import sys
import shutil

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def write_zarr(ds: xr.Dataset, path: str, exist_ok: bool=False):
    """
    Write a dataset to a zarr file.
    
    Args:
        ds: xarray.Dataset
            The dataset to write.
        path: str
            The path to write the dataset to.
        exist_ok: bool
            Whether to overwrite the file if it already exists
    """
    
    path = os.path.abspath(path)
    
    if os.path.exists(path):
        if exist_ok:
            shutil.rmtree(path)
        else:
            raise FileExistsError(f"File already exists: {path}")
        
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
        
    ds.to_zarr(path, mode="w")
    
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input_file", type=str, help="Input file, usually a .nc file", required=True)
    p.add_argument("--output_file", type=str, help="Output file, usually a .zarr file", required=True)
    args = p.parse_args()
    
    logger.info(f"Reading dataset from {args.input_file}")
    ds = xr.open_dataset(args.input_file, chunks={})
    
    logger.info(f"Cleaning up dataset\n{ds.data_vars}") 
    if "valid_time" in ds and "time" not in ds:
        ds = ds.rename({"valid_time": "time"})
        
    if "lat" in ds and "latitude" not in ds:
        ds = ds.rename({"lat": "latitude"})
        
    if "lon" in ds and "longitude" not in ds:
        ds = ds.rename({"lon": "longitude"})
        
    if "valid_time_bnds" in ds.data_vars:
        ds = ds.drop_vars("valid_time_bnds")
    logger.info(f"Cleaned dataset\n{ds.data_vars}")
    
    logger.info(f"Writing dataset to {args.output_file}")
    write_zarr(ds, args.output_file, exist_ok=True)
    logger.info("Done!")
    
    sys.exit(0)