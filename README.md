# Download ERA5 from the new CDS API 

## Basic usage
Create a configuration file similar to the ones in `configs` and a host config (see `configs/hosts/snellius.conf`). Then, run
```bash
./slurm/launch.sh --config ${CONFIG} --host_config ${HOST_CONFIG}
```
This will download the required data from CDS, apply basic preprocessing (only resampling at the moment), and save the result as zarr. Intermediary data is written to scratch and automatically cleaned. 

## A note on parallelism
We generate an array with one job per variable. Within these jobs, the data is requested yearly and at most 16 (hard-coded in `./slurm/launch.sh`) requests are sent concurrently. This is because the CDS API is quite slow and requests are queued anyways. 

## To do
- [ ] add support for regridding
- [ ] add support for variable pairs (e.g. `u` and `v` at the same time to compute `stream`)