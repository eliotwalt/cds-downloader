# Download data from the new CDS API 

## Basic usage
Create a configuration file similar to the ones in `configs` and a host config (see `configs/hosts/snellius.conf`). Then, run
```bash
./slurm/launch_{era5|seas5}.sh --config ${CONFIG} --host_config ${HOST_CONFIG}
```
This will download the required data from CDS, apply basic preprocessing (only resampling at the moment), and save the result as zarr. Intermediary data is written to scratch and automatically cleaned. 

## A note on parallelism
We generate an array with one job per variable. Within these jobs, the data is requested in chunks of `YEARS_PER_REQUEST` (for era5) and `MONTHS_PER_REQUEST` (for seas5) (`./slurm/job_{era5|seas5}.sh`) and at most `MAX_CPUS` (hard-coded in `./slurm/launch_{era5|seas5}.sh`) requests are sent concurrently. This is because the CDS API is quite slow and requests are queued anyways. 

Note that the regridding and aggregations are applied to the yearly files before having all of them be merged with `cdo mergetime`. 

## To do
- [x] add support for regridding
- [ ] add support for variable pairs (e.g. `u` and `v` at the same time to compute `stream`)