# Aggregation Scripts

These scripts generate the KEGG aggregations that are used for KEGG search.

A container running on Spin runs the `agg.sh` script, which performs the aggregations periodically (once every 4 hours, by default).

The container image is currently hosted at: https://github.com/microbiomedata/nmdc-aggregator/pkgs/container/nmdc-aggregator

## Configuration

### Environment variables

- `MONGO_URL`: Full Mongo URI for connecting to the Mongo database (no default)
- `LOG_FILE`: Path to file to which logs will be appended (Default: `/tmp/agg.log`)
- `POLL_TIME`: Number of seconds to sleep between each run (Default: `14400`, which is 4 hours)
- `NMDC_BASE_URL`: Base URL to access the data (Default: `https://data.microbiomedata.org/data`)
- `NMDC_BASE_PATH`: Base path to the data on disk (Default: `/global/cfs/cdirs/m3408/results`)

## Release Notes

### 1.0.2

- Configure GitHub Actions to build and push container images to GitHub Container Registry
- Update `Dockerfile` so that the entrypoint shell script is executable

### 1.0.1

- Fix a bug in the metaP script

### 1.0.0

- Initial release
