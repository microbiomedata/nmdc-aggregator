# Aggregation Scripts

These scripts generate the KEGG aggregations that are used for KEGG search.

A container running on Spin runs the `agg.sh` script, which performs the aggregations periodically (once every 4 hours, by default).

The container image is currently hosted at: https://hub.docker.com/r/microbiomedata/agg

## Configuration

### Environment variables

- `MONGO_URL`: Full Mongo URI for connecting to the Mongo database (no default)
- `LOG_FILE`: Path to file to which logs will be appended (Default: `/tmp/agg.log`)
- `POLL_TIME`: Number of seconds to sleep between each run (Default: `14400`, which is 4 hours)
- `NMDC_BASE_URL`: Base URL to access the data (Default: `https://data.microbiomedata.org/data`)
- `NMDC_BASE_PATH`: Base path to the data on disk (Default: `/global/cfs/cdirs/m3408/results`)