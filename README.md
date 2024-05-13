# Aggregation Scripts

These scripts generate the KEGG aggregations that are used for KEGG search.

A container hosted on Spin runs the `agg.sh` script, which performs the aggregations periodically (once every 4 hours, by default).

> [!NOTE]  
> The container image is hosted [here](https://github.com/microbiomedata/nmdc-aggregator/pkgs/container/nmdc-aggregator).

## Deployment

Here's how you can build a new version of the container image and push it to the [GitHub Container Registry](https://github.com/microbiomedata/nmdc-aggregator/pkgs/container/nmdc-aggregator):

1. Update the version number in the `VERSION` file.
   > Use "[`major.minor.patch`](https://semver.org/)" format (e.g. "`1.2.3`").
2. On GitHub, create a new Release.
    1. Create a tag.
       > Name it "`v`" followed by the version number in the `VERSION` file (e.g. "`v1.2.3`").
    2. Click the "Generate release notes" button.
    3. Leave the Release title empty (so GitHub reuses the tag name as the Release title).
    4. Click the "Publish release" button.
3. Wait 3-4 minutes for the container image to appear on the [GitHub Container Registry](https://github.com/microbiomedata/nmdc-aggregator/pkgs/container/nmdc-aggregator).
   > Taking a long time? Check the "Actions" tab on GitHub to see the status of the GitHub Actions workflow that builds the image.

Now that the container image is hosted there, you can configure a Spin workload to run it.

## Configuration

### Environment variables

- `MONGO_URL`: Full Mongo URI for connecting to the Mongo database (no default)
- `LOG_FILE`: Path to file to which logs will be appended (Default: `/tmp/agg.log`)
- `POLL_TIME`: Number of seconds to sleep between each run (Default: `14400`, which is 4 hours)
- `NMDC_BASE_URL`: Base URL to access the data (Default: `https://data.microbiomedata.org/data`)
- `NMDC_BASE_PATH`: Base path to the data on disk (Default: `/global/cfs/cdirs/m3408/results`)

## Release Notes

### v1.0.3

- Add some log statements to help with debugging

### v1.0.2

- Configure GitHub Actions to build and push container images to GitHub Container Registry
- Update `Dockerfile` so that the entrypoint shell script is executable

### v1.0.1

- Fix a bug in the metaP script

### v1.0.0

- Initial release
