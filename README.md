# Aggregation Scripts

These scripts generate the KEGG/COG/Pfam aggregations that are used for search.

A container hosted on Spin runs the `agg.sh` script, which performs the aggregations periodically (once every 4 hours, by default).

> [!NOTE]  
> The container image is hosted [here](https://github.com/microbiomedata/nmdc-aggregator/pkgs/container/nmdc-aggregator).

## Test scripts

From the root directory of the repository, run
```
$ pytest
=========================================================== test session starts ============================================================
platform darwin -- Python 3.8.16, pytest-7.4.3, pluggy-1.3.0
rootdir: /NMDC/nmdc-aggregator
collecting ...
collected 2 items
tests/test_generate_functional_agg.py ..                                                                                             [100%]

============================================================ 2 passed in 3.92s =============================================================

```

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

https://github.com/microbiomedata/nmdc-aggregator/releases
