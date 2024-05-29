# Mind the Gap

Mind the Gap is an algorithm for detecting gaps in building footprints datasets, with a preference for finsing the rectangular-shaped gaps caused by missing imagery tiles. It works best on country-sized datasets or smaller, or larger datasets divided into smaller chunks.

## Getting started

A Dockerfile and docker-compose.yml are provided, these will help you set up a suitable environment.

The core of the algorithm is found in /src/mind_the_gap.py, but is best implemented using the `auto_tune` module, which will free you from having to guess and check all the parameters. The default parameters in the `Region.run` method workgenerally well but may need a little tweaking.

### Inputs

Mind the Gap requires two inputs: building footprints (or just centroids) and a boundary to the aoi. Currently, it is set up to read these from a database.

### Outputs

Gaps will be stored as a GeoDataFrame of the Region object once `Region.run` has ran.

## What's new

#### Version 2.0 brings two significant updates:

The `auto_tune` module allows you to get some pretty good gaps without having to play around with too many parameters

`run_tiles.py` Provides a process for executing `auto_tune` on a large tiled dataset stored in a PostGIS database.

#### Version 2.0.1:
Improved logging in `run_tiles.py`

#### Version 2.1.0:
- Deprecated the 'rim polygons' feature of `mind_the_gap.mind_the_gap`
- Better avoids accidentally dropping the buildings table by automatically constructing the name of the buildigns table
- Saves `auto_tune.run` parameters to the log
- Pass `auto_tune.run` parameters to `run_region` as parameters instead of hardcoding (i.e. everything that needs to be changed for data source, destination, parameters, etc. is in the `if __name__ == "__main__":` block of `run_tiles.py`).
- Set logging level to 20 (info)
- Add option to resume or start new run

#### Version 2.1.1:
Bug patches