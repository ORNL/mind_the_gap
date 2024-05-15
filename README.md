# Mind the Gap

Mind the Gap is an algorithm for detecting gaps in building footprints datasets, with a preference for finsing the rectangular-shaped gaps caused by missing imagery tiles. It works best on country-sized datasets or smaller, or larger datasets divided into smaller chunks.

## Getting started

A Dockerfile and docker-compose.yml are provided, these will help you set up a suitable environment.

The core of the algorithm is found in /src/mind_the_gap.py, but is best implemented using the `auto_tune` module, which will free you from having to guess and check all the parameters. The default parameters in the `Region.run` method workgenerally well but may need a little tweaking.

### Inputs

Mind the Gap requires two inputs: building footprints (or just centroids) and a boundary to the aoi. Currently, it is set up to read these from a database.

### Outputs

Gaps will be stored as a GeoDataFrame of the Region object once `Region.run` has ran.