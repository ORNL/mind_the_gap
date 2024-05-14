# mind_the_gap



## Getting started

A Dockerfile and docker-compose.yml are provided, these will help you set up a suitable environment.

The core of the algorithm is found in /src/mind_the_gap.py, but is best implemented using the `auto_tune` module, which will free you from having to guess and check all the parameters. The default parameters in the `Region.run` method workgenerally well but may need a little tweaking.

## Inputs

Mind the Gap requires two inputs: building footprints (or just centroids) and a boundary to the aoi. Currently, it is set up to read these from a database.