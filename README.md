# Mind the Gap

Mind the Gap is an algorithm for detecting gaps in building footprints datasets, with a preference for finding the rectangular-shaped gaps caused by missing imagery tiles. It works best on country-sized datasets or smaller, or larger datasets divided into smaller chunks.

## Getting started

A Dockerfile and requirements are included, and can help you set up a suitable container. 

Alternatively, you can use poetry by running:
`pip install poetry`
and then
`poetry install`
from the root directory of the repository.

The core of the algorithm is found in mind_the_gap.py, and you can use `mind_the_gap` function to directly run the algorith. For an easier start, you can use the `auto_tune` module, which will free you from having to guess and check all the parameters. This will do a decent job of tuning all the paramters, but you may still need to tweak things. The default parameters in the `Region.run` method work generally well but may need a little tweaking. `Region.run_parallel` is advantageous for large datasets as it allows for parallel processing.

### Inputs

Mind the Gap requires two inputs: building footprints (or just their centroids) and a boundary to the aoi. These are best handled using GeoPandas

### Outputs

Gaps will be stored as a GeoDataFrame of the Region object once `Region.run` has ran.
