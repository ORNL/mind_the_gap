"""Test the Region class in auto_tune"""

import os
import sys
import csv

import geopandas as gpd
from geopandas.testing import assert_geoseries_equal
from geopandas.testing import assert_geodataframe_equal
import numpy as np
from numpy.testing import assert_array_equal
from shapely.geometry import Point
from shapely.geometry import LineString

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from auto_tune import Region
