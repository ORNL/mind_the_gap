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
from shapely.testing import assert_geometries_equal

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from auto_tune import Region

class TestRegion:
    def setup_method(self):
        self.points = gpd.read_file('./src/tests/data/test_points.geojson')
        self.bound = gpd.read_file('./src/tests/data/test_bound.geojson')

    def test_init(self):
        reg = Region(self.points, self.bound)

        exp_boundaries = LineString([(-1.2593125, 1.0945),
                                     (-1.1593125, -0.7175),
                                     (0.7886875, -0.7175),
                                     (0.7886875, 1.0945),
                                     (-1.2593125, 1.0945)])

        assert_geometries_equal(reg.boundaries, exp_boundaries)

    def test_make_grid(self):
        pass
