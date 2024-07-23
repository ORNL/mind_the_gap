"""Test for functions in Mind the Gap main module"""

import os
import sys

import geopandas as gpd
import numpy as np
from shapely.geometry import Point

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mind_the_gap

class TestMindTheGap:
    def test_get_coordinates(self):
        points_d = {'geometry': [Point(1,2), Point(2,1), Point(3,4)]}
        points_gdf = gpd.GeoDataFrame(points_d, crs='EPSG:4326')

        expected = np.array([[1,2],[2,1],[3,4]])
        output_points = mind_the_gap.get_coordinates(points_gdf)

        np.testing.assert_array_equal(output_points, expected)