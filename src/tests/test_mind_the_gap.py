"""Test for functions in Mind the Gap main module"""

import os
import sys

import geopandas as gpd
import numpy as np
from numpy.testing import assert_array_equal
from shapely.geometry import Point
from shapely.geometry import LineString

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mind_the_gap

class TestMindTheGap:
    def test_get_coordinates(self):
        points_d = {'geometry': [Point(1,2), Point(2,1), Point(3,4)]}
        points_gdf = gpd.GeoDataFrame(points_d, crs='EPSG:4326')

        expected = np.array([[1,2],[2,1],[3,4]])
        output_points = mind_the_gap.get_coordinates(points_gdf)

        assert_array_equal(output_points, expected)

    def test_into_the_bins(self):
        points = np.array([[1,2],[2,1],[3,4]])
        points_in_bins, y_bins, x_bins = \
            mind_the_gap.into_the_bins(points, x_bin_size=0.5, y_bin_size=0.5)

        expexted_points_in_bins = np.array([[1,2,2,0],
                                            [2,1,0,2],
                                            [3,4,6,4]])
        expected_x_bins = np.array([1,1.5,2,2.5,3,3.5,4])
        expected_y_bins = np.array([1,1.5,2,2.5,3])

        assert_array_equal(y_bins, expected_y_bins)
        assert_array_equal(x_bins, expected_x_bins)
        assert_array_equal(points_in_bins, expexted_points_in_bins)

    def test_find_lat_gaps(self):
        pass

    def test_find_lon_gaps(self):
        pass

    def test_does_cross(self):
        xg1 = [0, 1, 0, 1, 0, 3, 2]
        yg1 = [0, 2, 0, 0, 0, 3, 3]

        cross_1 = mind_the_gap.does_cross(xg1,yg1)

        xg2 = [0, 1, 0, 4, 0, 5, 1]
        yg2 = [0, 2, 0, 1, 1, 3, 2]
        cross_2 = mind_the_gap.does_cross(xg2,yg2)

        assert cross_1 is True and cross_2 is False

    def test_find_intersections(self):
        lines = [LineString([[1,0],[1,3]]),
                 LineString([[0,1],[4,1]]),
                 LineString([[0,2],[5,2]]),
                 LineString([[2,-1],[2,6]])]

        intersections = mind_the_gap.find_intersections(lines)

        expected_intersections = [Point(1,1),
                                  Point(1,2),
                                  Point(1,1),
                                  Point(2,1),
                                  Point(1,2),
                                  Point(2,2),
                                  Point(2,1),
                                  Point(2,2)]

        assert_array_equal(intersections, expected_intersections)
