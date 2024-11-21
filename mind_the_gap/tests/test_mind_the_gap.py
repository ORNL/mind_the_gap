"""Test for functions in Mind the Gap main module"""

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
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mind_the_gap as mtg

@pytest.fixture()
def points():
    _points = gpd.read_file('./mind_the_gap/tests/data/test_points.geojson')
    return _points

@pytest.fixture()
def expected_lat_gaps():
    with open('./mind_the_gap/tests/data/exp_x_gaps.csv') as f:
        _expected_lat_gaps = list(csv.reader(f,quoting=csv.QUOTE_NONNUMERIC))
    return _expected_lat_gaps

@pytest.fixture()
def expected_lon_gaps():
    with open('./mind_the_gap/tests/data/exp_y_gaps.csv') as f:
        _expected_lon_gaps = list(csv.reader(f, quoting=csv.QUOTE_NONNUMERIC))
    return _expected_lon_gaps

@pytest.fixture()
def exp_cluster_inters():
    cluster_inters = \
        gpd.read_file('./mind_the_gap/tests/data/exp_cluster_inters.geojson')
    _exp_cluster_inters = cluster_inters['geometry']
    return _exp_cluster_inters

@pytest.fixture()
def exp_gaps_shapes():
    _exp_gaps_shapes = \
        gpd.read_file('./mind_the_gap/tests/data/expected_gaps_shapes.geojson')
    return _exp_gaps_shapes

@pytest.fixture()
def exp_gaps_points():
    _exp_gaps_points = \
        gpd.read_file('./mind_the_gap/tests/data/expected_gaps_points.geojson')
    return _exp_gaps_points

@pytest.fixture()
def exp_write_points():
    _exp_write_points = \
        gpd.read_file('./mind_the_gap/tests/data/expected_write_points.geojson')
    return _exp_write_points

class TestMindTheGap:
    def test_get_coordinates(self):
        points_d = {'geometry': [Point(1,2), Point(2,1), Point(3,4)]}
        points_gdf = gpd.GeoDataFrame(points_d, crs='EPSG:4326')

        expected = np.array([[1,2],[2,1],[3,4]])
        output_points = mtg.get_coordinates(points_gdf)

        assert_array_equal(output_points, expected)

    def test_into_the_bins(self):
        _points = np.array([[1,2],[2,1],[3,4]])
        points_in_bins, y_bins, x_bins = \
            mtg.into_the_bins(_points, x_bin_size=0.5, y_bin_size=0.5)

        expexted_points_in_bins = np.array([[1,2,2,0],
                                            [2,1,0,2],
                                            [3,4,6,4]])
        expected_x_bins = np.array([1,1.5,2,2.5,3,3.5,4])
        expected_y_bins = np.array([1,1.5,2,2.5,3])

        assert_array_equal(y_bins, expected_y_bins)
        assert_array_equal(x_bins, expected_x_bins)
        assert_array_equal(points_in_bins, expexted_points_in_bins)

    def test_find_lat_gaps(self, points, expected_lat_gaps):
        point_coords = mtg.get_coordinates(points)
        stacked, x_bins, y_bins = mtg.into_the_bins(point_coords,0.061,0.07)
        lat_gaps = mtg.find_lat_gaps(stacked, x_bins, 0.3)

        assert isinstance(lat_gaps, list)
        assert isinstance(lat_gaps[0], list)
        assert_array_equal(lat_gaps, expected_lat_gaps)

    def test_find_lon_gaps(self, points, expected_lon_gaps):
        point_coords = mtg.get_coordinates(points)
        stacked, x_bins, y_bins = mtg.into_the_bins(point_coords,0.061,0.07)
        lon_gaps = mtg.find_lon_gaps(stacked, y_bins, 0.3)

        assert isinstance(lon_gaps, list)
        assert isinstance(lon_gaps[0], list)
        assert_array_equal(lon_gaps, expected_lon_gaps)

    def test_does_cross(self):
        xg1 = [0, 1, 0, 1, 0, 3, 2]
        yg1 = [0, 2, 0, 0, 0, 3, 3]

        cross_1 = mtg.does_cross(xg1,yg1)

        xg2 = [0, 1, 0, 4, 0, 5, 1]
        yg2 = [0, 2, 0, 1, 1, 3, 2]
        cross_2 = mtg.does_cross(xg2,yg2)

        assert cross_1 is True and cross_2 is False

    def test_find_intersections(self):
        lines = [LineString([[1,0],[1,3]]),
                 LineString([[0,1],[4,1]]),
                 LineString([[0,2],[5,2]]),
                 LineString([[2,-1],[2,6]])]

        intersections = mtg.find_intersections(lines)

        expected_intersections = [Point(1,1),
                                  Point(1,2),
                                  Point(1,1),
                                  Point(2,1),
                                  Point(1,2),
                                  Point(2,2),
                                  Point(2,1),
                                  Point(2,2)]

        assert_array_equal(intersections, expected_intersections)

    def test_intersection_filter(self, points):
        point_coords = mtg.get_coordinates(points)
        stacked, x_bins, y_bins = mtg.into_the_bins(point_coords,0.061,0.07)
        lat_gaps = mtg.find_lat_gaps(stacked, x_bins, 0.3)
        lon_gaps = mtg.find_lon_gaps(stacked, y_bins, 0.3)

        x_gaps, y_gaps = mtg.intersection_filter(lat_gaps,
                                                 lon_gaps,
                                                 3,
                                                 3)

        assert len(lat_gaps) == 11
        assert len(x_gaps) == 11
        assert len(lon_gaps) == 21
        assert len(y_gaps) == 19

    def test_find_clusters(self, points):
        point_coords = mtg.get_coordinates(points)
        stacked, x_bins, y_bins = mtg.into_the_bins(point_coords,0.061,0.07)
        lat_gaps = mtg.find_lat_gaps(stacked, x_bins, 0.3)
        lon_gaps = mtg.find_lon_gaps(stacked, y_bins, 0.3)

        x_gaps, y_gaps = mtg.intersection_filter(lat_gaps,
                                                 lon_gaps,
                                                 3,
                                                 3)

        all_gaps, ids, gap_clusters, split_ind = \
            mtg.find_clusters(x_gaps, y_gaps)

        expected_ids = np.array([1,1,1,1,1,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,2,1, \
                                 2,2,2,2,2,2,2,2])
        expected_gap_clusters = [[11,0,1,12,2,13,3,14,4,15,16,17,18,19,21],
                                 [11,0,1,12,2,13,3,14,4,15,16,17,18,19,21],
                                 [20,5,6,22,7,23,8,24,9,25,10,26,27,28,29]]

        assert_array_equal(all_gaps,np.vstack([x_gaps,y_gaps]))
        assert_array_equal(ids, expected_ids)
        assert_array_equal(gap_clusters,expected_gap_clusters)
        assert split_ind == 11

    def test_cluster_intersections(self, points, exp_cluster_inters):
        point_coords = mtg.get_coordinates(points)
        stacked, x_bins, y_bins = mtg.into_the_bins(point_coords,0.061,0.07)
        lat_gaps = mtg.find_lat_gaps(stacked, x_bins, 0.3)
        lon_gaps = mtg.find_lon_gaps(stacked, y_bins, 0.3)

        x_gaps, y_gaps = mtg.intersection_filter(lat_gaps,
                                                 lon_gaps,
                                                 3,
                                                 3)

        all_gaps, ids, gap_clusters, split_ind = \
            mtg.find_clusters(x_gaps, y_gaps)

        all_gap_segments = []
        for i, g in enumerate(all_gaps):
            if i < split_ind:
                seg = [(g[1],g[3]),(g[1],g[5])]
            elif i >= split_ind:
                seg = [(g[3],g[1]),(g[5],g[1])]
            all_gap_segments.append(seg)

        x_clusters = [0,1,2,3,4]
        y_clusters = [11,12,13,14,15,16,17,18,19,21]

        inters = mtg.cluster_intersections(x_clusters,
                                           y_clusters,
                                           all_gap_segments)

        inters_gs = gpd.GeoSeries(inters,crs='EPSG:4326')

        assert isinstance(inters, list)
        assert isinstance(inters[0],Point)
        assert_geoseries_equal(inters_gs,
                               exp_cluster_inters,
                               check_less_precise=True)

    def test_generate_alpha_polygons(self,
                                     points,
                                     exp_gaps_shapes,
                                     exp_gaps_points):
        point_coords = mtg.get_coordinates(points)
        stacked, x_bins, y_bins = mtg.into_the_bins(point_coords,0.061,0.07)
        lat_gaps = mtg.find_lat_gaps(stacked, x_bins, 0.3)
        lon_gaps = mtg.find_lon_gaps(stacked, y_bins, 0.3)

        x_gaps, y_gaps = mtg.intersection_filter(lat_gaps,
                                                 lon_gaps,
                                                 3,
                                                 3)

        all_gaps, ids, gap_clusters, split_ind = \
            mtg.find_clusters(x_gaps, y_gaps)

        all_gap_segments = []
        for i, g in enumerate(all_gaps):
            if i < split_ind:
                seg = [(g[1],g[3]),(g[1],g[5])]
            elif i >= split_ind:
                seg = [(g[3],g[1]),(g[5],g[1])]
            all_gap_segments.append(seg)

        x_clusters = [[0,1,2,3,4],[0,1,2,3,4],[5,6,7,8,9,10]]
        y_clusters = [[11,12,13,14,15,16,17,18,19,21],
                      [11,12,13,14,15,16,17,18,19,21],
                      [20,22,23,24,25,26,27,28,29]]

        shapes, points = mtg.generate_alpha_polygons(x_clusters,
                                                     y_clusters,
                                                     all_gap_segments,
                                                     18)

        assert_geodataframe_equal(shapes,
                                  exp_gaps_shapes,
                                  check_less_precise=True)
        assert_geodataframe_equal(points,
                                  exp_gaps_points,
                                  check_less_precise=True)

    def test_mind_the_gap(self,
                          points,
                          exp_gaps_shapes,
                          exp_gaps_points,
                          exp_write_points):
        gaps_shapes = mtg.mind_the_gap(points,
                                       0.061,
                                       0.07,
                                       0.3,
                                       0.3,
                                       3,
                                       3,
                                       alpha=18)

        assert_geodataframe_equal(gaps_shapes,
                                  exp_gaps_shapes,
                                  check_less_precise=True)

        gaps_shapes, gaps_points = mtg.mind_the_gap(points,
                                                    0.061,
                                                    0.07,
                                                    0.3,
                                                    0.3,
                                                    3,
                                                    3,
                                                    alpha=18,
                                                    cluster_points=True)

        assert_geodataframe_equal(gaps_shapes,
                                  exp_gaps_shapes,
                                  check_less_precise=True)
        assert_geodataframe_equal(gaps_points,
                                  exp_gaps_points,
                                  check_less_precise=True)

        gaps_points = mtg.mind_the_gap(points,
                                       0.061,
                                       0.07,
                                       0.3,
                                       0.3,
                                       3,
                                       3,
                                       alpha=18,
                                       write_points=True)

        assert_geodataframe_equal(gaps_points,
                                  exp_write_points,
                                  check_less_precise=True)
