"""Test the chainage module"""

import sys
import os

import pytest
import geopandas as gpd
from geopandas.testing import assert_geoseries_equal
from geopandas.testing import assert_geodataframe_equal
from mind_the_gap.chainage import chainage
from mind_the_gap.chainage import prepare_points

@pytest.fixture()
def bound():
    _bound = gpd.read_file('./tests/data/slovakia_bound.gpkg')
    return _bound

@pytest.fixture()
def chain():
    _chain = gpd.read_file('./tests/data/slovakia_chain.gpkg')
    _chain = _chain['geometry']
    return _chain

@pytest.fixture()
def test_points():
    _points = gpd.read_file('./tests/data/test_points.gpkg')
    return _points

@pytest.fixture
def test_bound():
    _bound = gpd.read_file('./tests/data/test_bound.gpkg')
    return _bound

@pytest.fixture
def exp_points():
    _exp_points = gpd.read_file('./tests/data/exp_points.gpkg')
    return _exp_points

class TestChainage:
    def test_chainage(self, bound, chain):
        expected_chain = chain

        test_chain = chainage(bound.boundary[0],0.01)

        assert type(expected_chain) == type(test_chain)

        assert_geoseries_equal(expected_chain,
                               test_chain,
                               check_less_precise=True,
                               check_geom_type=True)

    def test_prepare_points(self, test_points, test_bound, exp_points):
        all_points_gdf = prepare_points(test_points,test_bound,0.01)

        assert_geodataframe_equal(exp_points, all_points_gdf)
