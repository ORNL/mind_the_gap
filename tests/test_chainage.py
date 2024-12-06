"""Test the chainage module"""

import sys
import os

import pytest
import geopandas as gpd
from geopandas.testing import assert_geoseries_equal
from mind_the_gap.chainage import chainage

@pytest.fixture()
def bound():
    _bound = gpd.read_file('./tests/data/slovakia_bound.geojson')
    return _bound

@pytest.fixture()
def chain():
    _chain = gpd.read_file('./tests/data/slovakia_chain.geojson')
    _chain = _chain['geometry']
    return _chain

def test_chainage(bound, chain):
    expected_chain = chain

    test_chain = chainage(bound.boundary[0],0.01)

    assert type(expected_chain) == type(test_chain)

    assert_geoseries_equal(expected_chain,
                           test_chain,
                           check_less_precise=True,
                           check_geom_type=True)
