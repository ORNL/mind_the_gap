"""Test the chainage module"""

import sys
import os

import geopandas as gpd
from geopandas.testing import assert_geoseries_equal
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from chainage import chainage

def test_chainage():
    expected_chain = gpd.read_file('./src/tests/data/slovakia_chain.geojson')
    expected_chain = expected_chain['geometry']

    bound = gpd.read_file('./src/tests/data/slovakia_bound.geojson')

    test_chain = chainage(bound.boundary[0],0.01)

    assert type(expected_chain) == type(test_chain)

    assert_geoseries_equal(expected_chain,
                           test_chain,
                           check_less_precise=True,
                           check_geom_type=True)
