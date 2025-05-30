"""Test the Region class in auto_tune"""

import geopandas as gpd
from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.testing import assert_geometries_equal
import pytest

from mind_the_gap.auto_tune import Region

@pytest.fixture()
def points():
    _points = gpd.read_file('./tests/data/test_points.gpkg')
    return _points

@pytest.fixture()
def bound():
    _bound = gpd.read_file('./tests/data/test_bound.gpkg')
    return _bound

@pytest.fixture()
def exp_grid():
    _exp_grid = gpd.read_file('./tests/data/exp_grid.gpkg')
    _exp_grid = _exp_grid.set_index('index').rename_axis(None)
    return _exp_grid

@pytest.fixture()
def exp_mind_gaps():
    _exp_mind_gaps = gpd.read_file('./tests/data/exp_mind_gaps.gpkg')
    return _exp_mind_gaps

@pytest.fixture()
def exp_auto_gaps():
    _exp_auto_gaps = gpd.read_file('./tests/data/exp_auto_gaps.gpkg')
    return _exp_auto_gaps

@pytest.fixture()
def exp_parallel_gaps():
    _exp_parallel_gaps=gpd.read_file('./tests/data/exp_parallel_gaps.gpkg')
    return _exp_parallel_gaps

@pytest.fixture()
def exp_all_points():
    _exp_all_points = gpd.read_file('./tests/data/exp_points.gpkg')
    return _exp_all_points

class TestRegion:
    @pytest.fixture(autouse=True)
    def _test_points(self, points):
        self.points = points

    @pytest.fixture(autouse=True)
    def _test_bound(self, bound):
        self.bound = bound

    def test_init(self, exp_all_points):
        reg = Region(self.points, self.bound)

        exp_boundaries = LineString([(-1.2593125, 1.0945),
                                     (-1.1593125, -0.7175),
                                     (0.7886875, -0.7175),
                                     (0.7886875, 1.0945),
                                     (-1.2593125, 1.0945)])

        assert_geometries_equal(reg.boundaries, exp_boundaries)

        assert_geodataframe_equal(reg.all_points_gdf, exp_all_points)

    def test_make_grid(self, exp_grid):
        reg = Region(self.points, self.bound, grid_size = 0.02)
        reg.make_grid(size = 0.02)

        assert_geodataframe_equal(reg.grid,
                                  exp_grid,
                                  check_index_type=False,
                                  check_less_precise=True)

    def test_mind(self, exp_mind_gaps):
        reg = Region(self.points, self.bound)
        reg.mind(0.063, 2, 3, 18)

        assert_geodataframe_equal(reg.gaps, exp_mind_gaps)

    def test_fit_check(self):
        reg = Region(self.points, self.bound, grid_size=0.05)
        reg.mind(0.063, 2, 3, 18)
        fit = reg.fit_check(0.07,0.2,0.8)

        assert fit

        fit = reg.fit_check(0.07,0.7,0.8)

        assert fit is False

    def test_run(self, exp_auto_gaps):
        reg = Region(self.points, self.bound, grid_size=0.05)
        reg.run(area_ceiling=0.5)

        assert_geodataframe_equal(reg.gaps, exp_auto_gaps)

    def test_parallel_run(self, exp_auto_gaps):
        reg = Region(self.points, self.bound, grid_size=0.05)

        reg.parallel_run(0.07,0.2,0.5,0.1,0.025,2,(2,3,4),20)

        assert_geodataframe_equal(reg.gaps, exp_auto_gaps)

    def test_run_parallel(self, exp_parallel_gaps):
        reg = Region(self.points, self.bound, grid_size=0.05)

        reg.run_parallel(tile_size=0.8)

        assert_geodataframe_equal(reg.gaps, exp_parallel_gaps)
