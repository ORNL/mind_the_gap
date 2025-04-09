"""Execute Mind the Gap with automated parameter selection"""

import warnings
import multiprocessing as mp
from itertools import repeat

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely import geometry

import mind_the_gap.mind_the_gap as mtg
from mind_the_gap.chainage import prepare_points

class Region:
    """The region to run Mind the Gap on.
    
    Loads all necessary information and contains methods for automatically
    tuning parameters to generate a good no data mask.
    
    """

    def __init__(self,
                 buildings,
                 boundary,
                 grid_size=0.02):
        """The region we are running Mind the Gap on.

        Loads in building and boundary data, builds chainage, etc. Everything
        needed to run MtG and builds grid to autotune parameters to.

        Parameters
        ----------
        buildings : GeoDataFrame
            Input building centroids
        boundary : GeoDataFrame
            Polygon of region boundary
        grid_size : float
            Size of the grid used to find empty space

        """

        self.buildings = buildings
        self.boundary = boundary
        self.gaps = []
        self.grid = []
        self.in_gaps_ratio = 0
        self.area_ratio = 0
        self.all_points_gdf = None

        self.boundaries_shape = self.boundary
        self.boundaries = ([self.boundary.boundary][0])[0]

        # Generate chainage and combine with building points
        self.all_points_gdf = prepare_points(self.buildings,
                                             self.boundary,
                                             0.01)

        # Make grid
        self.make_grid(size=grid_size)

    def make_grid(self, size=0.02):
        """Make grid to check gap completeness.
        
        Parameters
        ----------
        size : float
            Size of each grid cell
            
        """

        bounds = self.boundaries.bounds

        min_x = bounds[0]
        min_y = bounds[1]
        max_x = bounds[2]
        max_y = bounds[3]

        cols = list(np.arange(min_x, max_x + size, size))
        rows = list(np.arange(min_y, max_y + size, size))

        polygons = []
        for x in cols[:-1]:
            for y in rows[:-1]:
                polygons.append(geometry.Polygon([(x,y),
                                                  (x + size, y),
                                                  (x + size, y + size),
                                                  (x, y + size)]))
        grid = gpd.GeoDataFrame({'geometry':polygons},crs='EPSG:4326')

        # Clip grid to region extent
        grid = gpd.clip(grid, self.boundaries_shape)

        self.grid = grid

    def mind(self, w, ln_ratio, i, a):
        """Execute mind the gap
    
        Parameters
        ----------
        w : float
            Width of the strips
        ln_ratio : float
            Ratio of strip length to width
        i : int
            Minimum number of intersections
        a : int
            Alpha value for alphashapes
        
        """

        # Execute mind the gap
        l = w * ln_ratio + (w / 4)

        self.gaps = mtg.mind_the_gap(self.all_points_gdf,
                                     w,
                                     w,
                                     l,
                                     l,
                                     i,
                                     i,
                                     alpha=a)

    def fit_check(self, build_thresh, area_floor, area_ceiling):
        """Checks how well the gaps fit the data
        
        Parameters
        ----------
        build_thresh : float
            Maximum proportion of buildings allowed in the gap mask
        area_floor : float
            Minimum area of open space gaps must fill
        area_ceiling : float
            Maximum area of open space gaps are allowed to fill
        
        Returns
        -------
        boolean
            True if gaps satisfy requirements, false if not

        """

        # First things first, check to make sure gaps aren't empty
        if self.gaps is None:
            return False
        elif len(self.gaps) < 1:
            return False
        else:
            # Check proportion of buildings in the gaps
            buildings_series = self.buildings.geometry
            in_gaps = self.buildings.sjoin(self.gaps, how='inner')

            self.in_gaps_ratio = in_gaps.size / buildings_series.size

            # Get open space or grid cells
            joined_grid = gpd.sjoin(self.grid,
                                    self.all_points_gdf,
                                    how='left',
                                    predicate='contains')
            empty_grid = joined_grid.loc[joined_grid['index_right'].isna()]

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore",
                                        message =
                                        "Geometry is in a geographic CRS.")
                empty_grid_area = sum(empty_grid['geometry'].area)

            gaps_in_empty_grid = gpd.overlay(empty_grid,
                                             self.gaps,
                                             how='intersection',
                                             keep_geom_type=True)
            gaps_in_empty_grid = gaps_in_empty_grid.unary_union

            if gaps_in_empty_grid is None:
                return False

            gaps_in_empty_grid_area = gaps_in_empty_grid.area
            self.area_ratio = gaps_in_empty_grid_area / empty_grid_area

            if (self.in_gaps_ratio < build_thresh) and \
                ((self.area_ratio > area_floor) and \
                 (self.area_ratio < area_ceiling)):
                return True
            else:
                return False

    def run(self,
            build_thresh=0.07,
            area_floor=0.2,
            area_ceiling=0.8,
            _w=0.1,
            _w_step=0.025,
            _ln_ratio=2,
            _is=(2,3,4),
            _a=20,):
        """Iterates through parameters until a good set is settled on"
        
        Parameters
        ----------
        build_thresh : float
            Maximum proportion of buildings allowed in gaps
        area_floor : float
            Minimum proportion of open space to be filled by gaps
        area_cieling : float
            Maximum proportion of open space to be filled by gaps
        _w : float
            Starting width value
        _w_step : float
            Value to update _w by each iteration
        _ln_ratio : float
            Ratio of minimum strip length to width
        _is : array_like
            1-d set of numbers of interesections to try
        _a : int
            Alpha value for alpha-shapes

        """

        past_gaps = []
        past_params = []

        while True:
            if self.buildings.geometry.size == 0:
                self.gaps = self.boundaries_shape
                break

            for i in _is:
                these_params = [_w, _ln_ratio, i, _a]

                if min(these_params) < 0 or _w < (_w_step/2):
                    self.gaps =  gpd.GeoDataFrame(columns=['geometry'],
                                                  geometry='geometry',
                                                  crs='EPSG:4326')
                    break

                self.mind(_w, _ln_ratio, i, _a)

                fit = self.fit_check(build_thresh, area_floor, area_ceiling)

                if fit:
                    these_params = [_w, _ln_ratio, i, _a, self.in_gaps_ratio, \
                                    self.area_ratio]
                    break
                else:
                    past_gaps.append(self.gaps)
                    these_params = [_w, _ln_ratio, i, _a, self.in_gaps_ratio, \
                                    self.area_ratio]
                    past_params.append(these_params)

            if fit:
                break
            elif min(these_params) < 0 or _w < (_w_step/2):
                break

            # Update paramaters
            _w = _w - _w_step

    def parallel_run(self,
                     b_thresh,
                     a_floor,
                     a_ceiling,
                     w,
                     w_step,
                     ln_ratio,
                     i,
                     a):
        """Wrapper to execute run method and return gaps"""

        self.run(build_thresh=b_thresh,
                 area_floor=a_floor,
                 area_ceiling=a_ceiling,
                 _w=w,
                 _w_step=w_step,
                 _ln_ratio=ln_ratio,
                 _is=i,
                 _a=a)

        return self.gaps

    def run_parallel(self,
                     tile_size=1,
                     build_thresh=0.07,
                     area_floor=0.2,
                     area_ceiling=0.8,
                     cpus=mp.cpu_count()-1,
                     _w=0.1,
                     _w_step=0.025,
                     _ln_ratio=2,
                     _is=(2,3,4),
                     _a=20):
        """Divides the region into square tiles and processes in parallel.
        
        Large datasets benefit from both using different parameters for
        different areas as well as parallel processing for performance gains.
        
        Parameters
        ----------
        build_thresh : float
            Maximum proportion of buildings allowed in gaps
        area_floor : float
            Minimum proportion of open space to be filled by gaps
        area_cieling : float
            Maximum proportion of open space to be filled by gaps
        tile_size : float
            Size of tiles to divide the dataset in degrees
        cpus : int
            Number of processes for multiprocessing
        _w : float
            Starting width value
        _w_step : float
            Value to update _w by each iteration
        _ln_ratio : float
            Ratio of minimum strip length to width
        _is : array_like
            1-d set of numbers of interesections to try
        _a : int
            Alpha value for alpha-shapes
            
        """

        # Divide data
        bounds = self.boundaries.bounds

        min_x = bounds[0]
        min_y = bounds[1]
        max_x = bounds[2]
        max_y = bounds[3]

        cols = list(np.arange(min_x, max_x + tile_size, tile_size))
        rows = list(np.arange(min_y, max_y + tile_size, tile_size))

        polygons = []
        for x in cols[:-1]:
            for y in rows[:-1]:
                polygons.append(geometry.Polygon([(x,y),
                                                  (x + tile_size, y),
                                                  (x + tile_size, y+tile_size),
                                                  (x, y + tile_size)]))
        tiles = gpd.GeoDataFrame({'geometry':polygons},crs='EPSG:4326')

        # Clip tiles to region extent
        tiles = gpd.clip(tiles, self.boundaries_shape)

        # Prepare tiles
        tile_regions = []
        for t in tiles['geometry']:
            bs = gpd.clip(self.buildings, t)
            t = gpd.GeoDataFrame({'geometry':[t]},crs='EPSG:4326')
            t_region = Region(bs,t)
            tile_regions.append(t_region)

        # Prepare args
        args = zip(tile_regions,
                   repeat(build_thresh),
                   repeat(area_floor),
                   repeat(area_ceiling),
                   repeat(_w),
                   repeat(_w_step),
                   repeat(_ln_ratio),
                   repeat(_is),
                   repeat(_a))

        # Execute
        with mp.Pool(processes=cpus) as p:
            gs = p.starmap(Region.parallel_run, args)

        # Combine gaps
        self.gaps = gpd.GeoDataFrame(pd.concat(gs, ignore_index=True))
