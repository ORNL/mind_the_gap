"""Execute Mind the Gap with automated parameter selection"""

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely import geometry

import mind_the_gap
from chainage import chainage

# Region object
class Region:
    """The region to run Mind the Gap on.
    
    Loads all necessary information and contains methods for automatically
    tuning parameters to generate a good no data mask.
    
    """

    def __init__(self,
                 name,
                 db_con,
                 bound_path='',
                 build_path='',
                 bound_from_file=False,
                 build_from_file=False):
        """The region we are running Mind the Gap on.

        Loads in building and boundary data, builds chainage, etc. Everything
        needed to run MTG and builds grid to autotune parameters to.
    
        Parameters
        ----------
        name : String
            Name of the region as used in the database
        db_con : String
            String used to establis database connection
        bound_path : String
            Optional path to load boundaries from file
        build_path : String
            Optional path to load boundaries from file
        bound_from_file : boolean
            Use path to load boundaries or not
        build_from_file : boolean
            Use path to load buildings or not

        """

        self.name = name
        self.db_con = db_con
        self.gaps = []
        self.grid = []
        self.in_gaps_ratio = 0
        self.area_ratio = 0
        self.all_points_gdf = None


        # Load boundaries
        if bound_from_file:
            self.boundaries = gpd.read_file(bound_path)
            self.boundaries_shape = self.boundaries
            self.boundaries = ([self.boundaries.boundary][0])[0]

        else:
            # Load boundaries
            boundaries_qry = f"""SELECT st_multi(st_buffer(geom,0.2)) as geom
                                 FROM boundary.admin0
                                 WHERE country = '{self.name}'"""
            self.boundaries = gpd.GeoDataFrame.from_postgis(boundaries_qry,
                                                            db_con,
                                                            geom_col='geom')
            self.boundaries_shape = self.boundaries
            self.boundaries = ([self.boundaries.boundary][0])[0]
            print('boundaries loaded')

        print('boundaries loaded')

        # Generate chainage
        bnd_chain = chainage(self.boundaries, 0.01)
        self.chainage_gdf = gpd.GeoDataFrame(geometry=bnd_chain)
        print('chainage done')

        # Load buildings
        if build_from_file:
            self.buildings = gpd.read_file(build_path)
        else:
            buildings_qry = f"""SELECT ST_Centroid(geom) as geometry
                                FROM microsoft.{self.name}"""
            self.buildings = gpd.GeoDataFrame.from_postgis(buildings_qry,
                                                           db_con,
                                                           geom_col='geometry')

        print('buildings loaded')

        # Make grid
        print('making grid')
        self.make_grid()

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

        # Combine buildings and border chainage
        self.all_points_gdf = gpd.GeoDataFrame(pd.concat([self.buildings,
                                                          self.chainage_gdf],
                                                         ignore_index=True))

        # Execute mind the gap
        l = w * ln_ratio + (w / 4)
        print('calling mtg')
        try:
            self.gaps = mind_the_gap.mind_the_gap(self.all_points_gdf,
                                                  w,
                                                  w,
                                                  l,
                                                  l,
                                                  i,
                                                  i,
                                                  alpha=a)
            print('mtg ran')
        except Exception as e:
            print(e)
            print('somehing broke setting gaps to []')
            self.gaps = None #This breaks fit_check

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
            empty_grid_area = sum(empty_grid['geometry'].area)
            gaps_in_empty_grid = gpd.overlay(empty_grid, # sjoin might be better
                                             self.gaps,
                                             how='intersection')
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


    def run(self, build_thresh=0.07, area_floor=0.4, area_ceiling=0.6):
        """Iterates through parameters until a good set is settled on"""

        print('tuning')
        # Starting params
        _w = 0.03
        _ln_ratio = 2
        _i = 3
        _a = 20

        past_gaps = []
        these_params = [_w, _ln_ratio, _i, _a]
        past_params = []

        while True:
            print(these_params)
            print(min(these_params))
            if min(these_params) < 0:
                break

            _is = [2,3,4]

            for i in _is:
                self.mind(_w, _ln_ratio, i, _a)

                fit = self.fit_check(build_thresh, area_floor, area_ceiling)

                if fit:
                    print('gaps found')
                    these_params = [_w, _ln_ratio, i, _a, self.in_gaps_ratio, self.area_ratio]
                    print(these_params)
                    break # Self.gaps will be our final gaps
                else: #We will save the gaps and parameters and update
                    past_gaps.append(self.gaps)
                    these_params = [_w, _ln_ratio, i, _a, self.in_gaps_ratio, self.area_ratio]
                    print(these_params)
                    past_params.append(these_params)

            if fit:
                break
            # Update paramaters
            _w = _w - 0.005 # Should this be hardcoded?
