"""Execute Mind the Gap with automated parameter selection"""

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from shapely import geometry

import mind_the_gap
from chainage import chainage

# Country object
class country:
    def __init__(self, 
                 name, 
                 db_con, 
                 bound_path='', 
                 build_path='',
                 bound_from_file=False, 
                 build_from_file=False):
        """The country we are running Mind the Gap on
    
        Parameters
        ----------
        name : String
            Name of the country as used in the database
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


        # Load boundaries
        if bound_from_file:
            self.boundaries = gpd.read_file(bound_path)
            self.boundaries_shape = self.boundaries
            self.boundaries = ([self.boundaries.boundary][0])[0]
            print(self.boundaries_shape)

        else:
            # Establish database connection
            con = create_engine(self.db_con)

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

    def make_grid(self, size=0.05):
        """Make grid to check gap completeness
        
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
        
        # Clip grid to country extent
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
            self.gaps = []

    def fit_check(self,in_gaps_thresh, space_thresh):
        """Checks how well the gaps fit the data
        
        Parameters
        ----------
        in_gaps_thresh : float
            Threshold for the proportion of buildigns allwoed in gaps
        space_thresh : float
            Threshold for the amoutn of open space to take up 
        """

        # Check proportion of buildings in the gaps
        gaps_series = self.gaps.geometry
        buildings_series = self.buildings.geometry
        #in_gaps = gaps_series.intersect(buildings_series)
        gaps_multi = gaps_series.unary_union
        in_gaps = self.buildings.sjoin(self.gaps, how='inner')
        print(in_gaps.size)
        print(buildings_series.size)
        
        # Get open space or grid cells
        joined_grid = gpd.sjoin(self.grid,
                                self.all_points_gdf,
                                how='left',
                                predicate='contains')
        empty_grid = joined_grid.loc[joined_grid['index_right'].isna()]
        empty_grid_area = sum(empty_grid['geometry'].area)
        gaps_in_empty_grid = gpd.overlay(empty_grid, 
                                         self.gaps, 
                                         how='intersection')
        gaps_in_empty_grid_area = sum(gaps_in_empty_grid['geometry'].area)
        

        area_ratio = (gaps_in_empty_grid_area / empty_grid_area)
        print(area_ratio)
        # Decision
        # Should decision be boolean or say something about suggested parameter updates?

    def prog(self):
        """Iterates through parameters until a good set is settled on"""

        print('proging')
        # Starting params
        _w = 0.015
        _ln_ratio = 2
        _i = 3
        _a = 15

        past_gaps = []
        these_params = [_w, _ln_ratio, _i, _a]
        past_params = []

        
        for i in range(5):
            
            #print(these_params)
            # Check if any parameters have become negative
            if min(these_params) <=0:
                break
            
            _is = [2,3,4]

            for i in _is:
                self.mind(_w, _ln_ratio, i, _a)

                self.fit_check(1,1)
                past_gaps.append(self.gaps)
                these_params = [_w,_ln_ratio,i,_a]
                past_params.append(these_params)

            
            # Evaluate
            #if self.fit_check(gaps):
            #   break
                # Hold onto parameters and gaps and metrics
            #past_gaps.append(self.gaps)
            #past_params.append(these_params)
            # Update parameters
            _w = _w - 0.005 # It would be reasonable to set a floor
            #these_params = [_w, _ln_ratio, _i, _a]
            # How to decide when/how to update which parameter?
            # Perhaps once width drops beneath a certain threshold we increase intersections
            # Intersections is typically going to be two or three, sometimes higher
            # So fo each w step we could just do runs for i = [2:4]

        print(past_gaps)
        print(past_params)