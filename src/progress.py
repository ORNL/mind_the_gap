"""Execute Mind the Gap with automated parameter selection"""

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

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
        
        # Load boundaries
        if bound_from_file:
            self.boundaries = gpd.read_file(bound_path)
            self.boundaries = self.boundaries['geometry'][0]
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
            self.boundaries = ([self.boundaries.boundary][0])[0]
            print('boundaries loaded')

            # Load grid
            grid_qry = f"""WITH grid AS (
                           SELECT (ST_SquareGrid(0.00083333333333,ST_Transform(geom,4326))).* as geom
                           FROM boundary.admin0
                           WHERE country = '{self.name}'
                           )
                           SELECT ST_AsText(geom)
                           FROM grd
                        """
            self.grid = gpd.GeoDataFrame.from_postgis(boundaries_qry,
                                                      db_con,
                                                      geom_col = 'geom')
        #print(self.grid)
        #self.grid.to_file('./grid.geojson',driver='GeoJSON')
        print('boundaries and grid loaded')

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

    def mind(self,w, ln_ratio, i, a):
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
        all_points_gdf = gpd.GeoDataFrame(pd.concat([self.buildings,self.chainage_gdf],
                                                    ignore_index=True))

        # Execute mind the gap
        l = w * ln_ratio + (w / 4)
        print('calling mtg')
        try:
            self.gaps = mind_the_gap.mind_the_gap(all_points_gdf, 
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
        
        # Check open space filled by gaps
        # Total open space
        # Decision

    def prog(self):
        """Iterates through parameters until a good set is settled on"""

        print('proging')
        # Starting params
        _w = 0.03
        _ln_ratio = 2
        _i = 3
        _a = 15

        past_gaps = []
        these_params = [_w, _ln_ratio, _i, _a]
        past_params = []

        """
        # Begin running
        while True:
            try:
                self.mind(w, ln_ratio, i, a)
            except:
                # Update paramters
                w = w - 0.02
                continue
                # Skip to update parameters
            
            # Evaluate
            if self.fit_check(gaps):
                break
                # Hold onto parameters and gaps and metrics
            past_gaps.append(self.gaps)
            past_params.append(self.gaps)
            # Update parameters
            w = w - 0.02
            # How to decide when/how to update which parameter?
            # Perhaps once width drops beneath a certain threshold we increase intersections
        """
        
        for i in range(5):
            
            print(these_params)
            # Check if any parameters have become negative
            if min(these_params) <=0:
                break
            
            print('trying to mind')
            self.mind(_w, _ln_ratio, _i, _a)
            
            self.fit_check(1,1)
            """try:
                print('trying to mind')
                self.mind(w, ln_ratio, i, a)
                print('that worked')
            except Exception as e:
                print(e)
                print('that shit broke updating parameters')
                               # Update paramters
                w = w - 0.02
                continue
                # Skip to update parameters
            """
            # Evaluate
            #if self.fit_check(gaps):
            #   break
                # Hold onto parameters and gaps and metrics
            past_gaps.append(self.gaps)
            past_params.append(these_params)
            # Update parameters
            _w = _w - 0.005
            these_params = [_w, _ln_ratio, _i, _a]
            # How to decide when/how to update which parameter?
            # Perhaps once width drops beneath a certain threshold we increase intersections

        print(past_gaps)
        print(past_params)