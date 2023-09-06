"""Execute Mind the Gap with automated parameter selection"""

import geopandas as gpd
from sqlalchemy import create_engine

import mind_the_gap
from chainage import chainage

# Country object
class country:
    def __init__(self, name, db_con):
        """The country we are running Mind the Gap on
    
        Parameters
        ----------
        name : String
            Name of the country as used in the database
        db_con : String
            String used to establis database connection
        
        """

        self.name = name
        self.db_con = db_con

        # Establish database connection
        con = create_engine(self.db_con)

        # Load boundaries
        boundaries_qry = f"""SELECT st_multi(st_buffer(geom,0.2)) as geom 
                            FROM boundary.admin0
                            WHERE country = '{self.name}'"""
        boundaries = gpd.GeoDataFrame.from_postgis(boundaries_qry,
                                                   db_con,
                                                   geom_col='geom')
        boundaries = ([boundaries.boundary][0])[0]

        # Generate chainage
        bnd_chain = chainage(boundaries, 0.01)
        chainage_gdf = gpd.GeoDataFrame(geometry=bnd_chain)

        # Load buildings 
        buildings_qry = f"""SELECT ST_Centroid(geom) as geometry
                            FROM microsoft.{self.name}"""
        buildings = gpd.GeoDataFrame.from_postgis(buildings_qry,
                                                  db_con,
                                                  geom_col='geometry')

        def mind(w, ln_ratio, i, a):
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
            self.gaps = mind_the_gap.mind_the_gap(all_points_gdf, 
                                                  w,
                                                  w,
                                                  l,
                                                  l,
                                                  i,
                                                  i,
                                                  alpha=a)

        def fit_check(in_gaps_thresh, space_thresh):
            """Checks how well the gaps fit the data

            Parameters
            ----------
            in_gaps_thresh : float
                Threshold for the proportion of buildigns allwoed in gaps
            space_thresh : float
                Threshold for the amoutn of open space to take up 
            """

            # Check proportion of buildings in the gaps
            in_gaps = self.gaps.intersection(self.buildings)
            # Check open space filled by gaps

            # Decision

        def prog():
            """Iterates through parameters until a good set is settled on"""

            # Starting params
            w = 1
            ln_ratio = 2
            i = 3
            a =20

            past_gaps = []
            these_params = [w, ln_ratio, i, a]
            past_params = []

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
            
                # Hold onto parameters and gaps
                past_gaps.append(self.gaps)
                past_params.append(self.gaps)
                # Update parameters
                w = w - 0.02
                # How to decide when/how to update which parameter?

