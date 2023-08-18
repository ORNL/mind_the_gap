"""Execute Mind the Gap with automated parameter selection"""

import geopandas as gpd
from sqlalchemy import create_engine

import mind_the_gap
import chainage

# Country object
class country(self, name, db_con):
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
                        WHERE country = '{self.country}'"""
    boundaries = gpd.GeoDataFrame.from_postgis(boundaries_qry,
                                               open_con,
                                               geom_col='geom')
    boundaries = ([boundaries.boundary][0])[0]

    # Generate chainage
    bnd_chain = chainage(boundaries, 0.01)
    chainage_gdf = gpd.GeoDataFrame(geometry=bnd_chain)

    # Load buildings 
    buildings_qry = f"""SELECT ST_Centroid(geom) as geometry
                        FROM microsoft.{self.country}"""
    buildings = gpd.GeoDataFrame.from_postgis(buildings_qry,
                                              open_con,
                                              geom_col='geometry')

    def self.mind(w, ln_ratio, i, a):
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
        all_points_gdf = gpd.GeoDataFrame(pd.concat([buildings,chainage_gdf],
                                                    ignore_index=True))

        # Execute mind the gap
        l = w * ln_ratio + (w / 4)
        gaps = mind_the_gap.mind_the_gap(all_points_gdf, 
                                         w,
                                         w,
                                         l,
                                         l,
                                         i,
                                         i,
                                         alpha=a)
        
        return gaps

        def self.fit_check(gaps, points):
            """Checks how well the gaps fit the data
            
            Parameters
            ----------
            gaps : GeoDataFrame
                Gaps to check
            points : GeoDataFrame
                Building centroids
            
            """

        