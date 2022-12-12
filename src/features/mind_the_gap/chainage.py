"""Generate boundary chainage for mind_the_gap"""

import geopandas as gpd
import numpy as np
from shapely.ops import unary_union
from shapely.geometry import MultiPoint

def chainage(boundary_line, interval, coord_sys='EPSG:4326'):
    """Generates a set of points at equal intervals along a line
    
    Parameters
    ----------
    line : LineString
        Line to generate the chainage on
    interval : float
        Space between points
    coord_sys : string
        Coordinate reference system
            
    Returns
    -------
    chain_points_ds : GeoSeries

    """

    chain_points = MultiPoint()
    for line in boundary_line.geoms:
        distances = np.arange(0, line.length, interval)
        points = [line.interpolate(distance) for distance in distances] + \
                [line.boundary]
        points = unary_union(points)
        chain_points = unary_union([chain_points, points])

    chainage_ds = gpd.GeoSeries(chain_points, crs=coord_sys)
    chainage_ds = chainage_ds.explode(ignore_index=True)

    return chainage_ds