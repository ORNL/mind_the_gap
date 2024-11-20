"""Generate boundary chainage for mind_the_gap"""

import geopandas as gpd
import numpy as np
from shapely.ops import unary_union
from shapely.geometry import MultiPoint
from shapely.geometry import MultiLineString
from shapely.geometry import LineString

def chainage(boundary_line, interval, coord_sys='EPSG:4326'):
    """Generates a set of points at equal intervals along a line
    
    Parameters
    ----------
    boundary_line : MultiLineString or LineString
        Line to generate the chainage on
    interval : float
        Space between points
    coord_sys : string
        Coordinate reference system
            
    Returns
    -------
    chain_points_ds : GeoSeries

    """

    if isinstance(boundary_line, MultiLineString):
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

    elif isinstance(boundary_line, LineString):
        chain_points = MultiPoint()
        distances = np.arange(0, boundary_line.length, interval)
        points = [boundary_line.interpolate(distance) \
            for distance in distances] + [boundary_line.boundary]
        points = unary_union(points)
        chain_points = unary_union([chain_points, points])

        chainage_ds = gpd.GeoSeries(chain_points, crs=coord_sys)
        chainage_ds = chainage_ds.explode(ignore_index=True)

        return chainage_ds
    else:
        raise TypeError("boundary_line must be LineString or MultiLineString")
