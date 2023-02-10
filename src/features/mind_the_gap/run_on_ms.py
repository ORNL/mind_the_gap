"""Run mind_the_gap on Microsoft Turkey buildings"""

import os
import sys

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import MultiPoint
from shapely.geometry import LineString
from shapely.ops import unary_union
from sqlalchemy import create_engine

import mind_the_gap

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
    for line in boundary_line:
        distances = np.arange(0, line.length, interval)
        points = [line.interpolate(distance) for distance in distances] + \
                [line.boundary]
        points = unary_union(points)
        chain_points = unary_union([chain_points, points])

    chainage_ds = gpd.GeoSeries(chain_points, crs=coord_sys)

    return chainage_ds

sys.setrecursionlimit(9000)

# Establish db connections
con = 'postgresql://lsadmin:lsadmin@manhattan:2022/lsdb'
open_con = create_engine('postgresql://openadmin:openadmin@manhattan:3022/opendb')

# Get boundaries
print('Loading boundaries')
#boundaries_qry = """SELECT ogc_fid, st_multi(st_buffer(geom, 0.1)) as geom 
#                    FROM ethiopia.boundary"""
#boundaries = gpd.GeoDataFrame.from_postgis(boundaries_qry,con,geom_col='geom')
boundaries = gpd.read_file('./turkey_box.geojson')
boundaries = [boundaries.boundary[0]]

# Generate chainage
print('Generating chainage')
border_chainage = chainage(boundaries, 0.005)
chainage_gdf = gpd.GeoDataFrame(geometry=border_chainage)
chainage_gdf.set_geometry(col='geometry', inplace=True)
print(chainage_gdf)
print('Chainage done')

# Get buildings
print('Loading buildings')
buildings_qry = """SELECT ogc_fid, pt_geom as geometry 
                   FROM microsoft2.turkey"""
buildings = gpd.GeoDataFrame.from_postgis(buildings_qry, 
                                          open_con,
                                          geom_col='geometry')
print(buildings)

# all_points_gdf = gpd.read_file('./Ethiopia_MS_Centroids_bounded_4326.geojson')

# Merge with chainge
#print('Combine buildings and border chainage')
all_points_gdf = gpd.GeoDataFrame(pd.concat([buildings,chainage_gdf],
                                            ignore_index=True))
print(all_points_gdf)
#all_points_gdf.to_file('./all_points_microsoft.geojson', driver='GeoJSON')

# Run mind_the_gap
print('Minding the gaps 1')
try:
    gaps_gdf = mind_the_gap.mind_the_gap(all_points_gdf,
                                         0.05,
                                         0.05,
                                         0.11,
                                         0.11,
                                         2,
                                         2,
                                         write_points=True)

    print('Saving gaps')
    gaps_gdf.to_file('./turkiye_w05_l11_i2_points.geojson',
                 driver='GeoJSON')
    print(gaps_gdf)
except:
    print('yo that shit dont work cuh')
print('minding gaps 2')
try:
    gaps_gdf_2 = mind_the_gap.mind_the_gap(all_points_gdf,
                                           0.05,
                                           0.05,
                                           0.11,
                                           0.11,
                                           2,
                                           2,
                                           polygon_type='rim',
                                           corners=True)
    print('Saving gaps 2')
    gaps_gdf_2.to_file('./turkiye_w05_l11_i2_rimcorners.geojson',driver='GeoJSON')
except:
    print('That dont work neither')