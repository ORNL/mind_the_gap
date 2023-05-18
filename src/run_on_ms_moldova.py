"""Run mind_the_gap on MS Moldova buildings"""

import os
import sys

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import MultiPoint
from shapely.geometry import LineString
from shapely.geometry import MultiLineString
from shapely.ops import unary_union
from sqlalchemy import create_engine

import mind_the_gap
from chainage import chainage

sys.setrecursionlimit(5000)

# Establish db connections
con = 'postgresql://lsadmin:lsadmin@manhattan:2022/lsdb'
open_con = create_engine('postgresql://openadmin:openadmin@manhattan:3022/opendb')

# Get boundaries
print('Loading boundaries')
boundaries_qry = """SELECT st_multi(st_buffer(geom,0.2)) as geom 
                    FROM boundary.admin0
                    WHERE country = 'moldova'"""
boundaries = gpd.GeoDataFrame.from_postgis(boundaries_qry,open_con,geom_col='geom')
#boundaries = gpd.read_file('./syria_box.geojson')
boundaries = ([boundaries.boundary][0])[0]

# Generate chainage
print('Generating chainage')
print(boundaries)
border_chainage = chainage(boundaries, 0.01)
chainage_gdf = gpd.GeoDataFrame(geometry=border_chainage)
chainage_gdf.set_geometry(col='geometry', inplace=True)
print(chainage_gdf)
print('Chainage done')

# Get buildings
print('Loading buildings')
buildings_qry = """SELECT ST_Centroid(geom) as geometry
                   FROM microsoft.moldova"""
buildings = gpd.GeoDataFrame.from_postgis(buildings_qry,
                                          open_con,
                                          geom_col='geometry')
print(buildings)

# Convert to centroids

# Merge with chainge
print('Combine buildings and border chainage')
all_points_gdf = gpd.GeoDataFrame(pd.concat([buildings,chainage_gdf],
                                            ignore_index=True))
print(all_points_gdf)
#all_points_gdf.to_file('./all_points_bfe.geojson', driver='GeoJSON')

# Run mind_the_gap
print('Mind the gaps')
try:
    gaps_gdf = mind_the_gap.mind_the_gap(all_points_gdf,
                                         0.04,
                                         0.04,
                                         0.08,
                                         0.08,
                                         5,
                                         5,
                                         alpha=18)
#print(type(gaps_gdf))
    print('Saving gaps')
    print(gaps_gdf)
    gaps_gdf.to_file('./moldova_w04_l08_i5_a18.geojson', driver='GeoJSON')
#gaps_gdf.to_postgis('mask', open_con, if_exists='replace',schema='bfe')
except Exception as e:
    print("run 1 didn't work")
    print(e)

try:
    gaps_gdf2 = mind_the_gap.mind_the_gap(all_points_gdf,
                                          0.035,
                                          0.035,
                                          0.08,
                                          0.08,
                                          3,
                                          3,
                                          alpha=25)
    print('Saving gaps 2')
    gaps_gdf2.to_file('./moldova_w035_l08_i3_a25.geojson', driver='GeoJSON')

except Exception as e:
    print("run 2 didn't work")
    print(e)
