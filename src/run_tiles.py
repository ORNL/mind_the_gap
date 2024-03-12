"""Run Mind the Gap worldwide tile-wise in parallel"""

import multiprocessing as mp

import geopandas as gpd
from auto_tune import Region 


grid_qry = """SELECT *
              FROM public.country_tiles_sliversfix
              WHERE 'Slovakia' = ANY(countries)"""
db_con = 'postgresql://landscanuser:iseeyou@gshs-aurelia01:5432/opendb'

grid = gpd.GeoDataFrame.from_postgis(grid_qry, db_con, geom_col = 'geom')
ms_qry = """SELECT b.pt_Geom
            FROM microsoft.bldgs_01302024 b
            INNER JOIN analytics.degree
            WHERE """

# Get buildings with row and column attributes
# Queue up tile row and columns
# build list of auto_tune.Region objects, built using queries in nested for
# loop for tile degree rows and columns

print('stop')