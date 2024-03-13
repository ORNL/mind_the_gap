"""Run Mind the Gap worldwide tile-wise in parallel"""

import multiprocessing as mp
from itertools import product

import geopandas as gpd
import pandas as pd
from auto_tune import Region 


#grid_qry = """SELECT *
#              FROM public.country_tiles_sliversfix
#              WHERE 'Slovakia' = ANY(countries)"""
read_con = 'postgresql://landscanuser:iseeyou@gshs-aurelia01:5432/opendb'
write_con = 'postgresql://mtgwrite:nomoregaps@gshs-aurelia01:5432/opendb'

#grid = gpd.GeoDataFrame.from_postgis(grid_qry, db_con, geom_col = 'geom')
#ms_qry = """SELECT b.pt_Geom
#            FROM microsoft.bldgs_01302024 b
#            INNER JOIN analytics.degree
#            WHERE """

# Get buildings with row and column attributes
# Queue up tile row and columns
# build list of auto_tune.Region objects, built using queries in nested for
# loop for tile degree rows and columns

row_col_qry = """SELECT DISTINCT degree_row, degree_col
                 FROM analytics.degree_tiles_stats"""

row_col_df = pd.read_sql_query(row_col_qry, read_con)
row_col = row_col_df.itertuples(index=False, name=None)

regions = []
for j in row_col:
    # For MS set schema and table
    schema = 'microsoft'
    table_name = 'bldgs_01302024'

    row = int(j[0])
    col = int(j[1])

    build_qry = f"""SELECT b.pt_geom as geometry
                    FROM {schema}.{table_name} b
                    INNER JOIN analytics.degree_tiles_stats t
                    ON st_intersects(b.pt_geom, t.geom)
                    WHERE t.degree_row = {row} and t.degree_col = {col}"""
    
    bound_qry = f"""SELECT t.geom
                    FROM analytics.degree_tiles_stats t
                    WHERE t.degree_row = {row} and t.degree_col = {col}"""
    
    tile_region = Region(read_con, bound_qry, build_qry)
    print('one_region')
print('stop')