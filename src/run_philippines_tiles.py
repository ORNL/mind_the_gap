"""Run Mind the Gap worldwide tile-wise in parallel"""

import multiprocessing as mp
from multiprocessing import Pool
from itertools import product
from math import isnan
import traceback
import sys

import geopandas as gpd
import pandas as pd
from auto_tune import Region
from sqlalchemy import create_engine
from sqlalchemy import text
from shapely import MultiPolygon
from shapely import Polygon

def run_region(row_col, schema='microsoft', table_name='bldgs_01302024'):
    """"Execute the `run` method on a region object
    
    Parameters
    ----------
    row_col : array_like
        Listof tuples containing row and column pairs
    schema : String
    table_name : String
    
    """

    row = row_col[0]
    col = row_col[1]

    # check if row or col is Nan
    if isnan(row) or isnan(col):
        return

    row = int(row)
    col = int(col)

    build_qry = f"""SELECT b.pt_geom as geometry
                    FROM {schema}.{table_name} b
                    INNER JOIN analytics.degree_tiles_stats t
                    on st_intersects(b.pt_geom, t.geom)
                    WHERE t.degree_row = {row} and t.degree_col = {col}"""

    bound_qry = f"""SELECT t.geom as geometry
                    FROM analytics.degree_tiles_stats t
                    WHERE t.degree_row = {row} and t.degree_col = {col}"""

    region = Region(read_con, bound_qry, build_qry)

    try:
        region.run(build_thresh=0.07, area_floor=0.3, area_ceiling=0.6)
        if region.gaps.empty:
            print('gaps are none')
            return
        elif isinstance(region.gaps['geometry'][0], Polygon):
            gaps_geoms = list(region.gaps['geometry'])
            gaps_geoms = MultiPolygon(gaps_geoms)
            region.gaps = gpd.GeoDataFrame(data={'geometry':[gaps_geoms]},
                                           crs='EPSG:4326')
            #region.gaps['geometry'][0] = MultiPolygon([region.gaps['geometry'][0]])
        region.gaps.to_postgis('bldgs_01302024_philippines_mtg_v4',
                               write_engine,
                               if_exists='append',
                               schema='microsoft')
        print(row_col)
    except:
        # Possibly should also just set gaps to be blank
        traceback.print_exc()

sys.setrecursionlimit(5000)

read_con = 'postgresql://landscanuser:iseeyou@gshs-aurelia01:5432/opendb'
write_con = 'postgresql://mtgwrite:nomoregaps@gshs-aurelia01:5432/opendb'
admin_con = 'postgresql://openadmin:openadmin@gshs-aurelia01:5432/opendb'
write_engine = create_engine(write_con)
admin_engine = create_engine(admin_con)

row_col_qry = """SELECT DISTINCT degree_row, degree_col
                 FROM public.country_tiles_sliversfix
                 WHERE 'Philippines' = ANY(countries)"""

row_col_df = pd.read_sql_query(row_col_qry, read_con)
row_col = row_col_df.itertuples(index=False, name=None)

regions = []
region_dict = {}

'''
for j in row_col:

    # For MS set schema and table
    schema = 'microsoft'
    table_name = 'bldgs_01302024'

    # check if row or col is Nan
    if isnan(j[0]) or isnan(j[1]):
        continue

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

    regions.append(tile_region)

    region_dict[tile_region] = j

print('regions made')
'''

# wipe table
bldgs_schema = 'microsoft'
gaps_table = 'bldgs_01302024_mtg_v1'
clear_qry = f"""DROP TABLE IF EXISTS {bldgs_schema}.{gaps_table}"""
#connection = admin_engine.connect()
#connection.execute(text(clear_qry))
#connection.commit()

with Pool(31) as p:
    try:
        p.map(run_region, row_col)
    except:
        traceback.print_exc()

print('done minding')

#for reg in region_dict:
#    if reg.gaps == []:
#        continue
#    reg.gaps.to_postgis('degree_tile_ms_gaps', 
#                        write_engine,
#                        if_exists='append',
#                        schema='analytics')
#print('done')