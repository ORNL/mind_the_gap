"""Run Mind the Gap worldwide tile-wise in parallel"""

import multiprocessing as mp
from multiprocessing import Pool
from itertools import product
from math import isnan
import traceback
import sys
import time
from datetime import timedelta

import geopandas as gpd
import pandas as pd
from auto_tune import Region
from sqlalchemy import create_engine
from sqlalchemy import text
from shapely import MultiPolygon
from shapely import Polygon
from tqdm import tqdm

def run_region(row_col, schema='google', table_name='bldgs_v3'):
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
        region.run(build_thresh=0.07, area_floor=0.3, area_ceiling=0.7)
        if region.gaps.empty:
            print('gaps are none')
            print(row_col)
            region.gaps = gpd.GeoDataFrame([MultiPolygon()],
                                           columns=['geometry'],
                                           crs='EPSG:4326')
            region.gaps.insert(1,'status', 'no_gaps', False)
        elif isinstance(region.gaps['geometry'][0], MultiPolygon):
            region.gaps.insert(1,'status','gaps_found',False)
        elif isinstance(region.gaps['geometry'][0], Polygon):
            gaps_geoms = list(region.gaps['geometry'])
            gaps_geoms = MultiPolygon(gaps_geoms)
            region.gaps = gpd.GeoDataFrame(data={'geometry':[gaps_geoms]},
                                           crs='EPSG:4326')
            region.gaps.insert(1,'status','gaps_found',False)
        region.gaps.insert(2,'row',row_col[0],False)
        region.gaps.insert(3,'col',row_col[1],False)

        region.gaps.to_postgis('bldgs_01302024_mtg_v14',
                               write_engine,
                               if_exists='append',
                               schema='microsoft')
        return
    except:
        # Need to have a way to tag tiles that we failed on
        print('failed: ', row_col)
        region.gaps = gpd.GeoDataFrame([MultiPolygon()],
                                       columns=['geometry'],
                                       crs='EPSG:4326')
        region.gaps.insert(1,'status', 'failed', False)
        region.gaps.insert(2,'row',row_col[0],False)
        region.gaps.insert(3,'col',row_col[1],False)

        region.gaps.to_postgis('bldgs_01302024_mtg_v14',
                               write_engine,
                               if_exists='append',
                               schema='microsoft')

        traceback.print_exc()

        return

if __name__ == "__main__":

    start_time = time.perf_counter()

    sys.setrecursionlimit(5000)

    read_con = 'postgresql://landscanuser:iseeyou@gshs-aurelia01:5432/opendb'
    write_con = 'postgresql://mtgwrite:nomoregaps@gshs-aurelia01:5432/opendb'
    admin_con = 'postgresql://openadmin:openadmin@gshs-aurelia01:5432/opendb'
    write_engine = create_engine(write_con)
    admin_engine = create_engine(admin_con)

    row_col_qry = """SELECT DISTINCT degree_row, degree_col
                     FROM analytics.degree_tiles_stats"""

    row_col_df = pd.read_sql_query(row_col_qry, read_con)
    row_col = row_col_df.itertuples(index=False, name=None)

    regions = []
    region_dict = {}

    # wipe table
    bldgs_schema = 'google'
    gaps_table = 'bldgs_v3_mtg_v1'
    clear_qry = f"""DROP TABLE IF EXISTS {bldgs_schema}.{gaps_table}"""
    #connection = admin_engine.connect()
    #connection.execute(text(clear_qry))
    #connection.commit()

    with Pool(processes=(mp.cpu_count()-1), maxtasksperchild=4) as p:
        try:
            #for i in tqdm(p.imap_unordered(run_region, row_col, chunksize=4),
            #              total=len(list(row_col))):
            #    pass
            p.map(run_region, row_col, chunksize=1)
        except:
            traceback.print_exc()

    duration = timedelta(seconds=time.perf_counter()-start_time)
    print('done minding')
    print('Run time: ', duration)