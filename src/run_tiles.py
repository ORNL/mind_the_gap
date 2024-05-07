"""Run Mind the Gap worldwide tile-wise in parallel"""

import multiprocessing as mp
from multiprocessing import Pool
from itertools import product
from math import isnan
import traceback
import sys
import time
from datetime import timedelta
import logging
from functools import partial

import geopandas as gpd
import pandas as pd
from auto_tune import Region
from sqlalchemy import create_engine
from sqlalchemy import text
from shapely import MultiPolygon
from shapely import Polygon
from tqdm import tqdm

def run_region(_row_col,
               _schema='google',
               _table_name='bldgs_v3'):
    """"Execute the `run` method on a region object
    
    Parameters
    ----------
    row_col : array_like
        Listof tuples containing row and column pairs
    schema : String
    table_name : String
    
    """

    _read_con = read_con
    _write_engine = write_engine

    _read_engine = create_engine(_read_con)

    row = _row_col[0]
    col = _row_col[1]

    # check if row or col is Nan
    if isnan(row) or isnan(col):
        return

    row = int(row)
    col = int(col)

    build_qry = f"""SELECT b.pt_geom as geometry
                    FROM {_schema}.{_table_name} b
                    INNER JOIN analytics.degree_tiles_stats t
                    on st_intersects(b.pt_geom, t.geom)
                    WHERE t.degree_row = {row} and t.degree_col = {col}"""

    bound_qry = f"""SELECT t.geom as geometry
                    FROM analytics.degree_tiles_stats t
                    WHERE t.degree_row = {row} and t.degree_col = {col}"""

    try:
        region = Region(_read_engine, bound_qry, build_qry)
        _read_engine.dispose()
    except:
        logging.exception('failed to make region')
        _read_engine.dispose()
        return

    try:
        region.run(build_thresh=0.07, area_floor=0.3, area_ceiling=0.7)
        if region.gaps.empty:
            #logging.info('gaps are none')
            #logging.info(row_col)
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
        region.gaps.insert(2,'row',row,False)
        region.gaps.insert(3,'col',col,False)

        region.gaps.to_postgis('bldgs_mtg_v2',
                               _write_engine,
                               if_exists='append',
                               schema=_schema)
        return
    except:
        # Need to have a way to tag tiles that we failed on
        error_msg = 'Failed. Row: '+str(row)+' col: '+str(col[1])
        logging.exception(error_msg)
        region.gaps = gpd.GeoDataFrame([MultiPolygon()],
                                       columns=['geometry'],
                                       crs='EPSG:4326')
        region.gaps.insert(1,'status', 'failed', False)
        region.gaps.insert(2,'row',row,False)
        region.gaps.insert(3,'col',col,False)

        region.gaps.to_postgis('bldgs_mtg_v2',
                               write_engine,
                               if_exists='append',
                               schema=_schema)

        # traceback.print_exc()

        return

def wrap_regions(args):
    """Wrapper for passing multiple arguments to run_region with map"""

    run_region(args[0],args[1],args[2],args[3],args[4])

    return

if __name__ == "__main__":

    logging.basicConfig(filename='run_tile_google.log')

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

    #with Pool(processes=1, maxtasksperchild=4) as p: # for debugging
    with Pool(processes=(mp.cpu_count()-1), maxtasksperchild=4) as p:
        try:
            #for i in tqdm(p.imap_unordered(run_region, row_col, chunksize=4),
            #              total=len(list(row_col))):
            #    pass
            p.map(run_region, row_col, chunksize=1)
        except:
            #traceback.print_exc()
            logging.exception('Failed at Pool')

    # Dispose of engines
    write_engine.dispose()
    admin_engine.dispose()
    # need to close connections if it fails
    # Finish
    duration = timedelta(seconds=time.perf_counter()-start_time)
    print('done minding')
    print('Run time: ', duration)