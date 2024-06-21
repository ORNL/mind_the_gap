"""Run Mind the Gap worldwide tile-wise in parallel"""

import multiprocessing as mp
from multiprocessing import Pool
from itertools import repeat
from math import isnan
import sys
import time
from datetime import timedelta
from datetime import datetime
import logging

import geopandas as gpd
import pandas as pd
from auto_tune import Region
from sqlalchemy import create_engine
from sqlalchemy import text
from shapely import MultiPolygon
from shapely import Polygon

def run_region(_row_col,
               _schema,
               _bldgs_table,
               _gaps_table,
               _read_con,
               _write_con,
               _b_thresh,
               _a_floor,
               _a_ceiling):
    """"Execute the `run` method on a region object
    
    Parameters
    ----------
    _row_col : array_like
        List of tuples containing row and column pairs
    _schema : String
    _bldgs_table : String
    _gaps_table : String
    _read_con : String
    _write_con : String
    _b_thresh : float
        building threshold for auto_tune.Region.run
    _a_floor : float
        area floor threshold for auto_tune.Region.run
    _a_ceiling : float
        area cieling threshold for auto_tune.Region.run

    """

    _read_engine = create_engine(_read_con)
    _write_engine = create_engine(_write_con)
    row = _row_col[0]
    col = _row_col[1]

    # check if row or col is Nan
    if isnan(row) or isnan(col):
        return

    row = int(row)
    col = int(col)

    build_qry = f"""SELECT b.pt_geom as geometry
                    FROM {_schema}.{_bldgs_table} b
                    INNER JOIN public.country_tiles_sliversfix t
                    on st_intersects(b.pt_geom, t.geom)
                    WHERE t.degree_row = {row} and t.degree_col = {col}"""

    bound_qry = f"""SELECT t.geom as geometry
                    FROM public.country_tiles_sliversfix t
                    WHERE t.degree_row = {row} and t.degree_col = {col}"""

    try:
        region = Region(_read_engine, bound_qry, build_qry)
        _read_engine.dispose()
    except: # pylint: disable=bare-except
        error_msg = 'Failed to make region. Row: '+str(row)+' Col: '+str(col)
        logging.exception(error_msg)
        _read_engine.dispose()
        return

    try:
        region.run(build_thresh = _b_thresh,
                   area_floor = _a_floor,
                   area_ceiling = _a_ceiling)
        if region.gaps.empty:
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

        region.gaps.to_postgis(_gaps_table,
                               _write_engine,
                               if_exists='append',
                               schema=_schema)

    except: # pylint: disable=bare-except
        error_msg = 'Failed to run. Row: '+str(row)+' col: '+str(col)
        logging.exception(error_msg)
        region.gaps = gpd.GeoDataFrame([MultiPolygon()],
                                       columns=['geometry'],
                                       crs='EPSG:4326')
        region.gaps.insert(1,'status', 'failed', False)
        region.gaps.insert(2,'row',row,False)
        region.gaps.insert(3,'col',col,False)

        region.gaps.to_postgis(_gaps_table,
                               _write_engine,
                               if_exists='append',
                               schema=_schema)

    finally:
        _write_engine.dispose()

    return

if __name__ == "__main__":

    # Logging
    now_str = str(datetime.now().strftime("%Y%m%d%H%M%S"))
    log_filename = 'mtg_' + now_str + '.log'
    logging.basicConfig(filename=log_filename,
                        filemode='w',
                        format = '%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=20)

    logging.info('Run started')

    start_time = time.perf_counter()

    sys.setrecursionlimit(5000)

    # db info
    read_con = 'postgresql://landscanuser:iseeyou@gshs-aurelia01:5432/opendb'
    write_con = 'postgresql://mtgwrite:nomoregaps@gshs-aurelia01:5432/opendb'
    admin_con = 'postgresql://openadmin:openadmin@gshs-aurelia01:5432/opendb'

    bldgs_schema = 'google'
    bldgs_table = 'bldgs_v3'
    gaps_version = '_sa_test_1'
    gaps_table = f"""{bldgs_table}_mtg_v{gaps_version}"""

    resume = False

    # Resume run or new
    if resume:
        logging.info('Resuming previous run')
        row_col_qry = f"""SELECT DISTINCT degree_row, degree_col
                          FROM public.country_tiles_sliversfix cts
                          LEFT JOIN {bldgs_schema}.{gaps_table} bvmv
                              ON cts.degree_row = bvmv.row
                                  AND cts.degree_col = bvmv.col
                          WHERE bvmv.status IS NULL"""

    else:
        logging.info('Starting new run')
        row_col_qry = """SELECT DISTINCT degree_row, degree_col
                         FROM public.country_tiles_sliversfix"""

        # Wipe table
        clear_qry = f"""DROP TABLE IF EXISTS {bldgs_schema}.{gaps_table}"""
        admin_engine = create_engine(admin_con)
        connection = admin_engine.connect()
        connection.execute(text(clear_qry))
        connection.commit()
        connection.close()
        admin_engine.dispose()

    row_col_df = pd.read_sql_query(row_col_qry, read_con)
    row_col = row_col_df.itertuples(index=False, name=None)

    regions = []
    region_dict = {}

    # Region.run parameters
    building_thresh = 0.07
    area_floor = 0.4
    area_ceiling = 0.8

    # prepare args
    args = zip(row_col,
               repeat(bldgs_schema),
               repeat(bldgs_table),
               repeat(gaps_table),
               repeat(read_con),
               repeat(write_con),
               repeat(building_thresh),
               repeat(area_floor),
               repeat(area_ceiling))

    # Add database info to the log file
    logging.info('Buildings schema: ' + bldgs_schema)
    logging.info('Buildings table: ' + bldgs_table)
    logging.info('Gaps table: ' + gaps_table)

    # Add Region.run parameters to the log file
    logging.info('Building threshold: ' + str(building_thresh))
    logging.info('Area floor: ' + str(area_floor))
    logging.info('Area ceiling: ' + str(area_ceiling))

    with Pool(processes=(mp.cpu_count()-1), maxtasksperchild=4) as p:
        try:
            p.starmap(run_region, args, chunksize=1)
        except: # pylint: disable=bare-except
            logging.exception('Failed at Pool')

    # Finish
    duration = timedelta(seconds=time.perf_counter()-start_time)
    print('Done minding')
    time_string = 'Run time: ' + str(duration)
    print(time_string)

    logging.info('Done minding')
    logging.info(time_string)
