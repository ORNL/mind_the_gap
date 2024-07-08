# -*- coding: utf-8 -*-
"""
Finds gaps in geographic point data.

Intended for buildings footprints (really centroids) by dividing the space into
vertical and horizontal strips. Large gaps of points in these strips along
their long axis are used to map out areas without any buildings present.

Intended for use with MS buildings, where missing image tiles result in data
gaps with a square shaped pattern, so this script tries to filter out
irregularly shaped gaps with jagged edges, but can be tuned to be more or less
sensitive to irregular shapes.

Returns a GeoDataFrame of either points of where horizontal and vertical gaps
intersect, or polygons generated by alpha shapes from these points.
"""

__author__ = "Jack Gonzales"

from operator import itemgetter
from itertools import chain
from warnings import warn

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.collections as mc
import pylab as pl
import geopandas as gpd
import shapely
from libpysal.cg import alpha_shape
from shapely.geometry import LineString
from shapely.geometry import Point
from shapely.geometry import MultiPoint
from shapely.geometry import MultiPolygon

# -----------------Put coordinates in np array----------------
def get_coordinates(points):
    """Get point coordinates in numpy array out of geodataframe.

    Parameters
    ----------
    points : GeoDataFrame
        Set of building centroids, as loaded in by load_points

    Returns
    -------
    ndarray
        Array of point coordinates (x,y)
    """
    point_geom = points['geometry']

    points_coords = np.zeros([np.size(point_geom),2])

    for i, point in enumerate(point_geom):
        if isinstance(point, MultiPoint):
            for p in point.geoms:
                points_coords[i,0] = p.x
                points_coords[i,1] = p.y
        elif isinstance(point, Point):
            points_coords[i,0] = point.x
            points_coords[i,1] = point.y

    return points_coords

# ----------Put points into bins based on lat and lon---------
def into_the_bins(points, x_bin_size=0.005, y_bin_size=0.005):
    """Sorts points into latidude and longitude bins.

    Points go into two sets of bins based on their latitude (y) and
    longitude (x). Calls into_the_x_bins and into_the_y_bins to handle the x
    and y bins respectively.

    Parameters
    ----------
    points : ndarray
        Array of n number of point coordinates (x,y) of shape (n,2)
    x_bin_size : float, optional
        Size of x bins, i.e. strip width, in the same units as the projection
        used for buildings centroids
    y_bin_size : float, optional
        Size of y bins, i.e. strip width, in the same units as the projection
        used for building centroids

    Returns
    -------
    points_in_bins : ndarray
        Array of shape (n, 4)  containing all the coordinates of n points with
        indices of the bins each point falls in in `x_bins` and `y_bins`.
        Format: [latitude(y), longitude(x), `yBin` index, `xBin` index
                    ...          ...            ...           ...    ]
    y_bins : ndarray
        Array of shape (n, 1) containing latitude values for each bin
    x_bins : ndarray
        Array of shape (n, 1) containing longitude values for each bin

    """

    # -------------------Sort points into X bins------------------
    def into_the_x_bins(points, bin_size=0.005):
        """Sorts points into bins based on their longitude (x) value.

        Bins are evenly spaced on intervals defined by `bin_size.

        Parameters
        ----------
        points : ndarray
             Array of n number of point coordinates (x,y) of shape (n,2)
        bin_size : float, optional
            Width of each bin, aka strip

        Returns
        -------
        bin_assignment : ndarray
            Bin index for each point
        bins : ndarray
            Coordinates of the center of each bin

        """

        x_max = max(points[:,0])
        x_min = min(points[:,0])
        bins = np.arange(x_min, (x_max + bin_size), step=bin_size)

        # build extra column of which bin index each point goes in
        bin_assignment = np.zeros(np.shape(points[:,0]))

        # put points into whichever bin they are closest to
        for i, _ in enumerate(bin_assignment):
            diffs = np.abs(points[i,0] - bins[:])
            bin_index = np.where(min(diffs) == np.abs(points[i,0] - bins[:]))[0]

            bin_assignment[i] = bin_index

        return bin_assignment, bins

    # -------------------Sort points into y bins------------------
    def into_the_y_bins(points, bin_size=0.005):
        """Sorts points into bins based on their latitude (y) value.

        Bins are evenly spaced on intervals defined by `bin_size.`

        Parameters
        ----------
        points : ndarray
            Array of n number of point coordinates (x,y) of shape (n,2)
        bin_size : float, optional
            Width of each bin aka strip

        Returns
        -------
        bin_assignment : ndarray
            Bin index for each point
        bins : ndarray
            Coordinates of the center of each bin

        """

        y_max = max(points[:,1])
        y_min = min(points[:,1])
        bins = np.arange(y_min, (y_max + bin_size), step=bin_size)


        # build extra column of which bin index each point goes in
        bin_assignment = np.zeros(np.shape(points[:,1]))

        # put points into whichever bin they are closest to
        for i, _ in enumerate(bin_assignment):
            diffs = np.abs(points[i,1] - bins[:])
            bin_index = np.where(min(diffs) ==
                                 np.abs(points[i,1] - bins[:]))[0]

            bin_assignment[i] = bin_index

        return bin_assignment, bins

    bin_x_indices, x_bins = into_the_x_bins(points, x_bin_size)
    bin_y_indices, y_bins = into_the_y_bins(points, y_bin_size)

    points_in_bins = np.vstack((points[:,0],
                                points[:,1],
                                bin_y_indices,
                                bin_x_indices)).T

    return points_in_bins, x_bins, y_bins

# ----------------Go through x_bins to find gaps---------------
def find_lat_gaps(points, bins, gap_length_threshold=0.05):
    """Finds gaps in longitude bins.

    For each longitude bin, this function takes all points that fall within
    that bin, and compresses them to one dimension by only considering their
    latitude coordinate, so that the fall on a number line along the bin's
    long axis. Gaps are found by finding the distance between each
    successive point, and filtering out all gaps that do not meet a minimum
    length threshold.

    Parameters
    ----------
    points :  array_like
        Array of points including coordinates, y Bin indices, and x bin
        indices
    bins : array_like
        Array of bin longitude (x) coordinates.
    gap_length_threshold : float, optional
        Minimum length gaps must meet to be returned

    Returns
    -------
    gaps : list
        List of gaps, where each individual gap is a list containing the bin
        index, bin longitude coordinate, endpoint 1 index, endpoint 1 latiude
        coordinate, endpoint 2 index, endpoint 2 latitude coordinate, and
        length.

    """

    gaps = []

    for i, binn in enumerate(bins):
        # Find indices of points in that bin
        indices_in_bin = np.where(points[:,3] == i)[0]

        # If there aren't any gaps in this bin, skip to the next one
        if np.shape(indices_in_bin)[0] == 0:
            continue

        # Sort latitudes into numerical order
        lats_in_bin = points[indices_in_bin,1]
        lats_sorted = lats_in_bin[np.argsort(lats_in_bin)]

        # Calcuate distances between each successive point
        lats_bottom = lats_sorted[0:len(lats_sorted) - 1]
        lats_top = lats_sorted[1:len(lats_sorted)]
        succ_dists = lats_top - lats_bottom
        successive_dists = succ_dists

        # Do some basic stats
        try:
            dist_max = max(successive_dists)

            # If large gaps are present, where are they?
            if dist_max >= (gap_length_threshold):
                big_dist_inds = np.where(successive_dists >=
                                         (gap_length_threshold))[0]

                # Append each gap's info to list of all gaps
                for gap_ind in big_dist_inds:
                    this_gap = [i,
                                binn,
                                gap_ind,
                                lats_sorted[gap_ind],
                                gap_ind + 1,
                                lats_sorted[gap_ind + 1],
                                successive_dists[gap_ind]]
                    gaps.append(this_gap)
        except Exception:
            pass
        finally:
            continue

    return gaps

# ----------------Go through y_bins to find gaps---------------
def find_lon_gaps(points, bins, gap_length_threshold=0.05):
    """Finds data gaps in latitude bins.

    For each latitude bin, this function takes all points that fall within
    that bin, and compresses them to one dimension by only considering their
    longitude coordinate, so that the fall on a number line along the bin's
    long axis. Gaps are found by finding the distance between each
    successive point, and filtering out all gaps that do not meet a minimum
    length threshold.

    Parameters
    ----------
    points :  array_like
        Array of points including coordinates, y Bin indices, and x bin indices
    bins : array_like
        Array of bin latitude (y) coordinates.
    gap_length_threshold : float, optional
        Minimum length gaps must meet to be returned

    Returns
    -------
    gaps : list
        List of gaps, where each individual gap is a list containing the bin
        index, bin latitude coordinate, endpoint 1 index, endpoint 1 longitude
        coordinate, endpoint 2 index, endpoint 2 longitude coordinate, and
        length.

    """

    gaps = []

    for i, binn in enumerate(bins):
        # Find indices of points in that bin
        indices_in_bin = np.where(points[:,2] == i)[0]

        # If there aren't any gaps in this bin, skip to the next one
        if np.shape(indices_in_bin)[0] == 0:
            i += 1
            continue

        # Sort latitudes into numerical order
        lons_in_bin = points[indices_in_bin,0]
        lons_sorted = lons_in_bin[np.argsort(lons_in_bin)]

        # Calcuate distances between each successive point
        lons_bottom = lons_sorted[0:len(lons_sorted) - 1]
        lons_top = lons_sorted[1:len(lons_sorted)]
        succ_dists = lons_top - lons_bottom
        successive_dists = succ_dists

        # Do some basic stats
        try:
            dist_max = max(successive_dists)

            # If large gaps are present, where are they?
            if dist_max >= (gap_length_threshold):
                big_dist_inds = np.where(successive_dists >=
                                         (gap_length_threshold))[0]
                # Append each gap's info to list of all gaps
                for gap_ind in big_dist_inds:
                    this_gap = [i,
                                binn,
                                gap_ind,
                                lons_sorted[gap_ind],
                                gap_ind + 1,
                                lons_sorted[gap_ind + 1],
                                successive_dists[gap_ind]]
                    gaps.append(this_gap)
        except Exception:
            pass
        finally:
            i += 1
    return gaps

# ---------------------Find Adjacent gaps---------------------
def is_adjacent(gap_1, gap_2, gap_size):
    """Tests to see if two gaps are adjacent.

    Parameters
    ----------
    gap_1 : array_like
        The first gap to test, containing the bin index, bin lat or lon
        coordinate, endpoint 1 index, endpoint 1 lat or lon coordinate,
        endpoint 2 index, endpoint 2 lat or lon coordinate, and length.
    gap_2 : array_like
        The first gap to test, containing the bin index, bin lat or lon
        cooridinate, endpoint 1 index, endpoint 1 lat or lon coordinate,
        endpoint 2 index, endpoint 2 lat or lon coordinate, and length.
    gap_size : float
        The width of each gap or bin

    Returns
    -------
    int
        0 if not adjacent, 1 if partially adjacent (one pair of endpoints
        match), and 2 if fully adjacent (both pairs of endpoints match)

    Notes
    -----
    Both gaps must have the same orientation.
    This function is not used and I'm not 100% sure it works properly.

    """

    gap_1_bin = gap_1[1]
    gap_2_bin = gap_2[1]
    if np.abs(gap_1_bin - gap_2_bin) > gap_size:
        return 0

    else:
        # Find if only one or two endpoints are close (within some threshold)
        gap_1_end_1 = np.asarray([gap_1[1], gap_1[3]])
        gap_1_end_2 = np.asarray([gap_1[1], gap_1[5]])
        gap_2_end_1 = np.asarray([gap_2[1], gap_2[3]])
        gap_2_end_2 = np.asarray([gap_2[1], gap_2[5]])

        gap_end_dists = [np.linalg.norm(gap_1_end_1 - gap_2_end_1),
                         np.linalg.norm(gap_1_end_1 - gap_2_end_2),
                         np.linalg.norm(gap_1_end_2 - gap_2_end_1),
                         np.linalg.norm(gap_1_end_2 - gap_2_end_2)]

        num_close_ends = 0
        for dist in gap_end_dists:
            if dist < .05: # If distance is within some threshold distance
                num_close_ends += 1

                if num_close_ends == 2:
                    return num_close_ends

            return num_close_ends

# --------------Find if a pair of segments cross--------------
def does_cross(x_gap, y_gap):
    """Finds if a y gap and x gap cross.

    Paramters
    ---------
    x_gap : array_like
        a single `x_gap`, containing the bin index, bin lon coordinate,
        endpoint 1 index, endpoint 1 lat coordinate, endpoint 2 index, endpoint
        2 coordinate, and length
    y_gap : array_like
        a single `y_gap`, containing the bin index, bin lat coordinate,
        endpoint 1 index, endpoint 1 lon coordinate, endpoint 2 index, endpoint
        2 coordinate, and length

    Returns
    -------
    boolean
        True if `x_gap` and `y_gap` do cross, False if they don't

    Notes
    -----
    x_gap and y_gap can actually be interchanged. Can also correctly handle
    parallel gaps/line segments, returning False

    """

    # Put all coordinates into easier to deal with variables
    x_G_x = x_gap[1]
    x_G_y1 = x_gap[3]
    x_G_y2 = x_gap[5]
    y_G_y = y_gap[1]
    y_G_x1 = y_gap[3]
    y_G_x2 = y_gap[5]

    if (y_G_x1<=x_G_x<=y_G_x2) and (x_G_y1<=y_G_y<=x_G_y2):
        return True

    return False

# ---------------------Find intersections---------------------
def find_intersections(gap_LineStrings):
    """Finds intersections amongst a set of LineStrings.

    Finds intersections between LineStrings using the
    shapely.LineString.intersection method.

    Parameters
    ----------
    gap_LineStrings : list
        List containing LineString objects

    Returns
    -------
    intersections : list
        List containing shapely points of each intersection between lines in
        `gap_LineStrings`

    Notes
    -----
    `gap_LineStrings` should be a list of Linestrings defining line segments,
    not multilines, so the maximum number of intersections between a pair of
    items in `gap_LineStrings` should be 1, and `intersections` should only
    contain points, not multipoints.

    """

    intersections = []
    for y, _ in enumerate(gap_LineStrings):
        ln1 = gap_LineStrings[y]
        for h, __ in enumerate(gap_LineStrings):
            ln2 = gap_LineStrings[h]
            cross = ln1.intersection(ln2)
            if cross.is_empty or y == h or isinstance(cross, LineString):
                continue
            else:
                intersections.append(cross)

    return intersections

# -----------Filter out gaps with few intersections-----------
def intersection_filter(x_gaps,
                        y_gaps,
                        x_min_intersections=3,
                        y_min_intersections=3):
    """Removes lines that don't meet a minimum number of intersections.

    Filters out gaps that don't intersect with a minimum number of other
    gaps, until no more  gaps can be filtered out.

    Parameters
    ----------
    x_gaps : array_like
        Array of gaps on the x bins containing containing the bin indices, bin
        lon coordinates, endpoint 1 indices, endpoint 1 lat coordinates,
        endpoint 2 indices, endpoint 2 coordinates, and length
    y_gaps : array_like
        Array of gaps on the y bins containing containing the bin indices, bin
        lat coordinates, endpoint 1 indices, endpoint 1 lon coordinates,
        endpoint 2 indices, endpoint 2 coordinates, and length
    x_min_intersections : int, optional
        Minumum number of intersections a gap in `x_gaps` must have with other
        gaps in order to be retained.
    y_min_intersections : int, optional
        Minumum number of intersections a gap in `y_gaps` must have with other
        gaps in order to be retained.

    Returns
    -------
    x_gaps : array_like
        Array of x_gaps that have at least the minimum number of intersections
        with other gaps
    y_gaps : array_like
        Array of y_gaps that have at least the minimum number of intersections
        with other gaps

    """

    # Convert gaps to np arrys
    x_gaps = np.asarray(x_gaps)
    y_gaps = np.asarray(y_gaps)

    # Filter repeatedly until things stop changing
    prev_x_gaps_num = 0
    prev_y_gaps_num = 0
    iterations = 0

    while True:
        x_gap_does_cross = np.zeros(np.shape(x_gaps[:,1])[0])
        y_gap_does_cross = np.zeros(np.shape(y_gaps[:,1])[0])

        for i, _ in enumerate(x_gaps[:,1]):
            thisx_gap = x_gaps[i,:]
            for o, __ in enumerate(y_gaps[:,1]):
                thisy_gap = y_gaps[o,:]

                if does_cross(thisx_gap, thisy_gap):
                    y_gap_does_cross[o] += 1
                    x_gap_does_cross[i] += 1

        # Remove gaps that don't have any connections
        connecting_x_gaps = np.where(x_gap_does_cross >=
                                     x_min_intersections)[0]
        x_gaps = x_gaps[connecting_x_gaps]
        connecting_y_gaps = np.where(y_gap_does_cross >=
                                     y_min_intersections)[0]
        y_gaps = y_gaps[connecting_y_gaps]

        # Compare number of gaps to previous iteration
        x_gaps_num_diff = np.shape(x_gaps[:,1])[0] - prev_x_gaps_num
        y_gaps_num_diff = np.shape(y_gaps[:,1])[0] - prev_y_gaps_num

        if x_gaps_num_diff == 0 and y_gaps_num_diff == 0:
            break

        # Save number of gaps for this iteration
        prev_x_gaps_num = np.shape(x_gaps[:,1])[0]
        prev_y_gaps_num = np.shape(y_gaps[:,1])[0]

        iterations += 1

    return x_gaps, y_gaps

# -------------Find clusters of connecting lines--------------
def find_clusters(x_gaps, y_gaps):
    """Finds discrete clusters of interconnecting lines.

    Gaps are often isolated from one another, resulting in discrete newtorks
    or clusters of intersecting lines. This function gathers all lines in
    each cluster and returns all lines sorted into separate lists for each
    cluster.

    Parameters
    ----------
    x_gaps : array_like
        Array of gaps on the x bins containing containing the bin indices, bin
        lon coordinates, endpoint 1 indices, endpoint 1 lat coordinates,
        endpoint 2 indices, endpoint 2 coordinates, and length
    y_gaps : array_like
        Array of gaps on the y bins containing containing the bin indices,
        bin lat coordinates, endpoint 1 indices, endpoint 1 lon coordinates,
        endpoint 2 indices, endpoint 2 coordinates, and length

    Returns
    -------
    all_gaps : ndarray
        Array of all all x and y gaps stacked together
    gap_cluster_IDs : ndarray
        Array of unique ID numbers for each cluster, indexing from one.
    clusters : list
        List containing a list of indices refering to `all_gaps` for each
        cluster
    split_index : int
        Index of where `all_gaps` shifts from being x_gaps to y_gaps

    """

    def take_a_walk(gaps,start_ind,done_inds=[],cross_inds=[]):
        """Finds all lines in a network of interconnecting lines.

        Starting from one gap/line segment, this function gets all other
        line segments that intersect with the start line segment, and adds
        the indices of those intersecting lines in `gaps` to a list. Then,
        for each intersecting line segment not already in the list of
        intersecting indices, the function recursively calls itself to find
        all line segments that intersect with each of those line segments,
        and then calls itself again, etc. This finally ends when all line
        segments of a cluster are added to `cross_inds` and no more recursive
        calls are made.

        Parameters
        ----------
        gaps : array_like
            `all_gaps` from the parent function
        start_ind : int
            Index in `gaps` of the first gap to test
        done_inds : array_like, optional
            Indices of gaps that have already been tested
        cross_inds : array_like, optional
            Indices of line segments that cross at least one other line segment
            in the cluster

        Returns
        -------
        cross_inds : array_like
            Indices in `gaps` (`all_gaps` in parent function) of line segments
            that are in the cluster

        """

        # Make sure we haven't already ran this index
        if start_ind in done_inds:
            return
        else:
            done_inds.append(start_ind)

        # Define test gap
        test_gap = gaps[start_ind,:]

        # Find which other gaps intersect with our test gap
        for i, _ in enumerate(gaps[:,1]):
            # If the gap crosses our test gap and isn't already in cross_inds,
            # then we append it. We also make recursive call of take_a_walk
            if does_cross(test_gap,gaps[i,:]) and not i in cross_inds:
                cross_inds.append(i)
                cross_inds.append(take_a_walk(gaps,
                                              i,
                                              done_inds,
                                              cross_inds=cross_inds))

        return cross_inds

    # Stack all gaps into one big array
    all_gaps = np.vstack([x_gaps,y_gaps])
    # Get the index that splits x_gaps and y_gaps
    split_index = np.shape(x_gaps[:,1])[0]
    # Create array of cluster IDs
    gap_cluster_ids = np.zeros(np.shape(all_gaps[:,1])[0])

    # Make lists of clusters and all indices in clusters
    clusters = []
    in_clusters = []

    # Make list of all gap indices
    all_gap_inds = list(range(0,len(all_gaps[:,0])))

    # start cluster ID
    cluster_id = 0

    # Sort into clusters
    while in_clusters.sort() != all_gap_inds:
        # Find the gap we want to walk from: the first gap that hasn't yet been
        # assigned to a cluster. If an IndexError is thrown, then all gaps have
        # been sorted into clusters and we break out of the loop
        try:
            walk_ind = np.where(gap_cluster_ids == 0)[0][0]
        except IndexError:
            break

        in_cluster = take_a_walk(all_gaps,
                                 walk_ind,
                                 done_inds=[],
                                 cross_inds=[])

        # take_a_walk returns a very messy list, including other lists.
        # We only want the integers from it.
        in_cluster = list([elm for elm in in_cluster if isinstance(elm, int)])
        # append all gaps in a cluster into list of lists of cluster indices
        clusters.append(in_cluster)
        # Add gaps to list of all clustered indices
        in_clusters += in_cluster
        # Assign all gaps in that cluster with their cluster ID
        gap_cluster_ids[in_cluster] = cluster_id
        # Move to next cluster ID
        cluster_id += 1

    return all_gaps, gap_cluster_ids, clusters, split_index

# ---------------Find intersections in clusters---------------
def cluster_intersections(x_inds, y_inds, gaps):
    """Finds intersections of lines in a cluster of connected lines.

    Finds all the intersection points between x_gaps and y_gaps, and returns a
    list of shapely points

    Parameters
    ----------
    x_inds : array_like
        Indices of x_gaps in the cluster
    y_inds : array_like
        Indices of y_gaps in the cluster
    gaps : array_like
        List of all gap line segment endpoints

    Returns
    -------
    intersections : list
        List containing shapely points of each intersection in the cluster

    """

    intersections = []

    x_gaps = list(itemgetter(*x_inds)(gaps))
    y_gaps = list(itemgetter(*y_inds)(gaps))


    # Make linestrings
    gap_LineStrings = []

    for gap in x_gaps:
        this_gap_ls = LineString(gap)
        gap_LineStrings.append(this_gap_ls)

    for gap in y_gaps:
        this_gap_ls = LineString(gap)
        gap_LineStrings.append(this_gap_ls)

    intersections = find_intersections(gap_LineStrings)

    return intersections

# -------------Generate polygons with alpha_shapes-------------
def generate_alpha_polygons(x_clusters, y_clusters, gaps, alpha):
    """Generates polygons for data gaps using alpha_shapes.

    Parameters
    ----------
    x_clusters : array_like
        List of x_gap indices in each cluster
    y_clusters : array_like
        List of y_gap indices in each cluster
    gaps : array_like
        List of all gap line segment endpoints
    alpha : int
        Alpha value for alpha_shape

    Returns
    -------
    gaps_df : GeoDataFrame
        Contains alpha shape polygons
    points_gdf : GeoDataFrame
        Contains a MultiPoint for each gap

    """

    def make_alpha_shape(points, alpha):
        """Generate alpha_shapes with the libpysal alpha_shape module.

        Parameters
        ----------
        points : MultiPoint object
            shapely multipoint object containing points of one cluster
        alpha : int
            The alpha value used for the alpha shapes function

        Returns
        -------
        shape : GeoDataFrame
            shapely polygon or multipolygon of alpha shapes

        """

        xs = np.asarray([point.x for point in points.geoms])
        ys = np.asarray([point.y for point in points.geoms])

        xy = np.vstack([xs,ys]).T

        shape = alpha_shape(xy, alpha)
        return shape

    shapes = []
    all_inters = []

    for i, _ in enumerate(x_clusters):
        x_inds = x_clusters[i]
        y_inds = y_clusters[i]

        inters = cluster_intersections(x_inds, y_inds, gaps)
        # Add gap segment endpoints to the gap intersections
        x_gaps = list(itemgetter(*x_inds)(gaps))
        y_gaps = list(itemgetter(*y_inds)(gaps))
        cluster_gaps = [x_gaps, y_gaps] 
        gap_ends = list(chain(*cluster_gaps))
        gap_ends = list(chain(*gap_ends))
        gap_ends = list(map(list, gap_ends))
        endpoints = MultiPoint(gap_ends)
        inters = MultiPoint(inters)
        inters = shapely.ops.unary_union([endpoints,inters])
        #print(type(inters))

        a_shape = make_alpha_shape(inters, alpha)[0]

        inters = MultiPoint(inters)
        all_inters.append([inters])

        if isinstance(a_shape, MultiPolygon): # Only put polygons into list
            for sh in a_shape:
                shapes.append(sh)
        else:
            shapes.append(a_shape)

    gaps_df = gpd.GeoDataFrame(shapes,columns=['geometry'],crs="EPSG:4326")
    gaps_df.set_geometry(col='geometry', inplace=True)


    points_gdf = gpd.GeoDataFrame(all_inters,
                                  columns=['geometry'],
                                  crs="EPSG:4326")
    points_gdf.set_geometry(col='geometry', inplace=True)

    return gaps_df, points_gdf

def mind_the_gap(in_points,
                 x_bin_size,
                 y_bin_size,
                 x_gap_len_threshold,
                 y_gap_len_threshold,
                 x_min_intersections,
                 y_min_intersections,
                 alpha=15,
                 cluster_points=False,
                 write_points = False):
    """Finds gaps in geographic point data.

    Given a set of points in 2D space, this function will find gaps in
    points, with adjustable sensitivity. This is best suited to identify
    rectilinear systematic gaps, such as those resulting from missing
    imagery. However it can also find gaps resulting from natural
    features such as lakes or forests. Produces either polygons representing 
    gaps or points filling in the gap area.

    Parameters :
    in_points : GeoDataFrame
        Input GeoDataFrame of points (e.g., building footprints)
    x_bin_size : float
        Width of vertical strips to identify gaps in whatever units the data
        is projected in
    y_bin_size : float
        Width of horizontal strips to identify gaps in whatever units the data
        is projected in
    x_gap_len_threshold : float
        Minimum length data projection units for an x_gap to be retained
    y_gap_len_threshold : float
        Minimum length in projection units for a y_gap to be retained
    x_min_intersections : int
        Minimum number of intersections to filter gap lines
    y_min_intersections : int
        Minimum number of intersections to filter gap lines
    cluster_points : boolean
        True with 'alpha' `polygon_type` returns a geodataframe of MultiPoints
        for each gap as well as polygons
    write_points : boolean
        If True, this function will return a GeoDataFrame of points that fill
        in the data gap instead of polygons.

    Returns
    -------
    GeoDataFrame
        Polygons and/or points representing the data gap

    """

    def gen_gap_LineStrings(x_gaps, y_gaps):
        """Converts data gaps from ndarrays to shapely linestrings"""

        x_gaps = np.asarray(x_gaps)
        x_gap_segments = []
        x_gap_LineStrings = []
        for o, _ in enumerate(x_gaps[:,0]):
            # Get segments ready to plot
            this_gapSegment = [(x_gaps[o,1], x_gaps[o,3]),\
                              (x_gaps[o,1], x_gaps[o,5])]
            x_gap_segments.append(this_gapSegment)

            # Make LineStrings for Shapely
            this_line = LineString([(x_gaps[o,1], x_gaps[o,3]),\
                                   (x_gaps[o,1], x_gaps[o,5])])
            x_gap_LineStrings.append(this_line)

        all_gap_segments = x_gap_segments
        all_gap_LineStrings = x_gap_LineStrings
        y_gaps = np.asarray(y_gaps)
        y_gap_segments = []
        y_gap_LineStrings = []
        for u, __ in enumerate(y_gaps[:,0]):
            # Get segments ready to plot
            this_gapSegment = [(y_gaps[u,3], y_gaps[u,1]),\
                               (y_gaps[u,5], y_gaps[u,1])]
            y_gap_segments.append(this_gapSegment)
            all_gap_segments.append(this_gapSegment)

            # Make LineStrings for Shapely
            this_line = LineString([(y_gaps[u,3], y_gaps[u,1]), \
                                    (y_gaps[u,5], y_gaps[u,1])])
            y_gap_LineStrings.append(this_line)
            all_gap_LineStrings.append(this_line)

        return all_gap_LineStrings, all_gap_segments, x_gap_LineStrings, \
            y_gap_LineStrings

    # Check for bad value
    #if polygon_type != 'rim' and polygon_type != 'alpha':
    #    raise ValueError("plolygon_type must be 'rim' or 'alpha'.")

    #Load in building centroids
    point_coords = get_coordinates(in_points)

    # Add columns to point coordinates of which Lon and Lat bins it goes in
    stacked, x_bins, y_bins = into_the_bins(point_coords,
                                            x_bin_size,
                                            y_bin_size)

    x_gaps = find_lat_gaps(stacked, x_bins, x_gap_len_threshold)
    y_gaps = find_lon_gaps(stacked, y_bins, y_gap_len_threshold)

    # ---------Filter out gap strips without intersections--------
    x_gaps, y_gaps = intersection_filter(x_gaps,
                                         y_gaps,
                                         x_min_intersections,
                                         y_min_intersections)

    # ------------------Generate gap LineStrings------------------
    all_gap_LineStrings, all_gap_segments, x_gap_LineStrings, \
    y_gap_LineStrings = gen_gap_LineStrings(x_gaps, y_gaps)

    # ---------------Find intersections with shapely--------------
    intersections = find_intersections(all_gap_LineStrings)

    # Make and return geodataframe of points if that is desired
    if write_points:
        points_gdf = gpd.GeoDataFrame(intersections,
                                      columns=['geometry'],
                                      crs="EPSG:4326")
        points_gdf.set_geometry(col='geometry', inplace=True)

        return points_gdf

    # ------------------Sort points into clusters-----------------
    all_gaps, ids, gap_clusters, split_ind = find_clusters(x_gaps,y_gaps)

    # Need to separate x_gaps and y_gaps for each cluster
    cluster_x = []
    cluster_y = []
    for cluster in gap_clusters:
        this_cluster_x_gaps = []
        this_cluster_y_gaps = []
        for gap in cluster:
            if gap < split_ind:
                this_cluster_x_gaps.append(gap)
            else:
                this_cluster_y_gaps.append(gap)
        cluster_x.append(this_cluster_x_gaps)
        cluster_y.append(this_cluster_y_gaps)

    # ------------------------Make polygons-----------------------
    polygons, points = generate_alpha_polygons(cluster_x,
                                               cluster_y,
                                               all_gap_segments,
                                               alpha)
    if cluster_points:
        return polygons, points
    return polygons
