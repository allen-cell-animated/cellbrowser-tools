#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import csv
import dataHandoffUtils as lkutils
import glob
import jobScheduler
import json
import numpy as np
import os
import pandas as pd
import platform
import random
import re
from sklearn.cluster import KMeans, SpectralClustering, AgglomerativeClustering
import sys
from typing import Union, Dict, List
import uploader.db_api as db_api

import featuredb as fh

from cellNameDb import CellNameDatabase
from processImageWithSegmentation import do_main_image

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)

# clustering algorithm defaults
DEFAULT_MIN_CLUSTERS = 2
DEFAULT_MAX_CLUSTERS = 8
DEFAULT_CLUSTER_STEP = 1

# ignore columns for clustering
# this is temporary as this is not future proof a better system for determining which
# features should actually be used in cluster calculation should be adopted at a later point
IGNORE_FEATURES_COLUMNS_DURING_CLUSTERING = [
    # "Cell Cycle State (unitless)",
    # "Cell Cycle State (curated)"
]

# type def
JSONList = List[Dict[str, Union[int, str, float]]]


def parse_args():
    parser = argparse.ArgumentParser(description='Validate data files and dump aggregate data to json.'
                                                 'Example: python validateProcessedImages.py')

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='prefs file')

    args = parser.parse_args()

    return args


def make_path(dir0, dir1, filename):
    # return os.path.join(dir0, dir1, filename)
    return dir0 + '/' + dir1 + '/' + filename


def do_image(args, prefs, rows, index, total_jobs, channel_name_list):
    # use row 0 as the "full field" row
    fovrow = rows[0]

    jobname = fovrow['FOV_3dcv_Name']

    imageName = jobname

    data_dir = prefs['out_ometifroot']
    thumbs_dir = prefs['out_thumbnailroot']
    cell_line = fovrow['CellLine']

    names = [imageName]
    for row in rows:
        seg = row['CellId']
        n = imageName + "_" + str(int(seg))
        # str(int(seg)) removes leading zeros
        names.append(n)

    exts = ['.ome.tif', '.png']
    # check existence of ome.tif and png.

    err = False
    for f in names:
        # check for thumbnail
        fullf = make_path(thumbs_dir, cell_line, f + '.png')
        if not os.path.isfile(fullf):
            err = True
            print("ERROR: " + jobname + ": Could not find file: " + fullf)

        # check for atlas meta
        fullaj = make_path(thumbs_dir, cell_line, f + '_atlas.json')
        if not os.path.isfile(fullaj):
            err = True
            print("ERROR: " + jobname + ": Could not find file: " + fullaj)
        else:
            # load file and look at channel names
            with open(fullaj, 'r') as json_file:
                atlasjsondata = json.load(json_file)
                for n in atlasjsondata['channel_names']:
                    if n not in channel_name_list:
                        channel_name_list.append(n)


        # expect 3 atlas png files
        for i in ['0', '1', '2']:
            fullat = make_path(thumbs_dir, cell_line, f + '_atlas_'+i+'.png')
            if not os.path.isfile(fullat):
                err = True
                print("ERROR: " + jobname + ": Could not find file: " + fullat)

        # check for image
        fullf = make_path(data_dir, cell_line, f + '.ome.tif')
        if not os.path.isfile(fullf):
            err = True
            print("ERROR: " + jobname + ": Could not find file: " + fullf)

    outrows = []
    if err is not True:
        outrows.append({
            "file_id": "F" + str(int(fovrow['FOVId'])),
            "file_name": imageName + '.ome.tif',
            "read_path": make_path(data_dir, cell_line, imageName + '.ome.tif'),
            "file_size": os.path.getsize(make_path(data_dir, cell_line, imageName + '.ome.tif')),
            "CellLineName": cell_line
        })
        for row in rows:
            seg = row['CellId']
            n = imageName + "_" + str(int(seg))
            outrows.append({
                "file_id": "C" + str(int(seg)),
                "file_name": n + '.ome.tif',
                "read_path": make_path(data_dir, cell_line, n + '.ome.tif'),
                "file_size": os.path.getsize(make_path(data_dir, cell_line, n + '.ome.tif')),
                "CellLineName": cell_line
            })
    return outrows, err


def compute_clusters_on_json_handoff(
    handoff: JSONList,
    min_clusters: int = DEFAULT_MIN_CLUSTERS,
    max_clusters: int = DEFAULT_MAX_CLUSTERS,
    cluster_step: int = DEFAULT_CLUSTER_STEP
) -> JSONList:
    """
    Generate clustering analysis on a json feature handoff blob.

    This adds a clusters blob to the json feature handoff blob that contains
    keys to each clustering algorithm used per cell. For each cluster algorithm,
    there will be a dictionary with keys as the parameter used to generate the
    cluster analysis, and values as which cluster the cell belongs to.

    #### Example
    ```
    >>> import featuredb as fh
    >>> handoff = fh.get_full_handoff("prod.json", "aics-feature", "1.0.1")
    >>> json_handoff = fh.df_to_json(handoff)
    >>> compute_clusters_on_json_handoff(json_handoff)
    [
        {
            "file_info": {
                "CellId": 2,
                "FOVId": 1,
                "CellLineName": "AICS-13"
            },
            "measured_features": {
                "Apical Proximity (unitless)": 2.1123541,
                ...
            },
            "clusters": {
                "KMeans": {
                    "2": 0,
                    "3": 0,
                    "4": 2,
                    "5": 2,
                    "6": 1,
                    "7": 5,
                    "8": 2
                },
                "Agglomerative": {
                    "2": 0,
                    "3": 1,
                    "4": 1,
                    "5": 1,
                    "6": 3,
                    "7": 4,
                    "8": 7
                },
                "Spectral": {
                    "2": 0,
                    "3": 4,
                    "4": 0,
                    "5": 0,
                    "6": 1,
                    "7": 7,
                    "8": 5
                }
            }
        }
        ...
    ]
    ```


    #### Parameters
    ##### handoff: List[Dict[str, Union[int, str, float]]]
    The output from a:
    `featuredb.df_to_json(featuredb.get_full_handoff(*args))`
    call. This is a list of dictionarys where each dictionary has a "file_info",
    and, "measured_features" blob.

    ##### min_clusters: int = 2
    For algorithms whose primary parameter is `n_clusters` based, what is the
    minimum amount of clusters to generate.

    ##### max_clusters: int = 8
    For algorithms whose primary parameter is `n_clusters` based, what is the
    maximum amount of clusters to generate.

    ##### cluster_step: int = 1
    For algorithms whose primary parameter is `n_clusters` based, what is the
    stepping distance to increase `n_clusters` by after each generation.


    #### Returns
    ##### handoff: List[Dict[str, Union[int, str, float]]]
    A list of dictionaries similar to that produced by
    `featuredb.df_to_json`, with an additional "clusters" key for each row.
    Each key in the clusters blob is the name of which clustering algorithm was
    used to generate that portion of the data. Inside that dictionary are
    key-value pairings that correspond to the parameter used to generate that
    cluster analysis and the resulting group.


    #### Errors

    """
    # This function accepts the json handoff instead of the dataframe handoff
    # as the json handoff is a more "complete" version of any handoff.
    # I claim it is more "complete" but what I truly mean by that is that the
    # features are explicitly stated due to them being all in the same block.
    # Why does this not pull and push directly to the database? Clustering
    # should be done as the last step after all feature handoffs have been
    # collected and merged. So while the clustering algorithm stays the same,
    # it is primarily the data used that changed.
    # Additionally, it makes more sense in my opinion to interact with a json
    # blob on ingest because this function spits out the same json but with
    # an additional child dictionary for each dictionary.

    # split the data into its parts
    meta = pd.DataFrame([row["file_info"] for row in handoff])
    features = pd.DataFrame([row["measured_features"]for row in handoff])

    # use only specific clustering features
    # this will return a dataframe that uses features as its base but with the ignore columns dropped
    clustering_data = features.drop(IGNORE_FEATURES_COLUMNS_DURING_CLUSTERING, axis=1)

    # normalize the features by zscoring every column
    for col in clustering_data:
        clustering_data[col] = (clustering_data[col] - clustering_data[col].mean()) / clustering_data[col].std(ddof=0)

    # generate kmeans
    kmeans = pd.DataFrame()
    for i in range(min_clusters, max_clusters + cluster_step, cluster_step):
        fitted = KMeans(n_clusters=i).fit(clustering_data)
        kmeans["{}".format(i)] = fitted.labels_

    # generate agglomerative
    agglo = pd.DataFrame()
    for i in range(min_clusters, max_clusters + cluster_step, cluster_step):
        fitted = AgglomerativeClustering(n_clusters=i).fit(clustering_data)
        agglo["{}".format(i)] = fitted.labels_

    # generate spectral
    spectral = pd.DataFrame()
    for i in range(min_clusters, max_clusters + cluster_step, cluster_step):
        fitted = SpectralClustering(
            n_clusters=i,
            eigen_solver="arpack",
            affinity="nearest_neighbors",
            n_neighbors=5
        ).fit(clustering_data)
        spectral["{}".format(i)] = fitted.labels_

    # cast all clusters to list of dict
    meta = meta.to_dict("records")
    features = features.to_dict("records")
    kmeans = kmeans.to_dict("records")
    agglo = agglo.to_dict("records")
    spectral = spectral.to_dict("records")

    # pandas stores the integer values as numpy.int64 which sometimes have
    # issues with being json serializable depending on OS and python install.
    # As a measure to ensure these clusters will be json seriablizable I
    # convert all key-value pairings for every row to base python int.
    kmeans = [{k: int(v) for k, v in r.items()} for r in kmeans]
    agglo = [{k: int(v) for k, v in r.items()} for r in agglo]
    spectral = [{k: int(v) for k, v in r.items()} for r in spectral]

    # format
    handoff = []
    for i, row in enumerate(meta):
        handoff.append({
            "file_info": row,
            "measured_features": features[i],
            "clusters": {
                "KMeans": kmeans[i],
                "Agglomerative": agglo[i],
                "Spectral": spectral[i]
            }
        })

    return handoff


def build_feature_data(prefs):
    configfile = "//allen/aics/animated-cell/Dan/featurehandoff/prod.json"

    data_sources = [
        ("aics-feature", "0.2.0"),
        # ("aics-mitosis-classifier-mitotic", "1.0.0"),
        # ("aics-mitosis-classifier-four-stage", "1.0.0"),
    ]
    allfeaturedata = None
    for data_source in data_sources:
        featuredata = fh.get_full_handoff(algorithm_name=data_source[0], algorithm_version=data_source[1], config=configfile)
        if allfeaturedata is None:
            allfeaturedata = featuredata
        else:
            allfeaturedata = pd.merge(allfeaturedata, featuredata, how='inner', left_on=['CellId', 'CellLineName', 'FOVId'], right_on=['CellId', 'CellLineName', 'FOVId'])

    allfeaturedata.dropna(inplace=True)

    jsondictlist = fh.df_to_json(allfeaturedata)
    jsondictlist = compute_clusters_on_json_handoff(jsondictlist)
    with open(os.path.join(prefs.get("out_status"), 'cell-feature-analysis.json'), 'w', newline="") as output_file:
        output_file.write(json.dumps(jsondictlist))


def do_main(args, prefs):
    # Read every cell image to be processed
    data = lkutils.collect_data_rows(fovids=prefs.get('fovs'))

    print('Number of total cell rows: ' + str(len(data)))
    # group by fov id
    data_grouped = data.groupby("FOVId")
    total_jobs = len(data_grouped)
    print('Number of total FOVs: ' + str(total_jobs))
    print('VALIDATING ' + str(total_jobs) + ' JOBS')

    #
    # arrange into list of lists of dicts?

    # one_of_each = data_grouped.first().reset_index()
    # data = data.to_dict(orient='records')

    errorFovs = []
    allfiles = []
    channel_name_list = []
    # process each file
    # run serially
    for index, (fovid, group) in enumerate(data_grouped):
        rows = group.to_dict(orient='records')
        filerows, err = do_image(args, prefs, rows, index, total_jobs, channel_name_list)
        if err is True:
            errorFovs.append(str(fovid))
        else:
            allfiles.extend(filerows)

    # write out all collected channel names
    with open(os.path.join(prefs.get("out_status"), 'allChannelNames.txt'), 'w', newline="") as channel_names_file:
        channel_names_file.write('\n'.join(channel_name_list))

    # write out all FOVs identified with errors
    if len(errorFovs) > 0:
        with open(os.path.join(prefs.get("out_status"), 'errorFovs.txt'), 'w', newline="") as error_file:
            error_file.write('\n'.join(errorFovs))

    # write out all files for downloader service
    if len(allfiles) > 0:
        keys = allfiles[0].keys()
        with open(os.path.join(prefs.get("out_status"), 'cellviewer-files.csv'), 'w', newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(allfiles)

    # write out the cell_feature_analysis.json database
    build_feature_data(prefs)


def main():
    args = parse_args()
    with open(args.prefs) as f:
        prefs = json.load(f)
    do_main(args, prefs)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
