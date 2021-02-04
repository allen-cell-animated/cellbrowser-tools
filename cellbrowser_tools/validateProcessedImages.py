#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
from typing import NamedTuple
import csv
from . import dataHandoffUtils as lkutils
from . import dataset_constants
from .dataset_constants import DataField
import json
import logging
import os
import pandas as pd
from pathlib import Path
import quilt3
import random
from sklearn.cluster import KMeans, SpectralClustering, AgglomerativeClustering
import sys
from typing import Union, Dict, List
from collections import OrderedDict

import featuredb as fh

log = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)

FILE_INFO_COLUMNS = [
    "CellId",
    "FOVId",
    "CellLineName",
    "thumbnailPath",
    "volumeviewerPath",
    "fovThumbnailPath",
    "fovVolumeviewerPath",
]

# clustering algorithm defaults
DEFAULT_MIN_CLUSTERS = 2
DEFAULT_MAX_CLUSTERS = 8
DEFAULT_CLUSTER_STEP = 1

# ignore columns for clustering
# this is temporary as this is not future proof a better system for determining which
# features should actually be used in cluster calculation should be adopted at a later point
IGNORE_FEATURES_COLUMNS_DURING_CLUSTERING = [
    "Interphase and Mitotic Stages (stage)",
    "Interphase and Mitosis (stage)",
    "Cell Segmentation (complete)",
]

# type def
JSONList = List[Dict[str, Union[int, str, float]]]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate data files and dump aggregate data to json."
        "Example: python validateProcessedImages.py"
    )

    parser.add_argument("prefs", nargs="?", default="prefs.json", help="prefs file")

    args = parser.parse_args()

    return args


def make_path(dir0, dir1, filename):
    # return os.path.join(dir0, dir1, filename)
    return dir0 + "/" + dir1 + "/" + filename


def do_image(args, prefs, rows, index, total_jobs, channel_name_list):
    # use row 0 as the "full field" row
    fovrow = rows[0]

    jobname = lkutils.get_fov_name_from_row(fovrow)

    imageName = jobname

    data_dir = prefs["images_dir"]
    thumbs_dir = prefs["thumbs_dir"]
    cell_line = fovrow[DataField.CellLine]

    names = [imageName]
    for row in rows:
        n = lkutils.get_cell_name(
            row[DataField.CellId], row[DataField.FOVId], row[DataField.CellLine]
        )
        names.append(n)

    # exts = [".ome.tif", ".png"]
    # check existence of ome.tif and png.

    err = False
    for f in names:
        # check for thumbnail
        fullf = make_path(thumbs_dir, cell_line, f + ".png")
        if not os.path.isfile(fullf):
            err = True
            log.info("ERROR: " + jobname + ": Could not find file: " + fullf)

        # check for atlas meta
        fullaj = make_path(thumbs_dir, cell_line, f + "_atlas.json")
        if not os.path.isfile(fullaj):
            err = True
            log.info("ERROR: " + jobname + ": Could not find file: " + fullaj)
        else:
            # load file and look at channel names
            with open(fullaj, "r") as json_file:
                atlasjsondata = json.load(json_file)
                for n in atlasjsondata["channel_names"]:
                    if n not in channel_name_list:
                        channel_name_list.append(n)

        # expect 3 atlas png files
        for i in ["0", "1", "2"]:
            fullat = make_path(thumbs_dir, cell_line, f + "_atlas_" + i + ".png")
            if not os.path.isfile(fullat):
                err = True
                log.info("ERROR: " + jobname + ": Could not find file: " + fullat)

        # check for image
        fullf = make_path(data_dir, cell_line, f + ".ome.tif")
        if not os.path.isfile(fullf):
            err = True
            log.info("ERROR: " + jobname + ": Could not find file: " + fullf)

    outrows = []
    if err is not True:
        outrows.append(
            {
                "file_id": "F" + str(int(fovrow[DataField.FOVId])),
                "file_name": imageName + ".ome.tif",
                "read_path": make_path(data_dir, cell_line, imageName + ".ome.tif"),
                "file_size": os.path.getsize(
                    make_path(data_dir, cell_line, imageName + ".ome.tif")
                ),
                "CellLineName": cell_line,
            }
        )
        for row in rows:
            seg = row[DataField.CellId]
            n = imageName + "_" + str(int(seg))
            outrows.append(
                {
                    "file_id": "C" + str(int(seg)),
                    "file_name": n + ".ome.tif",
                    "read_path": make_path(data_dir, cell_line, n + ".ome.tif"),
                    "file_size": os.path.getsize(
                        make_path(data_dir, cell_line, n + ".ome.tif")
                    ),
                    "CellLineName": cell_line,
                }
            )
    return outrows, err


def compute_clusters_on_json_handoff(
    handoff: JSONList,
    min_clusters: int = DEFAULT_MIN_CLUSTERS,
    max_clusters: int = DEFAULT_MAX_CLUSTERS,
    cluster_step: int = DEFAULT_CLUSTER_STEP,
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
    features = pd.DataFrame([row["measured_features"] for row in handoff])

    # use only specific clustering features
    # this will return a dataframe that uses features as its base but with the ignore columns dropped
    clustering_data = features.drop(IGNORE_FEATURES_COLUMNS_DURING_CLUSTERING, axis=1)

    # normalize the features by zscoring every column
    for col in clustering_data:
        clustering_data[col] = (
            clustering_data[col] - clustering_data[col].mean()
        ) / clustering_data[col].std(ddof=0)

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
            n_neighbors=5,
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
        handoff.append(
            {
                "file_info": row,
                "measured_features": features[i],
                "clusters": {
                    "KMeans": kmeans[i],
                    "Agglomerative": agglo[i],
                    "Spectral": spectral[i],
                },
            }
        )

    return handoff


def get_quilt_actk_features():
    dest_path = Path("./temp_dir")
    # features come from quilt data
    # look though whole package
    p = quilt3.Package.browse("aics/actk", registry="s3://allencell")
    # download just the feature data locally -- takes an hour at 300kbps
    p.install(
        "aics/actk/master/singlecellfeatures", registry="s3://allencell", dest=dest_path
    )
    # load the features manifest as a df
    df_feats_manifest = p["master/singlecellfeatures/manifest.parquet"]()
    # read all the jsons you downloaded in as a df
    cell_features_rows = []
    for cell in df_feats_manifest[["CellId", "CellFeaturesPath"]].iterrows():
        row = {"CellId": cell["CellId"]}
        with open(dest_path / Path(cell["CellFeaturesPath"]), mode="r") as f:
            feats = json.load(f)
            pd.concat
            row.update(feats)
        cell_features_rows.append(row)
    df_feats = pd.DataFrame(cell_features_rows)

    return df_feats


def make_rand_features(dataset, count=6):
    df = dataset[["CellId"]]
    for i in range(count):
        rand0 = [random.random() for cell in range(len(dataset))]
        df[f"Random{i}"] = rand0
    return df


def build_cfe_dataset_2020(prefs):
    # read dataset into dataframe
    data = lkutils.collect_csv_data_rows(
        fovids=prefs.get("fovs"), cell_lines=prefs.get("cell_lines")
    )
    log.info(f"Number of total cell rows: {len(data)}")
    # Per-cell
    #     {
    #     "file_info": {
    #         "CellId": 2,
    #         "FOVId": 1,
    #         "CellLineName": "AICS-13"
    #     },
    #     "measured_features": {
    #         "Apical Proximity (unitless)": 2.1123541,
    #         ...
    #     }
    # }
    file_infos = data[["CellId", "FOVId", "CellLine"]]
    # add file path locations
    file_infos["thumbnailPath"] = file_infos.apply(
        lambda x: f'{x["CellLine"]}/{lkutils.get_cell_name(x["CellId"], x["FOVId"], x["CellLine"])}.png',
        axis=1,
    )
    file_infos["volumeviewerPath"] = file_infos.apply(
        lambda x: f'{x["CellLine"]}/{lkutils.get_cell_name(x["CellId"], x["FOVId"], x["CellLine"])}_atlas.json',
        axis=1,
    )
    file_infos["fovThumbnailPath"] = file_infos.apply(
        lambda x: f'{x["CellLine"]}/{lkutils.get_fov_name(x["FOVId"], x["CellLine"])}.png',
        axis=1,
    )
    file_infos["fovVolumeviewerPath"] = file_infos.apply(
        lambda x: f'{x["CellLine"]}/{lkutils.get_fov_name(x["FOVId"], x["CellLine"])}_atlas.json',
        axis=1,
    )

    # need CellLineName here
    file_infos.rename(columns={"CellLine": "CellLineName"}, inplace=True)

    log.info("Collecting feature data")
    df_feats = lkutils.get_csv_features()
    feature_names = df_feats.columns.tolist()
    feature_names.remove("CellId")

    # df_feats = get_quilt_actk_features()
    # df_feats = make_rand_features(data, 6)
    if len(df_feats) != len(file_infos):
        raise ValueError(
            f"Features list has different number of cells ({len(df_feats)}) than source dataset ({len(file_infos)})"
        )

    # turn features into arrays per cell id.  order is important.
    # only_feats = df_feats.drop(columns=["CellId"]).values.tolist()
    # df_feats["features"] = only_feats

    # merge together on cellid
    dataset_df = pd.merge(file_infos, df_feats, how="inner", on="CellId")
    if len(dataset_df) != len(file_infos):
        raise ValueError(
            f"Features list has different cellIds than source dataset. Can not merge."
        )

    # make each row into two arrays of values
    # format
    dataset = []
    for i, row in dataset_df.iterrows():
        # i want to preserve key(column) order here.
        rowdict = row.to_dict(into=OrderedDict)
        dataset.append(
            {
                "file_info": [rowdict[x] for x in FILE_INFO_COLUMNS],
                "features": [rowdict[x] for x in feature_names],
            }
        )

    # write out the final data set
    with open(
        os.path.join(prefs.get("out_dir"), dataset_constants.FEATURE_DATA_FILENAME),
        "w",
        newline="",
    ) as output_file:
        # most compact json encoding
        output_file.write(json.dumps(dataset, separators=(",", ":")))


def convert_old_feature_data_format(prefs, old_json: str):
    # load file such that all entries remain in key order
    data = json.load(open(old_json), object_pairs_hook=OrderedDict)
    # make each row into two dicts
    # format
    dataset = []
    for i, row in enumerate(data):
        rowdict = row
        file_info = rowdict["file_info"]
        features = rowdict["measured_features"]
        dataset.append(
            {
                "file_info": [file_info[x] for x in file_info],
                "features": [features[x] for x in features],
            }
        )
    # write out the final data set
    with open(
        os.path.join(prefs.get("out_dir"), dataset_constants.FEATURE_DATA_FILENAME),
        "w",
        newline="",
    ) as output_file:
        output_file.write(json.dumps(dataset, separators=(",", ":")))


def build_feature_data(prefs, groups):
    configfile = "//allen/aics/animated-cell/Dan/featurehandoff/prod.json"

    class FeatureDataSource(NamedTuple):
        name: str
        version: str

    data_sources = [
        FeatureDataSource("aics-feature", "0.2.0"),
        FeatureDataSource("aics-cell-segmentation", "1.0.0"),
        FeatureDataSource("aics-mitosis-classifier-mitotic", "1.0.0"),
        FeatureDataSource("aics-mitosis-classifier-four-stage", "1.0.0"),
    ]
    allfeaturedata = None
    for data_source in data_sources:
        featuredata = fh.get_full_handoff(
            algorithm_name=data_source.name,
            algorithm_version=data_source.version,
            config=configfile,
        )
        if allfeaturedata is None:
            allfeaturedata = featuredata
        else:
            allfeaturedata = pd.merge(
                allfeaturedata,
                featuredata,
                how="left",
                left_on=["CellId", "CellLineName", "FOVId"],
                right_on=["CellId", "CellLineName", "FOVId"],
            )

    jsondictlist = fh.df_to_json(allfeaturedata)
    jsondictlist = generate_filenames(jsondictlist)
    jsondictlist = compute_clusters_on_json_handoff(jsondictlist)

    with open(
        os.path.join(prefs.get("out_dir"), dataset_constants.FEATURE_DATA_FILENAME_OLD),
        "w",
        newline="",
    ) as output_file:
        output_file.write(json.dumps(jsondictlist))


def generate_filenames(handoff):
    for row in handoff:
        fi = row["file_info"]
        fi["thumbnailPath"] = f'{fi["CellLineName"]}/{fi["FOVId"]}_{fi["CellId"]}.png'
        fi[
            "volumeviewerPath"
        ] = f'{fi["CellLineName"]}/{fi["FOVId"]}_{fi["CellId"]}_atlas.json'
    return handoff


def validate_rows(groups, args, prefs):
    errorFovs = []
    allfiles = []
    channel_name_list = []
    # process each file
    # run serially
    for index, group in enumerate(groups):
        rows = group  # .to_dict(orient="records")
        filerows, err = do_image(
            args, prefs, rows, index, len(groups), channel_name_list
        )
        if err is True:
            errorFovs.append(str(rows[0]["FOVId"]))
        else:
            allfiles.extend(filerows)
        # if index % 1000 == 0:
        #     log.info(f"Processed {index}")

    # write out all collected channel names
    with open(
        os.path.join(prefs.get("out_status"), dataset_constants.CHANNEL_NAMES_FILENAME),
        "w",
        newline="",
    ) as channel_names_file:
        channel_names_file.write("\n".join(channel_name_list))

    # write out all FOVs identified with errors
    if len(errorFovs) > 0:
        with open(
            os.path.join(
                prefs.get("out_status"), dataset_constants.ERROR_FOVS_FILENAME
            ),
            "w",
            newline="",
        ) as error_file:
            error_file.write("\n".join(errorFovs))

    # write out all files for downloader service
    if len(allfiles) > 0:
        keys = allfiles[0].keys()
        with open(
            os.path.join(prefs.get("out_dir"), dataset_constants.FILE_LIST_FILENAME),
            "w",
            newline="",
        ) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(allfiles)


def validate_processed_images(args, prefs):
    # Read every cell image to be processed
    data = lkutils.collect_data_rows(fovids=prefs.get("fovs"))

    print("Number of total cell rows: " + str(len(data)))
    # group by fov id
    data_grouped = data.groupby("FOVId")
    total_jobs = len(data_grouped)
    print("Number of total FOVs: " + str(total_jobs))
    print("VALIDATING " + str(total_jobs) + " JOBS")

    groups = []
    for index, (fovid, group) in enumerate(data_grouped):
        groups.append(group)

    validate_rows(groups, args, prefs)

    # write out the cell_feature_analysis.json database
    build_feature_data(prefs, groups)


def main():
    args = parse_args()
    prefs = lkutils.setup_prefs(args.prefs)
    validate_processed_images(args, prefs)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
