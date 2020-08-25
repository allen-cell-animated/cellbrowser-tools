from . import dataset_constants
from .dataset_constants import DataField
import json
import labkey
from lkaccess import LabKey
import lkaccess.contexts
import logging
import os
import pandas as pd
from quilt3 import Package
import re
import sys

logging.basicConfig(level=logging.INFO)


# CAUTION:
#  if you run this function on a host and pass the result to a different host using a different sys.platform,
#  then the path will not be properly normalized!
def normalize_path(path):
    windowsroot = "\\\\allen\\"
    macroot = "/Volumes/"
    linuxroot = "/allen/"
    linuxroot2 = "//allen/"

    # 1. strip away the root.
    if path.startswith(windowsroot):
        path = path[len(windowsroot) :]
    elif path.startswith(linuxroot):
        path = path[len(linuxroot) :]
    elif path.startswith(linuxroot2):
        path = path[len(linuxroot2) :]
    elif path.startswith(macroot):
        path = path[len(macroot) :]
    else:
        # if the path does not reference a known root, don't try to change it.
        # it's probably a local path.
        return path

    # 2. split the path up into a list of dirs
    path_as_list = re.split(r"\\|/", path)

    # 3. insert the proper system root for this platform (without the trailing slash)
    dest_root = ""
    if sys.platform.startswith("darwin"):
        dest_root = macroot[:-1]
    elif sys.platform.startswith("linux"):
        dest_root = linuxroot[:-1]
    else:
        dest_root = windowsroot[:-1]

    path_as_list.insert(0, dest_root)

    out_path = os.path.join(*path_as_list)
    return out_path


def setup_prefs(json_path):
    with open(json_path) as f:
        prefs = json.load(f)

    # make the output directories if it doesnt exist
    if not os.path.exists(prefs["out_status"]):
        os.makedirs(prefs["out_status"])
    if not os.path.exists(prefs["out_dir"]):
        os.makedirs(prefs["out_dir"])
    images_dir = os.path.join(prefs["out_dir"], dataset_constants.IMAGES_DIR)
    if not os.path.exists(images_dir):
        os.makedirs(images_dir)
    prefs["images_dir"] = images_dir
    thumbs_dir = os.path.join(prefs["out_dir"], dataset_constants.THUMBNAILS_DIR)
    if not os.path.exists(thumbs_dir):
        os.makedirs(thumbs_dir)
    prefs["thumbs_dir"] = thumbs_dir
    atlas_dir = os.path.join(prefs["out_dir"], dataset_constants.ATLAS_DIR)
    if not os.path.exists(atlas_dir):
        os.makedirs(atlas_dir)
    prefs["atlas_dir"] = atlas_dir

    prefs["sbatch_output"] = os.path.join(
        prefs["out_status"], prefs["script_dir"], prefs["job_prefs"]["output"]
    )
    os.makedirs(os.path.dirname(prefs["sbatch_output"]), exist_ok=True)

    prefs["sbatch_error"] = os.path.join(
        prefs["out_status"], prefs["script_dir"], prefs["job_prefs"]["error"]
    )
    os.makedirs(os.path.dirname(prefs["sbatch_error"]), exist_ok=True)

    # record the location of the data object
    prefs["save_log_path"] = prefs["out_status"] + os.sep + prefs["data_log_name"]

    return prefs


def get_cellline_name_from_row(row):
    return row[DataField.CellLine]


# cellline must be 'AICS-#'
def get_fov_name(fovid, cellline):
    return f"{cellline}_{fovid}"


def get_fov_name_from_row(row):
    celllinename = get_cellline_name_from_row(row)
    fovid = row[DataField.FOVId]
    return get_fov_name(fovid, celllinename)


def check_dups(dfr, column, remove=True):
    dupes = dfr.duplicated(column)
    repeats = (dfr[dupes])[column].unique()
    if len(repeats) > 0:
        print("FOUND DUPLICATE DATA FOR THESE " + column + " KEYS:")
        print(*repeats, sep=" ")
        # print(repeats)
    if remove:
        dfr.drop_duplicates(subset=column, keep="first", inplace=True)


def collect_data_rows(fovids=None, raw_only=False, max_rows=None):
    # lk = LabKey(host="aics")
    lk = LabKey(server_context=lkaccess.contexts.PROD)

    print("REQUESTING DATA HANDOFF")
    lkdatarows = lk.dataset.get_pipeline_4_production_data()
    df_data_handoff = pd.DataFrame(lkdatarows)

    # verify the expected column names in the above query
    for field in dataset_constants.DataField:
        if field.value not in df_data_handoff.columns:
            raise f"Expected {field.value} to be in labkey dataset results."

    if fovids is not None and len(fovids) > 0:
        df_data_handoff = df_data_handoff[df_data_handoff["FOVId"].isin(fovids)]

    if max_rows is not None:
        df_data_handoff = df_data_handoff.head(max_rows)

    print("GOT DATA HANDOFF")

    # Merge Aligned and Source read path columns
    df_data_handoff[DataField.SourceReadPath] = df_data_handoff[DataField.AlignedImageReadPath].combine_first(df_data_handoff[DataField.SourceReadPath])

    if raw_only:
        return df_data_handoff

    # get mitotic state name for all cells
    mitoticdata = lk.select_rows_as_list(
        schema_name="processing",
        query_name="MitoticAnnotation",
        sort="MitoticAnnotation",
        # columns=["CellId", "MitoticStateId/Name", "Complete"]
        columns=["CellId", "MitoticStateId/Name"],
    )
    print("GOT MITOTIC ANNOTATIONS")

    mitoticdata = pd.DataFrame(mitoticdata)
    mitoticbooldata = mitoticdata[mitoticdata["MitoticStateId/Name"] == "Mitosis"]
    mitoticstatedata = mitoticdata[mitoticdata["MitoticStateId/Name"] != "Mitosis"]
    mitoticbooldata = mitoticbooldata.rename(
        columns={"MitoticStateId/Name": "IsMitotic"}
    )
    mitoticstatedata = mitoticstatedata.rename(
        columns={"MitoticStateId/Name": "MitoticState"}
    )
    df_data_handoff = pd.merge(
        df_data_handoff,
        mitoticbooldata,
        how="left",
        left_on="CellId",
        right_on="CellId",
    )
    df_data_handoff = pd.merge(
        df_data_handoff,
        mitoticstatedata,
        how="left",
        left_on="CellId",
        right_on="CellId",
    )
    df_data_handoff = df_data_handoff.fillna(
        value={"IsMitotic": "", "MitoticState": ""}
    )

    # get legacy cell name for all cells
    legacycellname_results = lk.select_rows_as_list(
        schema_name="processing",
        query_name="CellAnnotationJunction",
        columns="CellId, Value",
        filter_array=[
            labkey.query.QueryFilter("AnnotationTypeId/Name", "Cell name", "in")
        ],
    )
    print("GOT LEGACY CELL NAMES")
    df_legacycellname = pd.DataFrame(legacycellname_results)
    df_legacycellname = df_legacycellname.rename(columns={"Value": "LegacyCellName"})
    df_data_handoff = pd.merge(
        df_data_handoff,
        df_legacycellname,
        how="left",
        left_on="CellId",
        right_on="CellId",
    )

    # get legacy fov name for all fovs.
    fovannotation_results = lk.select_rows_as_list(
        schema_name="processing",
        query_name="FOVAnnotationJunction",
        columns="FOVId, AnnotationTypeId/Name, Value",
        filter_array=[
            labkey.query.QueryFilter("AnnotationTypeId/Name", "FOV name", "in")
        ],
    )
    print("GOT LEGACY FOV NAMES")
    df_fovlegacyname = pd.DataFrame(fovannotation_results)
    df_fovlegacyname = df_fovlegacyname.rename(columns={"Value": "LegacyFOVName"})[
        ["FOVId", "LegacyFOVName"]
    ]
    # allow for multiple possible legacy names for a fov
    df_fovlegacyname = (
        df_fovlegacyname.groupby(["FOVId"])["LegacyFOVName"].apply(list).reset_index()
    )

    df_data_handoff = pd.merge(
        df_data_handoff, df_fovlegacyname, how="left", left_on="FOVId", right_on="FOVId"
    )
    # at this time since there have been duplicate legacy FOVs, let's eliminate them.
    print("Removing duplicate legacy fov names...")
    check_dups(df_data_handoff, "CellId")

    # get the aligned mitotic cell data
    imsc_dataset = Package.browse(
        "aics/imsc_align_cells", "s3://allencell-internal-quilt"
    )
    dataset = imsc_dataset["dataset.csv"]()
    print("GOT INTEGRATED MITOTIC DATA SET")

    # assert all the angles and translations are valid production cells
    # matches = dataset["CellId"].isin(df_data_handoff["CellId"])
    # assert(matches.all())

    df_data_handoff = pd.merge(
        df_data_handoff,
        dataset[["CellId", "Angle", "x", "y"]],
        left_on="CellId",
        right_on="CellId",
        how="left",
    )

    # put cell fov name in a new column:
    df_data_handoff["FOV_3dcv_Name"] = df_data_handoff.apply(
        lambda row: get_fov_name_from_row(row), axis=1
    )

    # deal with nans
    df_data_handoff = df_data_handoff.fillna(
        value={"LegacyCellName": "", "Angle": 0, "x": 0, "y": 0}
    )
    df_data_handoff["LegacyFOVName"] = df_data_handoff["LegacyFOVName"].apply(
        lambda d: d if isinstance(d, list) else []
    )

    # replace any remaining NaNs with None
    df_data_handoff = df_data_handoff.where((pd.notnull(df_data_handoff)), None)

    check_dups(df_data_handoff, "CellId")

    print("DONE BUILDING TABLES")

    print(list(df_data_handoff.columns))

    # verify the expected column names in the above query
    for field in dataset_constants.AugmentedDataField:
        if field.value not in df_data_handoff.columns:
            raise f"Expected {field.value} to be in combined dataset results."

    return df_data_handoff


if __name__ == "__main__":
    print(sys.argv)
    collect_data_rows()
    sys.exit(0)
