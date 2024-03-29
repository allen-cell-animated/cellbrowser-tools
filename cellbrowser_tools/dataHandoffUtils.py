from . import dataset_constants
from .dataset_constants import (
    DataField,
    SLURM_SCRIPTS_DIR,
    SLURM_OUTPUT_DIR,
    SLURM_ERROR_DIR,
    DATA_LOG_NAME,
)
import json

# from lkaccess import LabKey
# import lkaccess.contexts
import logging
import os
import pandas as pd

# from quilt3 import Package
import re
import sys

from datetime import datetime
from typing import List

log = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)


class ActionOptions:
    """
    Options for set of files to generate
    """

    def __init__(
        self, do_thumbnails: bool = True, do_atlases: bool = True, do_crop: bool = True
    ):
        self.do_thumbnails = do_thumbnails
        self.do_atlases = do_atlases
        self.do_crop = do_crop


class QueryOptions:
    """
    Filters / options for the querying of images in a data input manifest csv
    """

    def __init__(
        self,
        cell_lines: List[str] = None,
        plates: List[str] = None,
        fovids: List[int] = None,
        start_date: str = None,
        end_date: str = None,
        first_n: int = 0,
    ):
        self.cell_lines = cell_lines
        self.plates = plates
        self.fovids = fovids
        self.first_n = first_n

        self._ensureValidDateString(start_date)
        self._ensureValidDateString(end_date)
        self.start_date = start_date
        self.end_date = end_date

    def _ensureValidDateString(self, date: str):
        if date is None:
            return

        format = "%Y-%m-%d"  # YYYY-MM-DD
        try:
            datetime.strptime(date, format)
        except ValueError:
            # built-in error message isn't specific enough
            raise ValueError("Invalid date format - must be YYYY-MM-DD.")


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
        prefs["out_status"], SLURM_SCRIPTS_DIR, SLURM_OUTPUT_DIR
    )
    os.makedirs(os.path.dirname(prefs["sbatch_output"]), exist_ok=True)

    prefs["sbatch_error"] = os.path.join(
        prefs["out_status"], SLURM_SCRIPTS_DIR, SLURM_ERROR_DIR
    )
    os.makedirs(os.path.dirname(prefs["sbatch_error"]), exist_ok=True)

    # record the location of the data object
    prefs["save_log_path"] = prefs["out_status"] + os.sep + DATA_LOG_NAME

    return prefs


class OutputPaths:
    def __init__(self, out_dir: os.PathLike) -> None:
        self.out_dir = out_dir
        self.status_dir = self._create_dir(
            os.path.join(out_dir, dataset_constants.STATUS_DIR)
        )
        self.images_dir = self._create_dir(
            os.path.join(out_dir, dataset_constants.IMAGES_DIR)
        )
        self.thumbs_dir = self._create_dir(
            os.path.join(out_dir, dataset_constants.THUMBNAILS_DIR)
        )
        self.atlas_dir = self._create_dir(
            os.path.join(out_dir, dataset_constants.ATLAS_DIR)
        )
        self.sbatch_output = self._create_dir(
            os.path.join(self.status_dir, SLURM_SCRIPTS_DIR, SLURM_OUTPUT_DIR)
        )
        self.sbatch_error = self._create_dir(
            os.path.join(self.status_dir, SLURM_SCRIPTS_DIR, SLURM_ERROR_DIR)
        )
        self.save_log_path = os.path.join(self.status_dir, DATA_LOG_NAME)

    def _create_dir(self, d: os.PathLike):
        if d.startswith("s3:"):
            return d
        if not os.path.exists(d):
            os.makedirs(d)
        return d


def get_cellline_name_from_row(row):
    return row[DataField.CellLine]


# cellline must be 'AICS-#'
def get_fov_name(fovid, cellline):
    return f"{fovid}"


def get_cell_name(cellid, fovid, cellline):
    return f"{get_fov_name(fovid, cellline)}_{cellid}"


def get_fov_name_from_row(row):
    celllinename = get_cellline_name_from_row(row)
    # fovid = row[DataField.FOVId]
    fovid = row["SourceFilename"]
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


TEST_DATASET = "//allen/aics/assay-dev/computational/data/dna_cell_seg_on_production_data/production_run_test/mergedataset/manifest.csv"
FULL_DATASET = "//allen/aics/assay-dev/computational/data/dna_cell_seg_on_production_data/production_run/mergedataset/manifest.csv"

FULL_FEATURES_DATA = "//allen/aics/assay-dev/MicroscopyOtherData/Viana/forDan/cfe_table_2020/Production2020.csv"
# FULL_FEATURES_DATA = "//allen/aics/assay-dev/MicroscopyOtherData/Viana/forDan/cfe_table_2020/Production2020_beta.csv"


def collect_csv_data_rows(
    csvpath=FULL_DATASET,
    fovids=None,
    cell_lines=None,
    raw_only=False,
    max_rows=None,
):
    log.info("REQUESTING DATA HANDOFF")
    df_data_handoff = pd.read_csv(csvpath)

    # verify the expected column names in the above query
    # for field in dataset_constants.DataField:
    #     if field.value not in df_data_handoff.columns:
    #         raise ValueError(f"Expected {field.value} to be in labkey dataset results.")

    if fovids is not None and len(fovids) > 0:
        df_data_handoff = df_data_handoff[df_data_handoff["FOVId"].isin(fovids)]
    elif cell_lines is not None and len(cell_lines) > 0:
        df_data_handoff = df_data_handoff[df_data_handoff["CellLine"].isin(cell_lines)]

    if max_rows is not None:
        df_data_handoff = df_data_handoff.head(max_rows)

    log.info("GOT DATA HANDOFF")

    # Merge Aligned and Source read path columns
    if (
        "AlignedImageReadPath" in df_data_handoff.columns
        and "SourceReadPath" not in df_data_handoff.columns
    ):
        df_data_handoff[DataField.SourceReadPath] = df_data_handoff[
            DataField.AlignedImageReadPath
        ].combine_first(df_data_handoff[DataField.SourceReadPath])

    if raw_only:
        return df_data_handoff

    # replace any remaining NaNs with None
    df_data_handoff = df_data_handoff.where((pd.notnull(df_data_handoff)), None)

    # check_dups(df_data_handoff, "CellId")

    print("DONE BUILDING TABLES")

    print(list(df_data_handoff.columns))

    # verify the expected column names in the above query
    expected_columns = []
    # expected_columns = ["MitoticStateId", "Complete"]
    for field in expected_columns:
        if field not in df_data_handoff.columns:
            raise ValueError(f"Expected {field} to be in combined dataset results.")

    # put in string mitotic state names
    def mitotic_id_to_name(row):
        mid = row.MitoticStateId
        if mid == 1:
            return "M6/M7" if row.Complete else "M6/M7 Partial"
        elif mid == 2:
            return "M0"
        elif mid == 3:
            return "M1/M2"
        elif mid == 4:
            return "M3"
        elif mid == 5:
            return "M4/M5"
        elif mid == 6:
            return "Mitosis"
        elif mid is None:
            return ""
        else:
            raise ValueError("Unexpected value for MitoticStateId")

    if "MitoticStateId" in df_data_handoff.columns:
        df_data_handoff["MitoticStateId/Name"] = df_data_handoff.apply(
            lambda row: mitotic_id_to_name(row), axis=1
        )

    log.info("RETURNING COMPLETE DATASET")
    return df_data_handoff


# def collect_data_rows(fovids=None, raw_only=False, max_rows=None):
#     # lk = LabKey(host="aics")
#     lk = LabKey(server_context=lkaccess.contexts.PROD)

#     print("REQUESTING DATA HANDOFF")
#     lkdatarows = lk.dataset.get_pipeline_4_production_data()
#     df_data_handoff = pd.DataFrame(lkdatarows)

#     # verify the expected column names in the above query
#     for field in dataset_constants.DataField:
#         if field.value not in df_data_handoff.columns:
#             raise f"Expected {field.value} to be in labkey dataset results."

#     if fovids is not None and len(fovids) > 0:
#         df_data_handoff = df_data_handoff[df_data_handoff["FOVId"].isin(fovids)]

#     if max_rows is not None:
#         df_data_handoff = df_data_handoff.head(max_rows)

#     print("GOT DATA HANDOFF")

#     # Merge Aligned and Source read path columns
#     df_data_handoff[DataField.SourceReadPath] = df_data_handoff[
#         DataField.AlignedImageReadPath
#     ].combine_first(df_data_handoff[DataField.SourceReadPath])

#     if raw_only:
#         return df_data_handoff

#     # get mitotic state name for all cells
#     mitoticdata = lk.select_rows_as_list(
#         schema_name="processing",
#         query_name="MitoticAnnotation",
#         sort="MitoticAnnotation",
#         # columns=["CellId", "MitoticStateId/Name", "Complete"]
#         columns=["CellId", "MitoticStateId/Name"],
#     )
#     print("GOT MITOTIC ANNOTATIONS")

#     mitoticdata = pd.DataFrame(mitoticdata)
#     mitoticbooldata = mitoticdata[mitoticdata["MitoticStateId/Name"] == "Mitosis"]
#     mitoticstatedata = mitoticdata[mitoticdata["MitoticStateId/Name"] != "Mitosis"]
#     mitoticbooldata = mitoticbooldata.rename(
#         columns={"MitoticStateId/Name": "IsMitotic"}
#     )
#     mitoticstatedata = mitoticstatedata.rename(
#         columns={"MitoticStateId/Name": "MitoticState"}
#     )
#     df_data_handoff = pd.merge(
#         df_data_handoff,
#         mitoticbooldata,
#         how="left",
#         left_on="CellId",
#         right_on="CellId",
#     )
#     df_data_handoff = pd.merge(
#         df_data_handoff,
#         mitoticstatedata,
#         how="left",
#         left_on="CellId",
#         right_on="CellId",
#     )
#     df_data_handoff = df_data_handoff.fillna(
#         value={"IsMitotic": "", "MitoticState": ""}
#     )

#     # get the aligned mitotic cell data
#     imsc_dataset = Package.browse(
#         "aics/imsc_align_cells", "s3://allencell-internal-quilt"
#     )
#     dataset = imsc_dataset["dataset.csv"]()
#     print("GOT INTEGRATED MITOTIC DATA SET")

#     # assert all the angles and translations are valid production cells
#     # matches = dataset["CellId"].isin(df_data_handoff["CellId"])
#     # assert(matches.all())

#     df_data_handoff = pd.merge(
#         df_data_handoff,
#         dataset[["CellId", "Angle", "x", "y"]],
#         left_on="CellId",
#         right_on="CellId",
#         how="left",
#     )

#     # deal with nans
#     df_data_handoff = df_data_handoff.fillna(value={"Angle": 0, "x": 0, "y": 0})

#     # replace any remaining NaNs with None
#     df_data_handoff = df_data_handoff.where((pd.notnull(df_data_handoff)), None)

#     check_dups(df_data_handoff, "CellId")

#     print("DONE BUILDING TABLES")

#     print(list(df_data_handoff.columns))

#     return df_data_handoff


def get_csv_features(path: str = FULL_FEATURES_DATA):
    # replace NaN values with string "NaN"
    df = pd.read_csv(path)
    df.fillna("NaN", inplace=True)
    return df


def cache_dataset(out_dir: os.PathLike, groups):
    with open(
        os.path.join(
            out_dir,
            dataset_constants.STATUS_DIR,
            dataset_constants.DATASET_JSON_FILENAME,
        ),
        "w",
    ) as savefile:
        json.dump(groups, savefile)
    log.info("Saved dataset to json")


def uncache_dataset(out_dir: os.PathLike):
    groups = []
    with open(
        os.path.join(
            out_dir,
            dataset_constants.STATUS_DIR,
            dataset_constants.DATASET_JSON_FILENAME,
        ),
        "r",
    ) as savefile:
        groups = json.load(savefile)
    return groups


def get_data_groups(prefs, n=0):
    data = collect_csv_data_rows(
        fovids=prefs.get("fovs"), cell_lines=prefs.get("cell_lines")
    )
    log.info("Number of total cell rows: " + str(len(data)))
    # group by fov id
    data_grouped = data.groupby("FOVId")
    total_jobs = len(data_grouped)
    log.info("Number of total FOVs: " + str(total_jobs))
    # log.info('ABOUT TO CREATE ' + str(total_jobs) + ' JOBS')
    groups = []
    for index, (fovid, group) in enumerate(data_grouped):
        groups.append(group.to_dict(orient="records"))
        # only the first n FOVs (one group per FOV)
        if n > 0 and index >= n - 1:
            break

    log.info("Converted groups to lists of dicts")

    # make dataset available as a file for later runs
    cache_dataset(prefs.get("out_dir"), groups)

    return groups


def get_data_groups2(
    input_manifest: os.PathLike,
    query_options: QueryOptions,
    out_dir: os.PathLike,
):
    data = collect_csv_data_rows(
        input_manifest, fovids=query_options.fovids, cell_lines=query_options.cell_lines
    )
    log.info("Number of total cell rows: " + str(len(data)))

    # group by fov id
    # data_grouped = data.groupby("FOVId")
    data_grouped = data.groupby("SourceFilename")

    total_jobs = len(data_grouped)
    log.info("Number of total FOVs: " + str(total_jobs))
    # log.info('ABOUT TO CREATE ' + str(total_jobs) + ' JOBS')
    groups = []
    for index, (fovid, group) in enumerate(data_grouped):
        groups.append(group.to_dict(orient="records"))
        # only the first n FOVs (one group per FOV)
        if query_options.first_n > 0 and index >= query_options.first_n - 1:
            break

    log.info("Converted groups to lists of dicts")

    # make dataset available as a file for later runs
    cache_dataset(out_dir, groups)

    return groups


if __name__ == "__main__":
    print(sys.argv)
    # collect_data_rows()
    sys.exit(0)
