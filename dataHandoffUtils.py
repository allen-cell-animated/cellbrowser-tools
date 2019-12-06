import datasetdatabase as dsdb
import labkey
from lkaccess import LabKey, QueryFilter
import lkaccess.contexts
import logging
import os
import pandas as pd
import re
import sys

logging.basicConfig(level=logging.INFO)


def normalize_path(path):
    # windows: \\\\allen\\aics
    windowsroot = '\\\\allen\\'
    # mac:     /Volumes/aics (???)
    macroot = '/Volumes/'
    # linux:   /allen/aics
    linuxroot = '/allen/'
    linuxroot2 = '//allen/'

    # 1. strip away the root.
    if path.startswith(windowsroot):
        path = path[len(windowsroot):]
    elif path.startswith(linuxroot):
        path = path[len(linuxroot):]
    elif path.startswith(linuxroot2):
        path = path[len(linuxroot2):]
    elif path.startswith(macroot):
        path = path[len(macroot):]
    else:
        # if the path does not reference a known root, don't try to change it.
        # it's probably a local path.
        return path

    # 2. split the path up into a list of dirs
    path_as_list = re.split(r'\\|/', path)

    # 3. insert the proper system root for this platform (without the trailing slash)
    dest_root = ''
    if sys.platform.startswith('darwin'):
        dest_root = macroot[:-1]
    elif sys.platform.startswith('linux'):
        dest_root = linuxroot[:-1]
    else:
        dest_root = windowsroot[:-1]

    path_as_list.insert(0, dest_root)

    out_path = os.path.join(*path_as_list)
    return out_path


def get_cellline_name_from_row(row):
    return row["CellLine"]


# cellline must be 'AICS-#'
def get_fov_name(fovid, cellline):
    return f"{cellline}_{fovid}"


def get_fov_name_from_row(row):
    celllinename = get_cellline_name_from_row(row)
    fovid = row["FOVId"]
    return get_fov_name(fovid, celllinename)


def check_dups(dfr, column, remove=True):
    dupes = dfr.duplicated(column)
    repeats = (dfr[dupes])[column].unique()
    if len(repeats) > 0:
        print("FOUND DUPLICATE DATA FOR THESE " + column + " KEYS:")
        print(*repeats, sep=' ')
        # print(repeats)
    if remove:
        dfr.drop_duplicates(subset=column, keep="first", inplace=True)


# big assumption: any query_name passed in must return data of the same format!
def collect_data_rows(fovids=None, raw_only=False):
    # lk = LabKey(host="aics")
    lk = LabKey(server_context=lkaccess.contexts.PROD)

    print("REQUESTING DATA HANDOFF")
    lkdatarows = lk.dataset.get_pipeline_4_production_data()
    df_data_handoff = pd.DataFrame(lkdatarows)

    if fovids is not None and len(fovids) > 0:
        df_data_handoff = df_data_handoff[df_data_handoff['FOVId'].isin(fovids)]

    print("GOT DATA HANDOFF")

    if raw_only:
        return df_data_handoff

    # get mitotic state name for all cells
    mitoticdata = lk.select_rows_as_list(
        schema_name="processing",
        query_name="MitoticAnnotation",
        sort="MitoticAnnotation",
        # columns=["CellId", "MitoticStateId/Name", "Complete"]
        columns=["CellId", "MitoticStateId/Name"]
    )
    print("GOT MITOTIC ANNOTATIONS")

    mitoticdata = pd.DataFrame(mitoticdata)
    mitoticbooldata = mitoticdata[mitoticdata["MitoticStateId/Name"] == "Mitosis"]
    mitoticstatedata = mitoticdata[mitoticdata["MitoticStateId/Name"] != "Mitosis"]
    mitoticbooldata = mitoticbooldata.rename(columns={"MitoticStateId/Name": "IsMitotic"})
    mitoticstatedata = mitoticstatedata.rename(columns={"MitoticStateId/Name": "MitoticState"})
    df_data_handoff = pd.merge(df_data_handoff, mitoticbooldata, how='left', left_on='CellId', right_on='CellId')
    df_data_handoff = pd.merge(df_data_handoff, mitoticstatedata, how='left', left_on='CellId', right_on='CellId')
    df_data_handoff = df_data_handoff.fillna(value={'IsMitotic': '', 'MitoticState': ''})

    # get legacy cell name for all cells
    legacycellname_results = lk.select_rows_as_list(
        schema_name='processing',
        query_name='CellAnnotationJunction',
        columns='CellId, Value',
        filter_array=[
            labkey.query.QueryFilter('AnnotationTypeId/Name', 'Cell name', 'in')
        ]
    )
    print("GOT LEGACY CELL NAMES")
    df_legacycellname = pd.DataFrame(legacycellname_results)
    df_legacycellname = df_legacycellname.rename(columns={"Value": "LegacyCellName"})
    df_data_handoff = pd.merge(df_data_handoff, df_legacycellname, how='left', left_on='CellId', right_on='CellId')

    # get legacy fov name for all fovs.
    fovannotation_results = lk.select_rows_as_list(schema_name='processing',
                                                   query_name='FOVAnnotationJunction',
                                                   columns='FOVId, AnnotationTypeId/Name, Value',
                                                   filter_array=[
                                                       labkey.query.QueryFilter('AnnotationTypeId/Name', 'FOV name', 'in')
                                                   ])
    print("GOT LEGACY FOV NAMES")
    df_fovlegacyname = pd.DataFrame(fovannotation_results)
    df_fovlegacyname = df_fovlegacyname.rename(columns={"Value": "LegacyFOVName"})[['FOVId', 'LegacyFOVName']]
    # allow for multiple possible legacy names for a fov
    df_fovlegacyname = df_fovlegacyname.groupby(['FOVId'])['LegacyFOVName'].apply(list).reset_index()

    df_data_handoff = pd.merge(df_data_handoff, df_fovlegacyname, how='left', left_on='FOVId', right_on='FOVId')
    # at this time since there have been duplicate legacy FOVs, let's eliminate them.
    print('Removing duplicate legacy fov names...')
    check_dups(df_data_handoff, "CellId")

    # get the aligned mitotic cell data
    prod = dsdb.DatasetDatabase(config='//allen/aics/animated-cell/Dan/dsdb/prod.json')
    dataset = prod.get_dataset(name='april-2019-prod-cells')
    print("GOT INTEGRATED MITOTIC DATA SET")

    # assert all the angles and translations are valid production cells
    matches = (dataset.ds['CellId'].isin(df_data_handoff['CellId']))
    # assert(matches.all())
    df_data_handoff = pd.merge(df_data_handoff, dataset.ds[['CellId', 'Angle', 'x', 'y']], left_on='CellId', right_on='CellId', how='left')

    cell_line_protein_results = lk.select_rows_as_list(schema_name='celllines',
                                                       query_name='CellLineDefinition',
                                                       columns='CellLineId,CellLineId/Name,ProteinId/DisplayName,StructureId/Name,GeneId/Name'
                                                       )
    print("GOT CELL LINE DATA")

    df_cell_line_protein = pd.DataFrame(cell_line_protein_results)
    df_cell_lines = df_cell_line_protein.set_index('CellLineId')

    # put cell fov name in a new column:
    df_data_handoff['FOV_3dcv_Name'] = df_data_handoff.apply(lambda row: get_fov_name_from_row(row), axis=1)

    # deal with nans
    df_data_handoff = df_data_handoff.fillna(value={'LegacyCellName': '', 'Angle': 0, 'x': 0, 'y': 0})
    df_data_handoff['LegacyFOVName'] = df_data_handoff['LegacyFOVName'].apply(lambda d: d if isinstance(d, list) else [])

    # replace any remaining NaNs with None
    df_data_handoff = df_data_handoff.where((pd.notnull(df_data_handoff)), None)

    check_dups(df_data_handoff, "CellId")

    print("DONE BUILDING TABLES")
    return df_data_handoff


if __name__ == "__main__":
    print(sys.argv)
    collect_data_rows()
    sys.exit(0)


