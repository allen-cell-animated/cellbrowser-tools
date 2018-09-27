import labkey
import os
import pandas as pd
import re
import sys


def load_cell_line_info():
    server_context = labkey.utils.create_server_context('aics.corp.alleninstitute.org', 'AICS', 'labkey', use_ssl=False)
    my_results = labkey.query.select_rows(
        columns='CellLineId/Name,ProteinId/DisplayName,StructureId/Name,GeneId/Name',
        server_context=server_context,
        schema_name='celllines',
        query_name='CellLineDefinition'
    )
    # organize into dictionary by cell line
    my_results = {
        d["CellLineId/Name"]: {
            "ProteinName":d["ProteinId/DisplayName"],
            "StructureName":d["StructureId/Name"],
            "GeneName":d["GeneId/Name"]
        } for d in my_results['rows']
    }
    return my_results


def normalize_path(path):
    # legacy: windows paths that start with \\aibsdata
    path = path.replace("\\\\aibsdata\\aics\\AssayDevelopment", "\\\\allen\\aics\\assay-dev")
    path = path.replace("\\\\aibsdata\\aics\\Microscopy", "\\\\allen\\aics\\microscopy")

    # windows: \\\\allen\\aics
    windowsroot = '\\\\allen\\aics\\'
    # mac:     /Volumes/aics (???)
    macroot = '/Volumes/aics/'
    # linux:   /allen/aics
    linuxroot = '/allen/aics/'
    linuxroot2 = '//allen/aics/'

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


def trim_labkeyurl(rows):
    df = pd.DataFrame(rows)
    cols = [c for c in df.columns if not c.startswith('_labkeyurl_')]
    df = df[cols]
    return df


def get_read_path(fileid, basepath):
    return '%s/%s/%s' % (basepath, fileid[-2:], fileid)


# cellline must be 'AICS-#'
def get_cell_name(fovid, cellline, cellid):
    return f"{cellline}_{fovid}_{cellid}"


def get_cellline_name_from_row(row, df_celllines):
    return str(df_celllines.loc[row["CellLineId"]]["CellLineId/Name"])


# cellline must be 'AICS-#'
def get_fov_name(fovid, cellline):
    return f"{cellline}_{fovid}"


def get_fov_name_from_row(row, df_celllines):
    celllinename = get_cellline_name_from_row(row, df_celllines)
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
def collect_data_rows(query_name, fovids=None):
    server_context = labkey.utils.create_server_context('aics.corp.alleninstitute.org', 'AICS', 'labkey', use_ssl=False)

    data_handoff_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='processing',
        query_name=query_name,
        max_rows=-1
    )
    df_data_handoff = trim_labkeyurl(data_handoff_results['rows'])

    if fovids is not None and len(fovids) > 0:
        df_data_handoff = df_data_handoff[df_data_handoff['FOVId'].isin(fovids)]

    print("GOT DATA HANDOFF")

    data_grouped = df_data_handoff.groupby("FOVId")
    # get cell ids and indices into lists per FOV
    cell_ids = pd.DataFrame(data_grouped['CellId'].apply(list))
    cell_idx = pd.DataFrame(data_grouped['CellIndex'].apply(list))
    # now remove dups in the data_grouped
    df_data_handoff = data_grouped.first().reset_index()

    df_data_handoff = df_data_handoff.drop(columns=["CellId", "CellIndex"])
    df_data_handoff = pd.merge(df_data_handoff, cell_ids, how='left', left_on='FOVId', right_on='FOVId')
    df_data_handoff = pd.merge(df_data_handoff, cell_idx, how='left', left_on='FOVId', right_on='FOVId')

    # get colony position and legacy fov name for all fovs.
    fovannotation_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='processing',
        query_name='FOVAnnotationJunction',
        columns='FOVId, AnnotationTypeId/Name, Value',
        filter_array=[
            labkey.query.QueryFilter('AnnotationTypeId/Name', 'Colony position;FOV name', 'in')
        ],
        max_rows=-1
    )
    df_fovannotation = trim_labkeyurl(fovannotation_results['rows'])
    grouped_fovannotation = df_fovannotation.groupby("AnnotationTypeId/Name")
    df_fovcolonyposition = grouped_fovannotation.get_group("Colony position")
    df_fovcolonyposition = df_fovcolonyposition.rename(columns={"Value": "Colony position"})[['FOVId', 'Colony position']]
    df_fovlegacyname = grouped_fovannotation.get_group("FOV name")
    df_fovlegacyname = df_fovlegacyname.rename(columns={"Value": "LegacyFOVName"})[['FOVId', 'LegacyFOVName']]
    # allow for multiple possible legacy names for a fov
    df_fovlegacyname = df_fovlegacyname.groupby(['FOVId'])['LegacyFOVName'].apply(list).reset_index()

    # get mitotic stage and legacy cell name for all cells
    cellannotation_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='processing',
        query_name='CellAnnotationJunction',
        columns='CellId, AnnotationTypeId/Name, Value',
        filter_array=[
            labkey.query.QueryFilter('AnnotationTypeId/Name', 'Mitotic;Mitotic stage;Cell name', 'in')
        ],
        max_rows=-1
    )
    df_cellannotation = trim_labkeyurl(cellannotation_results['rows'])
    grouped_cellannotation = df_cellannotation.groupby("AnnotationTypeId/Name")
    df_legacycellname = grouped_cellannotation.get_group("Cell name")
    df_legacycellname = df_legacycellname.rename(columns={"Value": "LegacyCellName"})[['CellId', 'LegacyCellName']]

    # need to gather these into lists alongside the per-fov cell lists.
    # is there a more efficient way to do this?
    def find_legacy_cell_names(x):
        ret = []
        for cid in x:
            cellnames = df_legacycellname.loc[df_legacycellname['CellId'] == cid]['LegacyCellName']
            if cellnames.size > 0:
                ret.append(cellnames.tolist())
            else:
                ret.append(None)
        return ret
    df_data_handoff['LegacyCellName'] = df_data_handoff['CellId'].apply(lambda x: find_legacy_cell_names(x))


    df_data_handoff = pd.merge(df_data_handoff, df_fovcolonyposition, on='FOVId', how='left')

    check_dups(df_data_handoff, "FOVId")
    df_data_handoff = pd.merge(df_data_handoff, df_fovlegacyname, on='FOVId', how='left')
    # replace NaNs with None
    df_data_handoff = df_data_handoff.where((pd.notnull(df_data_handoff)), None)

    check_dups(df_data_handoff, "FOVId")

    cell_line_protein_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='celllines',
        query_name='CellLineDefinition',
        columns='CellLineId,CellLineId/Name,ProteinId/DisplayName,StructureId/Name,GeneId/Name',
        max_rows=-1
    )
    df_cell_line_protein = trim_labkeyurl(cell_line_protein_results['rows'])
    df_cell_lines = df_cell_line_protein.set_index('CellLineId')
    # put cell fov name in a new column:
    df_data_handoff['FOV_3dcv_Name'] = df_data_handoff.apply(lambda row: get_fov_name_from_row(row, df_cell_lines), axis=1)
    df_data_handoff['CellLineName'] = df_data_handoff.apply(lambda row: get_cellline_name_from_row(row, df_cell_lines), axis=1)
    print("DONE BUILDING TABLES")

    return df_data_handoff


if __name__ == "__main__":
    print(sys.argv)
    collect_data_rows("Pipeline 4 Handoff 1")
    sys.exit(0)


