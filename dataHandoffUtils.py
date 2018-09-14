import labkey
import numpy as np
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
    cols = [c for c in df.columns if c.lower()[:11] != '_labkeyurl_']
    df = df[cols]
    return df


def get_read_path(fileid, basepath):
    return '%s/%s/%s' % (basepath, fileid[-2:], fileid)


# cellline must be 'AICS-#'
def get_cell_name(fovid, cellline, cellid):
    return str(cellline) + "_" + str(fovid) + "_" + str(cellid)


def get_cellline_name_from_row(row, df_celllines):
    return str(df_celllines.loc[row["CellLineId"]]["CellLineId/Name"])


def get_fov_name_from_row(row, df_celllines):
    celllinename = df_celllines.loc[row["CellLineId"]]["CellLineId/Name"]
    # celllinename = df_celllines.loc[df_celllines["CellLineId"] == row["CellLineId"]]["CellLineId/Name"]
    return str(celllinename) + "_" + str(row["FOVId"])


# cellline must be 'AICS-#'
def get_fov_name(fovid, cellline):
    return str(cellline) + "_" + str(fovid)


def get_name_id_and_readpath(content_file_row, df_filefov):
    fileid = content_file_row['FileId'].iloc[0]
    filefovrow = df_filefov[df_filefov["FileId"] == fileid]
    filename = filefovrow['FileId/Filename'].iloc[0]
    filereadpath = get_read_path(fileid, (filefovrow['FileId/FileReplica/BasePath'].iloc[0])[0])
    return filename, fileid, filereadpath


def check_dups(dfr, column, remove=True):
    dupes = dfr.duplicated(column)
    repeats = (dfr[dupes])[column].unique()
    if len(repeats) > 0:
        print("FOUND DUPLICATE DATA FOR THESE " + column + " KEYS:")
        print(*repeats, sep=' ')
        # print(repeats)
    if remove:
        dfr.drop_duplicates(subset=column, keep="first", inplace=True)


def collect_data_rows():
    # call on labkey to give the set of publicly releaseable cell fovs.
    # get ome.tif image, cell and nuc segs, cell and nuc contour segs, and structure seg
    # get indices (and ids) of approved segmented cells

    # // FileFOV
    # // Content
    # // CellsPassingSegmentationQC
    # // FileCellPopulation , the plus view
    #
    # // PlateId.PlateType must be "Production - Imaging"
    # // ^^^^^ most important
    # // FOV.Objective == 100

    server_context = labkey.utils.create_server_context('aics.corp.alleninstitute.org', 'AICS', 'labkey', use_ssl=False)

    # my_results = labkey.query.select_rows(
    #     server_context=server_context,
    #     schema_name='processing',
    #     query_name='Pipeline 4 Production Data Passing QC'
    # )
    # df_allresults = trim_labkeyurl(my_results['rows'])

    # {'_labkeyurl_CellId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=processing&query.queryName=Cell&CellId=1', 'StructureSegmentationFileId': '561fc8a2ba574c33b3e3990b7442738a', 'StructureSegmentationAlgorithmVersion': '1.0.1', '_labkeyurl_SourceFileId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=fms&query.queryName=File&FileId=6a23a87b0f4c467bb6c2380a00cf7a30', 'NucleusSegmentationFilename': '3500001454_100X_20171023_1-Scene-01-P1-E04.czi_nucWholeIndexImageScale.tiff', 'NucMembSegmentationAlgorithm': 38, 'StructureContourFilename': '3500001454_100X_20171023_1-Scene-01-P1-E04_structImageScaleContourXY.tiff', 'CellIndex': 1, 'NucleusSegmentationFileId': 'fba6c129a33848bcb6e9c30a698e6f53', 'MembraneContourFilename': '3500001454_100X_20171023_1-Scene-01-P1-E04_cellImageScaleContourXY.tiff', 'NucleusContourFileId': 'c6645f26f9654b1880bb7e10daacb4f1', 'NucMembSegmentationAlgorithmVersion': '1.3.0', '_labkeyurl_NucleusSegmentationFileId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=fms&query.queryName=File&FileId=fba6c129a33848bcb6e9c30a698e6f53', '_labkeyurl_StructureSegmentationFileId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=fms&query.queryName=File&FileId=561fc8a2ba574c33b3e3990b7442738a', 'MembraneContourFileId': '7584d7057fd4412180e4136e590e85d7', '_labkeyurl_MembraneContourFileId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=fms&query.queryName=File&FileId=7584d7057fd4412180e4136e590e85d7', '_labkeyurl_InstrumentId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=microscopy&query.queryName=Instrument&InstrumentId=4', 'MembraneSegmentationFilename': '3500001454_100X_20171023_1-Scene-01-P1-E04.czi_cellWholeIndexImageScale.tiff', 'CellId': 1, 'ChannelNumberStruct': 3, 'Passage': [24], 'NucleusContourReadPath': '//allen/programs/allencell/data/proj0/f1/c6645f26f9654b1880bb7e10daacb4f1', 'InstrumentId': 4, '_labkeyurl_PlateId': '/labkey/aics_microscopy/AICS/editPlate.view?PlateId=175', 'Objective': 100.0, 'StructureSegmentationAlgorithm': 40, 'NucleusContourFilename': '3500001454_100X_20171023_1-Scene-01-P1-E04_nucImageScaleContourXY.tiff', '_labkeyurl_CellPopulationId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=celllines&query.queryName=CellPopulation&CellPopulationId=5353', 'MembraneSegmentationReadPath': '//allen/programs/allencell/data/proj0/0a/dc25b07403174870ad4803596bda1a0a', 'Row': 4, 'CellPopulationId': [5353], 'MembraneSegmentationFileId': 'dc25b07403174870ad4803596bda1a0a', '_labkeyurl_NucMembSegmentationAlgorithm': '/labkey/query/AICS/detailsQueryRow.view?schemaName=processing&query.queryName=ContentGenerationAlgorithm&ContentGenerationAlgorithmId=38', 'MembraneContourReadPath': '//allen/programs/allencell/data/proj0/d7/7584d7057fd4412180e4136e590e85d7', 'Cell name': None, 'StructureSegmentationReadPath': '//allen/programs/allencell/data/proj0/8a/561fc8a2ba574c33b3e3990b7442738a', '_labkeyurl_NucleusContourFileId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=fms&query.queryName=File&FileId=c6645f26f9654b1880bb7e10daacb4f1', 'NucleusSegmentationReadPath': '//allen/programs/allencell/data/proj0/53/fba6c129a33848bcb6e9c30a698e6f53', 'CellLineId': [26], 'StructureContourReadPath': '//allen/programs/allencell/data/proj0/d1/3597a5b3076b470a876de9aa7e2869d1', 'ChannelNumber405': 5, 'SourceFilename': '3500001454_100X_20171023_1-Scene-01-P1-E04.ome.tiff', 'FOVId': 7649, 'StructureContourFileId': '3597a5b3076b470a876de9aa7e2869d1', 'RunId': None, 'Clone': ['37'], 'Col': 3, '_labkeyurl_MembraneSegmentationFileId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=fms&query.queryName=File&FileId=dc25b07403174870ad4803596bda1a0a', '_labkeyurl_StructureSegmentationAlgorithm': '/labkey/query/AICS/detailsQueryRow.view?schemaName=processing&query.queryName=ContentGenerationAlgorithm&ContentGenerationAlgorithmId=40', '_labkeyurl_StructureContourFileId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=fms&query.queryName=File&FileId=3597a5b3076b470a876de9aa7e2869d1', 'SourceFileId': '6a23a87b0f4c467bb6c2380a00cf7a30', 'StructureSegmentationFilename': '3500001454_100X_20171023_1-Scene-01-P1-E04_structImageScaleSegment.tiff', 'ChannelNumber638': 1, 'ChannelNumberBrightfield': 6, '_labkeyurl_FOVId': '/labkey/query/AICS/detailsQueryRow.view?schemaName=microscopy&query.queryName=FOV&FOVId=7649', 'PlateId': 175, '_labkeyurl_RunId': None, 'SourceReadPath': '//allen/programs/allencell/data/proj0/30/6a23a87b0f4c467bb6c2380a00cf7a30'}

    data_handoff_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='processing',
        query_name='Pipeline 4 Handoff 1',
        max_rows=-1
    )
    df_data_handoff = trim_labkeyurl(data_handoff_results['rows'])
    print("GOT DATA HANDOFF")
    data_grouped = df_data_handoff.groupby("FOVId")
    # get cell ids and indices into lists per FOV
    cell_ids = pd.DataFrame(data_grouped['CellId'].apply(list))
    cell_idx = pd.DataFrame(data_grouped['CellIndex'].apply(list))
    # now remove dups in the data_grouped
    df_data_handoff = data_grouped.first().reset_index()
    # df_data_handoff.drop_duplicates(subset='FOVId', keep="first", inplace=True)

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

    cell_line_protein_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='celllines',
        query_name='CellLineDefinition',
        columns='CellLineId,CellLineId/Name,ProteinId/DisplayName,StructureId/Name,GeneId/Name',
        max_rows=-1
    )
    df_cell_line_protein = trim_labkeyurl(cell_line_protein_results['rows'])
    print("GOT CELL LINE INFO")

    # Building tables
    print("BUILDING TABLES")

    df_data_handoff = pd.merge(df_data_handoff, df_fovcolonyposition, on='FOVId', how='left')

    ################## FIX THIS ################################
    check_dups(df_data_handoff, "FOVId")
    df_data_handoff = pd.merge(df_data_handoff, df_fovlegacyname, on='FOVId', how='left')
    ##########################

    check_dups(df_data_handoff, "FOVId")

    # put cell fov name in a new column:
    # print(get_fov_name_from_row(df_data_handoff.iloc[0], df_cell_line_protein))
    df_cell_lines = df_cell_line_protein.set_index('CellLineId')
    df_data_handoff['FOV_3dcv_Name'] = df_data_handoff.apply(lambda row: get_fov_name_from_row(row, df_cell_lines), axis=1)
    df_data_handoff['CellLineName'] = df_data_handoff.apply(lambda row: get_cellline_name_from_row(row, df_cell_lines), axis=1)

    return df_data_handoff


if __name__ == "__main__":
    print(sys.argv)
    collect_data_rows()
    sys.exit(0)


