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


def read_excel(inputfilename):
    df1 = pd.ExcelFile(inputfilename)
    # only use first sheet.
    x = df1.sheet_names[0]
    df2 = pd.read_excel(inputfilename, sheetname=x, encoding='iso-8859-1')
    return df2.to_dict(orient='records')
    # else:
    #     dicts = []
    #     for x in df1.sheet_names:
    #         # out to csv
    #         df2 = pd.read_excel(inputfilename, sheetname=x, encoding='iso-8859-1')
    #         dicts.append(df2.to_dict(orient='records'))
    #     return dicts


def read_csv(inputfilename):
    df = pd.read_csv(inputfilename, comment='#')
    return df.to_dict(orient='records')
    # with open(csvfilename, 'rU') as csvfile:
    #     reader = csv.DictReader(csvfile)
    #     return [row for row in reader]


def get_rows(inputfilename):
    if inputfilename.endswith('.xlsx'):
        rows = read_excel(inputfilename)
    elif inputfilename.endswith('.csv'):
        rows = read_csv(inputfilename)
    else:
        rows = []
    return rows


def collect_files(file_or_dir):
    # collect up the files to process
    files = []
    if os.path.isfile(file_or_dir):
        files.append(file_or_dir)
    else:
        for workingFile in os.listdir(file_or_dir):
            if workingFile.endswith('.csv') and not workingFile.startswith('~'):
                fp = os.path.join(file_or_dir, workingFile)
                if os.path.isfile(fp):
                    files.append(fp)
    return files


def trim_labkeyurl(rows):
    df = pd.DataFrame(rows)
    cols = [c for c in df.columns if c.lower()[:11] != '_labkeyurl_']
    df = df[cols]
    return df


def get_read_path(fileid, basepath):
    return '%s/%s/%s' % (basepath, fileid[-2:], fileid)


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
        print(repeats)
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

    # get cell ids and their fovs
    # which fovs are true production data?
    # // PlateId.PlateType must be "Production - Imaging"
    # // ^^^^^ most important
    # // FOV.Objective == 100
    cells_passing_segmentation_qc_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='processing',
        query_name='CellsPassingSegmentationQC',
        columns='CellId, CellId/FOVId, CellId/CellIndex',
        filter_array=[
            # labkey.query.QueryFilter('CellId/FOVId/ROIId/WellId/PlateId/PlateTypeId/Name', 'Production - Imaging', 'eq'),
            labkey.query.QueryFilter('CellId/FOVId/Objective', '100.0', 'eq')
        ],
        max_rows=-1
    )
    df_cells_passing_segmentation_qc = trim_labkeyurl(cells_passing_segmentation_qc_results['rows'])
    print("GOT CELLS")

    # the unique FOVs of these cells.
    fov_list = df_cells_passing_segmentation_qc['CellId/FOVId'].unique()
    df_fovs = pd.DataFrame(fov_list, columns=['FOVId'])
    print("GOT FOVS of CELLS")

    #df_cells_per_fov = df_cells_passing_segmentation_qc.groupby('CellId/FOVId').apply(list)  # ['CellId'].apply(list)
    #assert(df_cells_per_fov.size == df_fovs.size)

    # start getting file readpaths.

    # all files for a fov
    file_fov_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='microscopy',
        query_name='FileFOV',
        view_name='FileFOV+basepath',
        columns='FileId, FOVId, FileId/Filename, FileId/CellLineId/Name, FileId/FileReplica/BasePath',
        max_rows=-1
    )
    df_file_fov = trim_labkeyurl(file_fov_results['rows'])
    # filter away down to the FOVs in df_fovs.
    # df_file_fov = df_file_fov[(df_file_fov['FOVId'].isin(df_fovs['FOVId']))]
    print("GOT FILEIDS FOR FOVS")

    # all files content types
    content_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='processing',
        query_name='Content',
        columns='FileId, ContentTypeId/Name, ChannelNumber, ContentGenerationAlgorithmId, ContentGenerationAlgorithmId/Name',
        max_rows=-1
        # filter_array=[
        #     labkey.query.QueryFilter('ContentTypeId/Name', 'Raw 405nm;Raw 488nm;Raw 561nm;Raw 638nm;Raw brightfield', 'notin')
        # ],
        #sort='FileId/FileId'
    )
    df_content = trim_labkeyurl(content_results['rows'])
    # filter away down to the FOVs in df_fovs.
    df_content = df_content[(df_content['FileId'].isin(df_file_fov['FileId']))]
    # ContentTypeId, ChannelNumber, FileId
    print("GOT CONTENTS OF FILEIDS")

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
    df_fovlegacyname = df_fovlegacyname.rename(columns={"Value": "FOV name"})[['FOVId', 'FOV name']]

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
    # AnnotationTypeId: "FOV name" Value gives legacy cell name.

    # CellPopulationId/CellLineId/Name
    file_cell_population_results = labkey.query.select_rows(
        server_context=server_context,
        schema_name='celllines',
        query_name='FileCellPopulation',
        view_name='FileCellPopulation+',
        columns='CellPopulationId/CellLineId/Name, FileId',
        max_rows=-1
    )
    df_file_cell_population = trim_labkeyurl(file_cell_population_results['rows'])
    print("GOT CELL POPULATION INFO")

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

    cell_ids = pd.DataFrame(df_cells_passing_segmentation_qc.groupby("CellId/FOVId")['CellId'].apply(list))
    cell_idx = pd.DataFrame(df_cells_passing_segmentation_qc.groupby("CellId/FOVId")['CellId/CellIndex'].apply(list))

    fov_cell = pd.merge(df_fovs, cell_ids, how='left', left_on='FOVId', right_on='CellId/FOVId')
    fov_cell = pd.merge(fov_cell, cell_idx, how='left', left_on='FOVId', right_on='CellId/FOVId')

    fov_cell = pd.merge(fov_cell, df_fovcolonyposition, on='FOVId', how='left')

    ################## FIX THIS ################################
    check_dups(df_fovlegacyname, "FOVId")
    fov_cell = pd.merge(fov_cell, df_fovlegacyname, on='FOVId', how='left')
    ##########################

    ### left or not?
    df_content_merged = pd.merge(df_content, df_file_fov, on='FileId', how='left')

    # x[0] is basepath. x[1] is fileid
    # make_read_path = lambda x: '%s/%s/%s' % (x[0], (x[1])[-2:], x[1])

    gb_groupedcontent = df_content_merged.groupby("ContentTypeId/Name")
    df_nucleuschannelfiles = gb_groupedcontent.get_group("Raw 405nm")
    df_nucleuschannelfiles = df_nucleuschannelfiles.rename(columns={"ChannelNumber": "NucleusChannel"})

    # construct file read path into table.
    # df_nucleuschannelfiles['ReadPath'] = df_nucleuschannelfiles[['FileId/FileReplica/BasePath', 'FileId']].apply(make_read_path, axis=1)

    df_membranechannelfiles = gb_groupedcontent.get_group("Raw 638nm")
    df_membranechannelfiles = df_membranechannelfiles.rename(columns={"ChannelNumber": "MembraneChannel"})
    df_membranechannelfiles.drop(['ContentGenerationAlgorithmId'], axis=1)
    df_brightfieldchannelfiles = gb_groupedcontent.get_group("Raw brightfield")
    df_brightfieldchannelfiles = df_brightfieldchannelfiles.rename(columns={"ChannelNumber": "BrightfieldChannel"})
    df_brightfieldchannelfiles.drop(['ContentGenerationAlgorithmId'], axis=1)
    df_structurechannelfiles = gb_groupedcontent.get_group("Raw 488nm")
    df_structurechannelfiles = df_structurechannelfiles.rename(columns={"ChannelNumber": "StructureChannel"})
    df_structurechannelfiles.drop(['ContentGenerationAlgorithmId'], axis=1)

    df_structuresegfiles = gb_groupedcontent.get_group("Structure segmentation")[["FOVId", "FileId", "FileId/FileReplica/BasePath", "ContentGenerationAlgorithmId"]]
    df_structuresegfiles = df_structuresegfiles.rename(columns={"FileId": "StructureSegFileId", "FileId/FileReplica/BasePath": "StructureSegBasePath", "ContentGenerationAlgorithmId": "StructureSegmentationAlgorithm"})
    df_nucleussegfiles = gb_groupedcontent.get_group("Nucleus segmentation")[["FOVId", "FileId", "FileId/FileReplica/BasePath", "ContentGenerationAlgorithmId"]]
    df_nucleussegfiles = df_nucleussegfiles.rename(columns={"FileId": "NucleusSegFileId", "FileId/FileReplica/BasePath": "NucleusSegBasePath", "ContentGenerationAlgorithmId": "CellSegmentationAlgorithm"})
    df_membranesegfiles = gb_groupedcontent.get_group("Membrane segmentation")[["FOVId", "FileId", "FileId/FileReplica/BasePath"]]
    df_membranesegfiles = df_membranesegfiles.rename(columns={"FileId": "MembraneSegFileId", "FileId/FileReplica/BasePath": "MembraneSegBasePath"})
    df_nucleuscontourfiles = gb_groupedcontent.get_group("Nucleus contour")[["FOVId", "FileId", "FileId/FileReplica/BasePath"]]
    df_nucleuscontourfiles = df_nucleuscontourfiles.rename(columns={"FileId": "NucleusContourFileId", "FileId/FileReplica/BasePath": "NucleusContourBasePath"})
    df_membranecontourfiles = gb_groupedcontent.get_group("Membrane contour")[["FOVId", "FileId", "FileId/FileReplica/BasePath"]]
    df_membranecontourfiles = df_membranecontourfiles.rename(columns={"FileId": "MembraneContourFileId", "FileId/FileReplica/BasePath": "MembraneContourBasePath"})

    ##### FIX REDUNDANT ROWS FROM ONE OF THESE MERGES

    fov_cell = pd.merge(fov_cell, df_nucleuschannelfiles[["FOVId", "NucleusChannel", "FileId/Filename", "FileId/FileReplica/BasePath", "FileId/CellLineId/Name", "FileId"]], on="FOVId")
    check_dups(fov_cell, "FOVId")

    fov_cell = pd.merge(fov_cell, df_membranechannelfiles[["FOVId", "MembraneChannel"]], on="FOVId")
    check_dups(fov_cell, "FOVId")

    fov_cell = pd.merge(fov_cell, df_brightfieldchannelfiles[["FOVId", "BrightfieldChannel"]], on="FOVId")
    check_dups(fov_cell, "FOVId")

    fov_cell = pd.merge(fov_cell, df_structurechannelfiles[["FOVId", "StructureChannel"]], on="FOVId")
    check_dups(fov_cell, "FOVId")

    fov_cell = pd.merge(fov_cell, df_structuresegfiles, on="FOVId")
    fov_cell = pd.merge(fov_cell, df_nucleussegfiles, on="FOVId")
    fov_cell = pd.merge(fov_cell, df_membranesegfiles, on="FOVId")
    fov_cell = pd.merge(fov_cell, df_nucleuscontourfiles, on="FOVId")
    fov_cell = pd.merge(fov_cell, df_membranecontourfiles, on="FOVId")

    check_dups(fov_cell, "FOVId")

    return fov_cell


if __name__ == "__main__":
    print (sys.argv)
    collect_data_rows()
    sys.exit(0)


