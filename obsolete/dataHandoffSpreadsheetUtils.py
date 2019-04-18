from cellNameDb import CellNameDatabase
import glob
import os
import pandas as pd
import re
import sys

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


def collect_data_rows(data_glob, save_db=True, db_path='imageIDs.csv'):
    # Get all the .csv files in the data dir
    data_paths = glob.glob(data_glob)

    # cell name listing
    db = CellNameDatabase(db_path)

    # Read every .csv file and concat them together
    data = list()
    for path in data_paths:
        data_tmp = get_rows(path)
        for r in data_tmp:
            r['source_data'] = normalize_path(path)

            # get cell line id by whatever means necessary
            cellLineId = r.get('CellLine')
            if cellLineId is None:
                cellLineId = r.get('cell_line_ID')
            if cellLineId is None:
                cellLineId = r.get('cellLineId')
            r['cell_line_ID'] = cellLineId

            r['cbrCellName'] = db.get_cell_name(r['cell_line_ID'], r['inputFilename'], r['inputFolder'])
            # print(r['cbrCellName'])
        data = data + data_tmp
    if save_db:
        db.writedb()
    return data

