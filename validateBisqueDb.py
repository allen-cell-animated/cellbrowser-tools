#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import csv
import glob
import jobScheduler
import json
import os
import pandas as pd
import platform
import re
import sys
import uploader.db_api as db_api

from cellNameDb import CellNameDatabase
from processImageWithSegmentation import do_main_image

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)


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

    # 1. strip away the root.
    if path.startswith(windowsroot):
        path = path[len(windowsroot):]
    elif path.startswith(linuxroot):
        path = path[len(linuxroot):]
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


def validate(batchname, jobname, info, sheet_row):
    imageName = info.cbrCellName
    segs = sheet_row["outputCellSegIndex"]
    segs = segs.split(";")
    # get rid of empty strings in segs
    segs = list(filter(None, segs))

    names = [imageName]
    for seg in segs:
        # str(int(seg)) removes leading zeros
        names.append(imageName + "_" + str(int(seg)))

    exts = ['.ome.tif', '.png']
    # check existence of ome.tif and png.

    data_dir = '\\\\allen\\aics\\animated-cell\\Allen-Cell-Explorer\\Allen-Cell-Explorer_1.1.0\\Cell-Viewer_Data'
    thumbs_dir = '\\\\allen\\aics\\animated-cell\\Allen-Cell-Explorer\\Allen-Cell-Explorer_1.1.0\\Cell-Viewer_Thumbnails'
    # assume that the file location has same name as this subdir name of where the spreadsheet lives:
    data_subdir = batchname.split('\\')[-3]
    # data_subdir = '2017_03_08_Struct_First_Pass_Seg'
    cell_line = 'AICS-' + str(sheet_row["CellLine"])
    for f in names:
        # check for thumbnail
        fullf = os.path.join(thumbs_dir, data_subdir, cell_line, f + '.png')
        if not os.path.isfile(fullf):
            print(batchname + ": Could not find file: " + fullf)

        # check for image
        fullf = os.path.join(data_dir, data_subdir, cell_line, f + '.ome.tif')
        if not os.path.isfile(fullf):
            print(batchname + ": Could not find file: " + fullf)

        # see if image is in bisque db.
        session_dict = {
            # 'root': 'http://dev-aics-dtp-001',
            'root': 'http://10.128.62.98',
            'user': 'admin',
            'password': 'admin'
        }
        db_api.DbApi.setSessionInfo(session_dict)
        xml = db_api.DbApi.getImagesByName(f)
        if len(xml.getchildren()) != 1:
            print('Retrieved ' + str(len(xml.getchildren())) + ' images with name ' + f)
        if len(xml.getchildren()) > 1:
            dbnames = []
            for i in xml:
                imname = i.get("name")
                if imname in dbnames:
                    imid = i.get("resource_uniq")
                    print("  Deleting: " + imid)
                    db_api.DbApi.deleteImage(imid)
                else:
                    dbnames.append(imname)

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


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py -c -n --dataset 2017_03_08_Struct_First_Pass_Seg')

    # python validateDataHandoff --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY

    parser.add_argument('input', nargs='?', default='delivery_summary.csv', help='input csv files')

    # sheets replaces input...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    args = parser.parse_args()

    return args


def do_image_list(args, inputfilename, db, skip_structure_segmentation=False):
    # get the "current" max ids from this database.
    id_authority = db

    if inputfilename.endswith('.xlsx'):
        rows = read_excel(inputfilename)
    elif inputfilename.endswith('.csv'):
        rows = read_csv(inputfilename)
    else:
        return 0

    count = 0
    print("# Validating " + inputfilename)
    for row in rows:
        # print("Processing Row " + str(count) + " in " + inputfilename)
        info = cellJob.CellJob(row)
        info.cbrAddToDb = True

        aicscelllineid = info.cellLineId
        subdir = 'AICS-' + str(aicscelllineid)

        # does this cell already have a number?
        info.cbrCellName = id_authority.get_cell_name(aicscelllineid, info.inputFilename, info.inputFolder)

        jobname = info.cbrCellName

        validate(inputfilename, jobname, info, row)

        count += 1

    return count  # len(rows)


def do_main(args):
    if platform.system() == 'Windows':
        filenames = []
        for filename in args.input:
            if '*' in filename or '?' in filename or '[' in filename:
                filenames += glob.glob(filename)
            else:
                filenames.append(filename)
        args.input = filenames
    input_files = args.input

    jobcounter = 0
    db = CellNameDatabase()

    # collect up the files to process
    files = []
    if os.path.isfile(args.sheets):
        files.append(args.sheets)
    else:
        for workingFile in os.listdir(args.sheets):
            if (workingFile.endswith('.xlsx') or workingFile.endswith('.csv')) and not workingFile.startswith('~'):
                fp = os.path.join(args.sheets, workingFile)
                if os.path.isfile(fp):
                    files.append(fp)

    # process each file
    for fp in files:
        jbs = do_image_list(args, fp, db, False)
        jobcounter += jbs

    # nothing should have changed, but just in case.
    db.writedb()


def main():
    args = parse_args()
    do_main(args)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
