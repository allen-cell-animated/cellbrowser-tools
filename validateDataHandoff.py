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


def validate(batchname, jobname, info):
    if not info.cellLineId:
        print(batchname + ": " + jobname + ": Bad CellLine: " + str(info.cellLineId))

    seg_path = info.outputSegmentationPath
    seg_path = normalize_path(seg_path)
    con_path = info.outputSegmentationContourPath
    con_path = normalize_path(con_path)

    file_list = []

    image_file = os.path.join(info.inputFolder, info.inputFilename)
    image_file = normalize_path(image_file)
    file_list.append(image_file)


    struct_seg_path = info.structureSegOutputFolder
    if struct_seg_path != '' and not struct_seg_path.startswith('N/A') and not info.cbrSkipStructureSegmentation:
        struct_seg_path = normalize_path(struct_seg_path)

        # structure segmentation
        struct_seg_file = os.path.join(struct_seg_path, info.structureSegOutputFilename)
        # print(struct_seg_file)
        file_list.append(struct_seg_file)

    # cell segmentation
    cell_seg_file = os.path.join(seg_path, info.outputCellSegWholeFilename)
    file_list.append(cell_seg_file)

    # cell contour segmentation
    cell_con_file = os.path.join(con_path, info.outputCellSegContourFilename)
    file_list.append(cell_con_file)

    # nucleus contour segmentation
    nuc_con_file = os.path.join(con_path, info.outputNucSegContourFilename)
    # print(nuc_seg_file)
    file_list.append(nuc_con_file)

    for f in file_list:
        if not os.path.isfile(f):
            print(batchname + ": " + jobname + ": Could not find file: " + f)


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

        validate(inputfilename, jobname, info)

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
