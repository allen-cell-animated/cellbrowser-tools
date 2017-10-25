#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import csv
import dataHandoffSpreadsheetUtils as utils
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
    cell_line = 'AICS-' + str(sheet_row["cell_line_ID"])
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
            'root': 'http://dev-aics-dtp-001',
            # 'root': 'http://10.128.62.98',
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

    rows = utils.get_rows(inputfilename)

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
    files = utils.collect_files(args.sheets)

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
