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

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='prefs file')

    # sheets replaces input...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    args = parser.parse_args()

    return args


def do_image(args, prefs, row, index, total_jobs):
    info = cellJob.CellJob(row)

    imageName = info.cbrCellName
    segs = row["outputCellSegIndex"]
    segs = segs.split(";")
    # get rid of empty strings in segs
    segs = list(filter(None, segs))

    names = [imageName]
    for seg in segs:
        # str(int(seg)) removes leading zeros
        names.append(imageName + "_" + str(int(seg)))

    exts = ['.ome.tif', '.png']
    # check existence of ome.tif and png.

    data_dir = prefs['out_ometifroot']
    thumbs_dir = prefs['out_thumbnailroot']
    # assume that the file location has same name as this subdir name of where the spreadsheet lives:
    path_as_list = re.split(r'\\|/', row['source_data'])
    data_subdir = path_as_list[-3]
    # data_subdir = '2017_03_08_Struct_First_Pass_Seg'
    cell_line = 'AICS-' + str(row["cell_line_ID"])
    for f in names:
        # check for thumbnail
        fullf = os.path.join(thumbs_dir, data_subdir, cell_line, f + '.png')
        if not os.path.isfile(fullf):
            print("ERROR: " + row['source_data'] + ' : Could not find file: ' + fullf)

        # check for image
        fullf = os.path.join(data_dir, data_subdir, cell_line, f + '.ome.tif')
        if not os.path.isfile(fullf):
            print("ERROR: " + row['source_data'] + ' : Could not find file: ' + fullf)


def do_main(args, prefs):
    # Read every .csv file and concat them together
    data = utils.collect_data_rows(prefs['data_files'])

    total_jobs = len(data)
    print('VALIDATING ' + str(total_jobs) + ' JOBS')

    # process each file
    # run serially
    for index, row in enumerate(data):
        do_image(args, prefs, row, index, total_jobs)


def main():
    args = parse_args()
    with open(args.prefs) as f:
        prefs = json.load(f)
    do_main(args, prefs)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
