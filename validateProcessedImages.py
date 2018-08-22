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
    batchname = row['source_data']
    jobname = row['inputFilename']
    info = cellJob.CellJob(row)

    imageName = info.cbrCellName
    segs = row.outputCellSegIndex
    segs = str(segs).split(";")
    # get rid of empty strings in segs
    segs = list(filter(None, segs))
    # convert to ints
    segs = list(map(int, segs))

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
            print("ERROR: " + batchname + ": " + jobname + ": Could not find file: " + fullf)

        # check for atlas meta
        fullaj = os.path.join(thumbs_dir, data_subdir, cell_line, f + '_atlas.json')
        if not os.path.isfile(fullaj):
            print("ERROR: " + batchname + ": " + jobname + ": Could not find file: " + fullaj)

        # expect 3 atlas png files
        for i in ['0', '1', '2']:
            fullat = os.path.join(thumbs_dir, data_subdir, cell_line, f + '_atlas_'+i+'.png')
            if not os.path.isfile(fullat):
                print("ERROR: " + batchname + ": " + jobname + ": Could not find file: " + fullat)

        # check for image meta
        fullmj = os.path.join(thumbs_dir, data_subdir, cell_line, f + '_meta.json')
        if not os.path.isfile(fullmj):
            print("ERROR: " + batchname + ": " + jobname + ": Could not find file: " + fullmj)

        # check for image
        fullf = os.path.join(data_dir, data_subdir, cell_line, f + '.ome.tif')
        if not os.path.isfile(fullf):
            print("ERROR: " + batchname + ": " + jobname + ": Could not find file: " + fullf)


def do_main(args, prefs):
    # Read every .csv file and concat them together
    data = utils.collect_data_rows(prefs['data_files'], db_path=prefs['imageIDs'])

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
