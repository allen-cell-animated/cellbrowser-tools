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
from cellNameDb import CellNameDatabase
from processImageWithSegmentation import do_main_image

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)


def do_image(args, prefs, row, index, total_jobs):
    batchname = row['source_data']
    jobname = row['inputFilename']
    info = cellJob.CellJob(row)
    if not info.cellLineId:
        print(batchname + ": " + jobname + ": Bad CellLine: " + str(info.cellLineId))

    seg_path = info.outputSegmentationPath
    seg_path = utils.normalize_path(seg_path)
    con_path = info.outputSegmentationContourPath
    con_path = utils.normalize_path(con_path)

    file_list = []

    image_file = os.path.join(info.inputFolder, info.inputFilename)
    image_file = utils.normalize_path(image_file)
    file_list.append(image_file)


    struct_seg_path = info.structureSegOutputFolder
    if struct_seg_path != '' and not struct_seg_path.startswith('N/A') and not info.cbrSkipStructureSegmentation:
        struct_seg_path = utils.normalize_path(struct_seg_path)

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


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py -c -n --dataset 2017_03_08_Struct_First_Pass_Seg')

    # python validateDataHandoff --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='input prefs')

    # sheets replaces input...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    args = parser.parse_args()

    return args


def do_main(args, prefs):
    # Read every .csv file and concat them together
    data = utils.collect_data_rows(prefs['data_files'], save_db=False)

    total_jobs = len(data)
    print('VALIDATING ' + str(total_jobs) + ' ROWS')

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
