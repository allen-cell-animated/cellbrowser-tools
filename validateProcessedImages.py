#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import csv
import dataHandoffUtils as utils
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
    info = cellJob.CellJob(row)
    jobname = info.SourceFilename

    imageName = info.FOV_3dcv_Name
    segs = info.CellId

    data_dir = prefs['out_ometifroot']
    thumbs_dir = prefs['out_thumbnailroot']
    cell_line = info.CellLineName

    names = [imageName]
    for seg in segs:
        n = imageName + "_" + str(int(seg))
        # str(int(seg)) removes leading zeros
        names.append(n)

    exts = ['.ome.tif', '.png']
    # check existence of ome.tif and png.

    for f in names:
        # check for thumbnail
        fullf = os.path.join(thumbs_dir, cell_line, f + '.png')
        if not os.path.isfile(fullf):
            print("ERROR: " + jobname + ": Could not find file: " + fullf)

        # check for atlas meta
        fullaj = os.path.join(thumbs_dir, cell_line, f + '_atlas.json')
        if not os.path.isfile(fullaj):
            print("ERROR: " + jobname + ": Could not find file: " + fullaj)

        # expect 3 atlas png files
        for i in ['0', '1', '2']:
            fullat = os.path.join(thumbs_dir, cell_line, f + '_atlas_'+i+'.png')
            if not os.path.isfile(fullat):
                print("ERROR: " + jobname + ": Could not find file: " + fullat)

        # check for image meta
        fullmj = os.path.join(thumbs_dir, cell_line, f + '_meta.json')
        if not os.path.isfile(fullmj):
            print("ERROR: " + jobname + ": Could not find file: " + fullmj)

        # check for image
        fullf = os.path.join(data_dir, cell_line, f + '.ome.tif')
        if not os.path.isfile(fullf):
            print("ERROR: " + jobname + ": Could not find file: " + fullf)


    outrows = []
    outrows.append({
        "file_id": imageName,
        "file_name": imageName + '.ome.tif',
        "read_path": os.path.join(data_dir, cell_line, imageName + '.ome.tif'),
        "file_size": os.path.getsize(os.path.join(data_dir, cell_line, imageName + '.ome.tif')),
        "CellLineName": cell_line
    })
    for seg in segs:
        n = imageName + "_" + str(int(seg))
        outrows.append({
            "file_id": n,
            "file_name": n + '.ome.tif',
            "read_path": os.path.join(data_dir, cell_line, n + '.ome.tif'),
            "file_size": os.path.getsize(os.path.join(data_dir, cell_line, n + '.ome.tif')),
            "CellLineName": cell_line
        })
    return outrows


def do_main(args, prefs):
    # Read every .csv file and concat them together
    data = utils.collect_data_rows(prefs['data_query'])
    data = data.to_dict(orient='records')

    total_jobs = len(data)
    print('VALIDATING ' + str(total_jobs) + ' JOBS')

    allfiles = []
    # process each file
    # run serially
    for index, row in enumerate(data):
        filerows = do_image(args, prefs, row, index, total_jobs)
        allfiles.extend(filerows)

    keys = allfiles[0].keys()
    with open('cellviewer-files-1.3.0.csv', 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(allfiles)

def main():
    args = parse_args()
    with open(args.prefs) as f:
        prefs = json.load(f)
    do_main(args, prefs)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
