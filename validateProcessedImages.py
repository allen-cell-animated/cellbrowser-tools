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
import random
import re
import sys
import uploader.db_api as db_api

import featurehandoff as fh

from cellNameDb import CellNameDatabase
from processImageWithSegmentation import do_main_image

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)


def parse_args():
    parser = argparse.ArgumentParser(description='Validate data files and dump aggregate data to json.'
                                                 'Example: python validateProcessedImages.py')

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='prefs file')

    args = parser.parse_args()

    return args


def make_path(dir0, dir1, filename):
    # return os.path.join(dir0, dir1, filename)
    return dir0 + '/' + dir1 + '/' + filename


def do_image(args, prefs, row, index, total_jobs):
    info = cellJob.CellJob(row)
    jobname = info.FOV_3dcv_Name

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

    err = False
    for f in names:
        # check for thumbnail
        fullf = make_path(thumbs_dir, cell_line, f + '.png')
        if not os.path.isfile(fullf):
            err = True
            print("ERROR: " + jobname + ": Could not find file: " + fullf)

        # check for atlas meta
        fullaj = make_path(thumbs_dir, cell_line, f + '_atlas.json')
        if not os.path.isfile(fullaj):
            err = True
            print("ERROR: " + jobname + ": Could not find file: " + fullaj)

        # expect 3 atlas png files
        for i in ['0', '1', '2']:
            fullat = make_path(thumbs_dir, cell_line, f + '_atlas_'+i+'.png')
            if not os.path.isfile(fullat):
                err = True
                print("ERROR: " + jobname + ": Could not find file: " + fullat)

        # check for image meta
        fullmj = make_path(thumbs_dir, cell_line, f + '_meta.json')
        if not os.path.isfile(fullmj):
            err = True
            print("ERROR: " + jobname + ": Could not find file: " + fullmj)

        # check for image
        fullf = make_path(data_dir, cell_line, f + '.ome.tif')
        if not os.path.isfile(fullf):
            err = True
            print("ERROR: " + jobname + ": Could not find file: " + fullf)


    outrows = []
    if err is not True:
        outrows.append({
            "file_id": imageName,
            "file_name": imageName + '.ome.tif',
            "read_path": make_path(data_dir, cell_line, imageName + '.ome.tif'),
            "file_size": os.path.getsize(make_path(data_dir, cell_line, imageName + '.ome.tif')),
            "CellLineName": cell_line
        })
        for seg in segs:
            n = imageName + "_" + str(int(seg))
            outrows.append({
                "file_id": n,
                "file_name": n + '.ome.tif',
                "read_path": make_path(data_dir, cell_line, n + '.ome.tif'),
                "file_size": os.path.getsize(make_path(data_dir, cell_line, n + '.ome.tif')),
                "CellLineName": cell_line
            })
    return outrows, err


def build_feature_data(prefs):
    featuredata0 = fh.get_full_handoff(algorithm_name="aics-feature", algorithm_version="1.0.0", config="prod.json")
    featuredata1 = fh.get_full_handoff(algorithm_name="aics-mitosis-classifier", algorithm_version="1.0.0", config="prod.json")
    allfeaturedata = pd.merge(featuredata0, featuredata1, how='inner', left_on=['CellId', 'CellLineName', 'FOVId'], right_on=['CellId', 'CellLineName', 'FOVId'])
    allfeaturedata.dropna(inplace=True)
    jsondictlist = fh.df_to_json(allfeaturedata)
    with open(os.path.join(prefs.get("out_status"), 'cell-feature-analysis.json'), 'w', newline="") as output_file:
        output_file.write(json.dumps(jsondictlist))


def do_main(args, prefs):
    # Read every .csv file and concat them together
    data = utils.collect_data_rows(prefs['data_query'], prefs.get("fovs"))
    data = data.to_dict(orient='records')

    total_jobs = len(data)
    print('VALIDATING ' + str(total_jobs) + ' JOBS')

    errorFovs = []
    allfiles = []
    # process each file
    # run serially
    for index, row in enumerate(data):
        filerows, err = do_image(args, prefs, row, index, total_jobs)
        if err is True:
            errorFovs.append(row['FOV_3dcv_Name'])
        else:
            allfiles.extend(filerows)

    if len(errorFovs) > 0:
        with open(os.path.join(prefs.get("out_status"), 'errorFovs.txt'), 'w', newline="") as error_file:
            error_file.write('\n'.join(errorFovs))

    if len(allfiles) > 0:
        keys = allfiles[0].keys()
        with open(os.path.join(prefs.get("out_status"), 'cellviewer-files.csv'), 'w', newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(allfiles)

    build_feature_data(prefs)


def main():
    args = parse_args()
    with open(args.prefs) as f:
        prefs = json.load(f)
    do_main(args, prefs)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
