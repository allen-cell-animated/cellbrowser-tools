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
import shutil
import sys
from cellNameDb import CellNameDatabase
from processImageWithSegmentation import do_main_image_with_celljob

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)


def generate_sh_for_row(outdir, jobname, info, do_run, prefs):
    # dump row data into json
    # Cell_job_postfix = subdir + "_" + str(jobnumber)
    cell_job_postfix = jobname
    current_dir = os.path.join(prefs['out_status'], prefs['script_dir']) # os.path.join(os.getcwd(), outdir)
    jsonname = os.path.join(current_dir, 'aicsCellJob_'+cell_job_postfix+'.json')
    pathjson = os.path.join(outdir, jsonname)
    with open(pathjson, 'w') as fp:
        json.dump(info.__dict__, fp)
    script_string = ""
    script_string += "export PYTHONPATH=$PYTHONPATH$( find /allen/aics/apps/tools/cellbrowser-tools/ " \
                     "-not -path '*/\.*' -type d -printf ':%p' )\n"
    script_string += "source activate /home/danielt/.conda/envs/cellbrowser\n"
    script_string += "python " + os.getcwd() + "/processImageWithSegmentation.py "
    script_string += jsonname

    path = os.path.join(current_dir, 'aicsCellJob_' + cell_job_postfix + '.sh')
    with open(path, 'w') as fp:
        fp.write(script_string)
        fp.write(os.linesep)
    return path


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py -c -n')

    # to generate images on cluster:
    # python createJobsFromCSV.py -c -n
    # to generate images serially:
    # python createJobsFromCSV.py -r -n

    # to add images to bisque db via cluster:
    # python createJobsFromCSV.py -c -p
    # to add images to bisque db serially:
    # python createJobsFromCSV.py -r -p

    # python createJobsFromCSV.py -c -n myprefs.json

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='prefs file')

    parser.add_argument('--first', type=int, help='how many to process', default=-1)

    # sheets overrides prefs file...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    # control what data to process.

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--notdb', '-n', help='write to the server dirs but do not add to db', action='store_true')
    group.add_argument('--dbonly', '-p', help='only post to db', action='store_true')

    generation = parser.add_mutually_exclusive_group()
    generation.add_argument('--thumbnailsonly', '-t', help='only generate thumbnail', action='store_true')
    generation.add_argument('--imagesonly', '-i', help='only generate images', action='store_true')

    cell_images = parser.add_mutually_exclusive_group()
    cell_images.add_argument('--fullfieldonly', '-f', help='only generate fullfield images', action='store_true')
    cell_images.add_argument('--segmentedonly', '-s', help='only generate segmented cell images', action='store_true')

    parser.add_argument('--all', '-a', action='store_true')

    runner = parser.add_mutually_exclusive_group()
    runner.add_argument('--run', '-r', help='run the jobs locally', action='store_true', default=False)
    runner.add_argument('--cluster', '-c', help='run jobs using the cluster', action='store_true', default=False)

    args = parser.parse_args()

    return args


def do_image(args, prefs, row, index, total_jobs):
    # dataset is assumed to be in source_data = ....dataset_cellnuc_seg_curated/[DATASET]/spreadsheets_dir/sheet_name
    path_as_list = re.split(r'\\|/', row['source_data'])
    dataset = path_as_list[-3]
    # print(dataset)
    print("(" + str(index) + '/' + str(total_jobs) + ") : Processing " + dataset + ' : ' + row['cbrCellName'] + ' in ' + row['inputFilename'])

    aicscelllineid = row['cell_line_ID']
    subdir = 'AICS-' + str(aicscelllineid)

    info = cellJob.CellJob(row)
    info.cbrAddToDb = True

    # drop images here
    info.cbrDataRoot = prefs['out_ometifroot']
    # drop thumbnails here
    info.cbrThumbnailRoot = prefs['out_thumbnailroot']

    info.cbrDatasetName = dataset

    info.cbrImageLocation = info.cbrDataRoot + info.cbrDatasetName + '/' + subdir
    info.cbrThumbnailLocation = info.cbrThumbnailRoot + info.cbrDatasetName + '/' + subdir
    info.cbrThumbnailURL = info.cbrDatasetName + '/' + subdir

    info.dbUrl = prefs['out_bisquedb']

    if args.all:
        info.cbrAddToDb = True
        info.cbrGenerateThumbnail = True
        info.cbrGenerateCellImage = True
        info.cbrGenerateSegmentedImages = True
        info.cbrGenerateFullFieldImages = True
    else:
        if args.dbonly:
            info.cbrAddToDb = True
            info.cbrGenerateThumbnail = False
            info.cbrGenerateCellImage = False
        elif args.notdb:
            info.cbrAddToDb = False

        if args.thumbnailsonly:
            info.cbrGenerateThumbnail = True
            info.cbrGenerateCellImage = False
        elif args.imagesonly:
            info.cbrGenerateThumbnail = False
            info.cbrGenerateCellImage = True
        elif not args.dbonly:
            info.cbrGenerateThumbnail = True
            info.cbrGenerateCellImage = True

        if args.fullfieldonly:
            info.cbrGenerateSegmentedImages = False
            info.cbrGenerateFullFieldImages = True
        elif args.segmentedonly:
            info.cbrGenerateSegmentedImages = True
            info.cbrGenerateFullFieldImages = False
        else:
            info.cbrGenerateSegmentedImages = True
            info.cbrGenerateFullFieldImages = True


    output_dir = os.path.join(info.cbrDataRoot, subdir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_th_dir = os.path.join(info.cbrThumbnailRoot, subdir)
    if not os.path.exists(output_th_dir):
        os.makedirs(output_th_dir)

    jobname = info.cbrCellName
    if args.run:
        do_main_image_with_celljob(info)
    elif args.cluster:
        # TODO: set arg to copy each indiv file to another output
        return generate_sh_for_row(output_dir, jobname, info, "cluster", prefs)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def do_main(args, prefs):
    # Get all the .csv files in the data dir
    data_paths = glob.glob(prefs['data_files'])

    # cell name listing
    db = CellNameDatabase()

    # Read every .csv file and concat them together
    data = list()
    for path in data_paths:
        data_tmp = utils.get_rows(path)
        for r in data_tmp:
            r['source_data'] = utils.normalize_path(path)
            cellLineId = r.get('CellLine')
            if cellLineId is None:
                cellLineId = r.get('cell_line_ID')
            if cellLineId is None:
                cellLineId = r.get('cellLineId')
            r['cell_line_ID'] = cellLineId
            r['cbrCellName'] = db.get_cell_name(r['cell_line_ID'], r['inputFilename'], r['inputFolder'])
            # print(r['cbrCellName'])
        data = data + data_tmp

    # nothing should have changed, but just in case.
    db.writedb()
    # done with db now.

    total_jobs = len(data)
    print('ABOUT TO CREATE ' + str(total_jobs) + ' JOBS')

    # process each file
    if args.cluster:
        # gather cluster commands and submit in batch
        cmdlist = list()
        for index, row in enumerate(data):
            shcmd = do_image(args, prefs, row, index, total_jobs)
            cmdlist.append(shcmd)

        print('SUBMITTING ' + str(total_jobs) + ' JOBS')
        # submit up to 40 at a time, and use the previous 40 as deps for the next 40.
        jobprefs = prefs['job_prefs']
        for x in batch(cmdlist, 40):
            last_deps = jobScheduler.submit_job_deps(cmdlist, jobprefs)
            jobprefs['deps'] = last_deps

    else:
        # run serially
        for index, row in enumerate(data):
            do_image(args, prefs, row, index, total_jobs)

def setup_prefs(json_path):
    with open(json_path) as f:
        prefs = json.load(f)

    # make the output directories if it doesnt exist
    if not os.path.exists(prefs['out_status']):
        os.makedirs(prefs['out_status'])
    if not os.path.exists(prefs['out_ometifroot']):
        os.makedirs(prefs['out_ometifroot'])
    if not os.path.exists(prefs['out_thumbnailroot']):
        os.makedirs(prefs['out_thumbnailroot'])

    json_path_local = prefs['out_status'] + os.sep + 'prefs.json';
    shutil.copyfile(json_path, json_path_local)
    # if not os.path.exists(json_path_local):
    #     # make a copy of the json object in the parent directory
    #     shutil.copyfile(json_path, json_path_local)
    # else:
    #     # use the local copy
    #     print('Local copy of preference file already exists at ' + json_path_local)
    #     with open(json_path_local) as f:
    #         prefs = json.load(f)

    #record the location of the json object
    prefs['my_path'] = json_path_local
    #record the location of the data object
    prefs['save_log_path'] = prefs['out_status'] + os.sep + prefs['data_log_name']
    prefs['job_prefs']['script_dir'] = prefs['out_status'] + os.sep + prefs['script_dir']

    if not os.path.exists(prefs['job_prefs']['script_dir']):
        os.makedirs(prefs['job_prefs']['script_dir'])

    return prefs

def main():
    args = parse_args()

    prefs = setup_prefs(args.prefs)

    do_main(args, prefs)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
