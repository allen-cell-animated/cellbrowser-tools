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


def generate_sh_for_row(jobname, info, prefs):
    # dump row data into json
    # Cell_job_postfix = subdir + "_" + str(jobnumber)
    cell_job_postfix = jobname
    current_dir = os.path.join(prefs['out_status'], prefs['script_dir']) # os.path.join(os.getcwd(), outdir)
    jsonname = os.path.join(current_dir, 'aicsCellJob_'+cell_job_postfix+'.json')
    pathjson = jsonname
    with open(pathjson, 'w') as fp:
        json.dump(info.__dict__, fp)
    script_string = ""
    # script_string += "env > /allen/aics/animated-cell/Dan/env.txt\n"
    script_string += "export PATH=/bin:$PATH\n"
    script_string += "export PYTHONPATH=$PYTHONPATH:/home/danielt/cellbrowserpipeline/cellbrowser-tools:/home/danielt/cellbrowserpipeline/cellbrowser-tools/uploader\n"
    # script_string += "source /home/danielt/.conda/envs/cellbrowser/bin/activate\n"    
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
    # drop texture atlases here
    info.cbrTextureAtlasRoot = prefs['out_atlasroot']

    info.cbrDatasetName = dataset

    info.cbrImageRelPath = os.path.join(info.cbrDatasetName, subdir)
    info.cbrImageLocation = os.path.join(info.cbrDataRoot, info.cbrImageRelPath)
    info.cbrThumbnailLocation = os.path.join(info.cbrThumbnailRoot, info.cbrImageRelPath)
    info.cbrTextureAtlasLocation = os.path.join(info.cbrTextureAtlasRoot, info.cbrImageRelPath)
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

    if not os.path.exists(info.cbrImageLocation):
        os.makedirs(info.cbrImageLocation)
    if not os.path.exists(info.cbrThumbnailLocation):
        os.makedirs(info.cbrThumbnailLocation)

    jobname = info.cbrCellName
    if args.run:
        do_main_image_with_celljob(info)
    elif args.cluster:
        # TODO: set arg to copy each indiv file to another output
        return generate_sh_for_row(jobname, info, prefs)


def do_main(args, prefs):

    # Read every .csv file and concat them together
    data = utils.collect_data_rows(prefs['data_files'], db_path=prefs['imageIDs'])

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
        jobprefs = prefs['job_prefs']
        jobScheduler.submit_jobs_batches(cmdlist, jobprefs, batch_size=196)

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
    if not os.path.exists(prefs['out_atlasroot']):
        os.makedirs(prefs['out_atlasroot'])

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
