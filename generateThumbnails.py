#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import csv
import dataHandoffUtils as lkutils
import glob
import jobScheduler
import json
import labkey
import os
import platform
import re
import shutil
import sys

from aicsimageprocessing import thumbnailGenerator


def check_nonnegative(value):
    ivalue = int(value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError("%s is an invalid negative int value" % value)
    return ivalue


def parse_args():
    parser = argparse.ArgumentParser(description='Generate a batch of thumbnail images')

    # to generate images on cluster:
    # python createJobsFromCSV.py -c -n
    # to generate images serially:
    # python createJobsFromCSV.py -r -n

    # python createJobsFromCSV.py -c -n myprefs.json

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='prefs file')

    # control what data to process.
    parser.add_argument("--channels", nargs='+', type=int, default=[0], help='which channels')
    parser.add_argument("--projection", type=str, default='max', help='projection max or slice')

    parser.add_argument("-n", type=check_nonnegative, default=0, help='how many randomly selected (0 for all)')
    parser.add_argument("--size", type=int, default=128, help='pixel size')

    runner = parser.add_mutually_exclusive_group()
    runner.add_argument('--run', '-r', help='run the jobs locally', action='store_true', default=False)
    runner.add_argument('--cluster', '-c', help='run jobs using the cluster', action='store_true', default=False)

    args = parser.parse_args()

    return args


def do_image(args, prefs, row, index):
    # I am generating file names here for the outputs of this code.
    # the channel indices will be strung together to form part of the filename:
    # for cell id 789, channels=[0,1,2] and projection="max", the filename will be:
    # 789_c012max.png
    channelsstr_nospace = 'c' + "".join(map(str, args.channels)) + args.projection

    outdir = prefs['out_thumbnailroot']  # e.g. '//allen/aics/animated-cell/Dan/april2019mitotic/randomcells/' + channelsstr_nospace + '/'
    outfilename = str(row['CellId']) + '_' + channelsstr_nospace + '.png'
    outfilepath = os.path.join(outdir, outfilename)
    # This code is assuming that pre-cropped cell ome.tifs exist in prefs['out_ometifroot']/cellline
    # find the pre-cropped zstack image from the data set.
    infilename = row["CellLine"] + '_' + str(row['FOVId']) + '_' + str(row['CellId']) + '.ome.tif'
    infilepath = os.path.join(prefs['out_ometifroot'], row['CellLine'], infilename)

    label = str(row['CellId'])

    channelsstr = " ".join(map(str, args.channels))
    print(str(index) + ' -- ' + label)

    # this is a big assumption about where the cell membrane segmentation lives in all the input files
    mask_channel_index = 5

    if args.run:
        thumbnailGenerator.make_one_thumbnail(infilepath, outfilepath, label=label, channels=args.channels, colors=[[1, 1, 1]], size=128, projection=args.projection, axis=2, apply_mask=True, mask_channel=mask_channel_index)
    elif args.cluster:
        return f"make_thumbnail {infilepath} {outfilepath} --size {args.size} --channels {channelsstr} --mask {mask_channel_index} --projection {args.projection} --label {label}"


def do_main(args, prefs):
    # Read every cell image to be processed
    data = lkutils.collect_data_rows(fovids=prefs.get('fovs'), raw_only=True)

    print('Number of total cell rows: ' + str(len(data)))
    # # group by fov id
    # data_grouped = data.groupby("FOVId")
    # total_jobs = len(data_grouped)
    # print('Number of total FOVs: ' + str(total_jobs))

    if args.n == 0:
        data_shuffled = data.to_dict(orient='records')
    else:
        data_shuffled = data.sample(n=args.n, random_state=1234).to_dict(orient='records')

    total_jobs = len(data_shuffled)
    print('ABOUT TO CREATE ' + str(total_jobs) + ' JOBS')

    # process each file
    if args.cluster:
        # gather cluster commands and submit in batch
        jobdata_list = []
        for index, row in enumerate(data_shuffled):
            jobdata = do_image(args, prefs, row, index)
            jobdata_list.append(jobdata)

        print('SUBMITTING ' + str(total_jobs) + ' JOBS')
        jobScheduler.slurp_commands(jobdata_list, prefs, name="thumbs")

    else:
        # run serially
        for index, row in enumerate(data_shuffled):
            do_image(args, prefs, row, index)


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

    json_path_local = prefs['out_status'] + os.sep + 'prefs.json'
    shutil.copyfile(json_path, json_path_local)
    # if not os.path.exists(json_path_local):
    #     # make a copy of the json object in the parent directory
    #     shutil.copyfile(json_path, json_path_local)
    # else:
    #     # use the local copy
    #     print('Local copy of preference file already exists at ' + json_path_local)
    #     with open(json_path_local) as f:
    #         prefs = json.load(f)

    # record the location of the json object
    prefs['my_path'] = json_path_local
    # record the location of the data object
    prefs['save_log_path'] = prefs['out_status'] + os.sep + prefs['data_log_name']

    return prefs


def main():
    args = parse_args()

    prefs = setup_prefs(args.prefs)

    do_main(args, prefs)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
