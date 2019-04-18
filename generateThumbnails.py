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

import make_one_thumbnail


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

    # control what data to process.
    parser.add_argument("--channels", nargs='+', type=int, default=[0], help='which channels')

    runner = parser.add_mutually_exclusive_group()
    runner.add_argument('--run', '-r', help='run the jobs locally', action='store_true', default=False)
    runner.add_argument('--cluster', '-c', help='run jobs using the cluster', action='store_true', default=False)

    args = parser.parse_args()

    return args


def do_image(args, prefs, row, index):
    channelsstr_nospace = 'c' + "".join(map(str, args.channels))

    outdir = '//allen/aics/animated-cell/Dan/april2019mitotic/randomcells/' + channelsstr_nospace + '/'
    outfilename = outdir + '/' + str(row['CellId']) + '_' + channelsstr_nospace + '.png'
    # find the pre-cropped zstack image from the data set.
    infilename = row["CellLine"] + '_' + str(row['FOVId']) + '_' + str(row['CellId']) + '.ome.tif'
    infilename = prefs['out_ometifroot'] + '/' + row['CellLine'] + '/' + infilename

    label = str(row['CellId'])

    channelsstr = " ".join(map(str, args.channels))
    print(str(index) + ' -- ' + label)

    if args.run:
        make_one_thumbnail.make_one_thumbnail(infilename, outfilename, label=label, channels=args.channels, colors=[[1, 1, 1]], size=128, projection='max', axis=2, apply_mask=True, mask_channel=5)
    elif args.cluster:
        return {
            "infile": infilename,
            "outfile": outfilename,
            "label": label,
            "channels": channelsstr
        }


def do_main(args, prefs):
    # Read every cell image to be processed
    data = lkutils.collect_data_rows(fovids=prefs.get('fovs'), raw_only=True)

    print('Number of total cell rows: ' + str(len(data)))
    # # group by fov id
    # data_grouped = data.groupby("FOVId")
    # total_jobs = len(data_grouped)
    # print('Number of total FOVs: ' + str(total_jobs))

    N = 2
    # pick NxN random cells
    data_shuffled = data.sample(n=N*N, random_state=1234).to_dict(orient='records')

    print('ABOUT TO CREATE ' + str(N*N*3) + ' JOBS')

    total_jobs = N*N

    # process each file
    if args.cluster:
        # gather cluster commands and submit in batch
        jobdata_list = []
        for index, row in enumerate(data_shuffled):
            jobdatadict = do_image(args, prefs, row, index)
            jobdata_list.append(jobdatadict)

        print('SUBMITTING ' + str(total_jobs) + ' JOBS')
        jobScheduler.slurp_dicts(jobdata_list, prefs)

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
