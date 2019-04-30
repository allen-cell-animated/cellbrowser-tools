import argparse
import dataHandoffUtils as lkutils
import datasetdatabase as dsdb
import glob
import json
import labkey
import os
import pandas as pd
import platform
import re
import shutil
import sys


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py -c -n')
    parser.add_argument('prefs', nargs='?', default='prefs.json', help='prefs file')
    args = parser.parse_args()
    return args


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


def do_main(args, prefs):
    # get the aligned mitotic cell data
    prod = dsdb.DatasetDatabase(config='//allen/aics/animated-cell/Dan/dsdb/prod.json')
    mitodataset = prod.get_dataset(name='april-2019-prod-cells')
    print("GOT INTEGRATED MITOTIC DATA SET")

    # Read every cell image to be processed
    lkdata = lkutils.collect_data_rows(fovids=prefs.get('fovs'), raw_only=True)

    mitodataset = pd.merge(lkdata, mitodataset.ds[['CellId', 'Angle', 'x', 'y']], left_on='CellId', right_on='CellId', how='right')
    # Angle, x, y
    mitodataset = mitodataset.to_dict(orient='records')
    for row in mitodataset:
        # load the atlas.json edit it and resave it
        dir = prefs['out_atlasroot']
        subdir = row['CellLine']
        name = f"{row['CellLine']}_{row['FOVId']}_{row['CellId']}_atlas.json"
        fpath = dir + '/' + subdir + '/' + name

        #open the file

        #write the file

        # open the json and doctor it
        jsondata = None
        with open(fpath, 'r') as json_file:
            jsondata = json.load(json_file)
            ud = jsondata['userData']
            ud['alignedTransform'] = {
                "translation": [row['x'], row['y'], 0],
                "rotation": [0, 0, row['Angle']]
            }

        with open(fpath, 'w') as json_file:
            json.dump(jsondata, json_file)


def main():
    args = parse_args()

    prefs = setup_prefs(args.prefs)

    do_main(args, prefs)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
