import argparse
import dataHandoffUtils as lkutils
import datasetdatabase as dsdb
import glob
import json
import labkey
from lkaccess import LabKey, QueryFilter
import lkaccess.contexts
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


def do_main(args, prefs):
    lk = LabKey(server_context=lkaccess.contexts.PROD)

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
        atlasdir = prefs['atlas_dir']
        subdir = row['CellLine']
        name = f"{row['CellLine']}_{row['FOVId']}_{row['CellId']}_atlas.json"
        fpath = atlasdir + '/' + subdir + '/' + name

        # get the fov's Dimensiony value.
        fovdims_result = lk.select_first(
            schema_name='microscopy',
            query_name='FOV',
            columns='DimensionX, DimensionY, DimensionZ',
            filter_array=[('FOVId', row['FOVId'], 'eq')]
        )
        dim_x = fovdims_result['DimensionX']
        dim_y = fovdims_result['DimensionY']
        dim_z = fovdims_result['DimensionZ']

        # open the json and doctor it
        print(name)
        print(str(row['x']), str(dim_y - row['y']))
        jsondata = None
        with open(fpath, 'r') as json_file:
            jsondata = json.load(json_file)
            ud = jsondata['userData']
            ud['alignedTransform'] = {
                "translation": [row['x'], dim_y - row['y'], 0],
                "rotation": [0, 0, row['Angle']]
            }

        # write the file
        with open(fpath, 'w') as json_file:
            json.dump(jsondata, json_file)


def main():
    args = parse_args()

    prefs = lkutils.setup_prefs(args.prefs)

    do_main(args, prefs)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
