#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org

import argparse
import dataHandoffSpreadsheetUtils as utils
import json
import sys


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python assignCellNames.py --sheets spreadsheets_dir')

    # python assignCellNames --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY
    # python assignCellNames --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='prefs file')

    # sheets overrides prefs file...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    args = parser.parse_args()

    return args


def do_main(args, prefs):
    # Read every .csv file and concat them together
    # this will assign cell names and rewite the cell name db.
    utils.collect_data_rows(prefs['data_files'])


def main():
    args = parse_args()
    with open(args.prefs) as f:
        prefs = json.load(f)
    do_main(args, prefs)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
