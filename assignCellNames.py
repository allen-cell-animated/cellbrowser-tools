#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org

import argparse
import cellJob
import dataHandoffSpreadsheetUtils as utils
import os
import pandas as pd
import sys
from cellNameDb import CellNameDatabase


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python assignCellNames.py --sheets spreadsheets_dir')

    # python assignCellNames --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY
    # python assignCellNames --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY

    # sheets replaces input...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    args = parser.parse_args()

    return args


def do_image_list(inputfilename, db):
    rows = utils.get_rows(inputfilename)
    for row in rows:
        cell_job = cellJob.CellJob(row)

        image_dir = cell_job.inputFolder
        image_filename = cell_job.inputFilename
        cell_line_id = cell_job.cellLineId

        db.get_cell_name(cell_line_id, image_filename, image_dir)


def do_main(args):
    # GAME PLAN
    # LOAD CELLNAMES.CSV
    # FIND MAX ID PER CELL LINE
    # ADD ENTRIES FROM THE DATA DELIVERY SPREADSHEETS
    # SAVE TABLE BACK TO CELLNAMES.CSV
    # USE TABLE ENTRIES IN CREATEJOBSFROMCSV

    # BIG ASSUMPTION: CELL NAMES ARE UNIQUE

    # get the "current" max ids from this database.
    db = CellNameDatabase()

    files = utils.collect_files(args.sheets)
    for fp in files:
        do_image_list(fp, db)

    db.writedb()


def main():
    args = parse_args()
    do_main(args)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
