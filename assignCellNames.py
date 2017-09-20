#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org

import argparse
import os
import pandas as pd
import sys
from cellNameDb import CellNameDatabase


def read_excel(inputfilename):
    df1 = pd.ExcelFile(inputfilename)
    # only use first sheet.
    x = df1.sheet_names[0]
    df2 = pd.read_excel(inputfilename, sheetname=x, encoding='iso-8859-1')
    return df2.to_dict(orient='records')


def read_csv(inputfilename):
    df = pd.read_csv(inputfilename, comment='#')
    return df.to_dict(orient='records')


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
    rows = read_excel(inputfilename)
    for row in rows:
        image_dir = row.get('inputFolder')
        image_filename = row.get('inputFilename')
        cell_line_id = row.get('CellLine')
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

    for workingFile in os.listdir(args.sheets):
        if workingFile.endswith('.xlsx') and not workingFile.startswith('~'):
            fp = os.path.join(args.sheets, workingFile)
            if os.path.isfile(fp):
                do_image_list(fp, db)

    db.writedb()


def main():
    args = parse_args()
    do_main(args)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
