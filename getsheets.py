# -*- coding: utf-8 -*-

import argparse
import glob
import os
import pandas as pd
import platform
import sys


def file_split(file):
    s = file.split('.')
    name = '.'.join(s[:-1])  # get directory name
    return name


def getsheets(inputfile):
    name = file_split(inputfile)

    df1 = pd.ExcelFile(inputfile)
    if len(df1.sheet_names) == 1:
        x = df1.sheet_names[0]
        df2 = pd.read_excel(inputfile, sheetname=x, encoding='iso-8859-1')
        filename = os.path.join(name + '.csv')
        df2.to_csv(filename, index=False, encoding='utf-8')
        print(name + '.csv', 'Done!')
    else:
        try:
            os.makedirs(name)
        except:
            pass
        for x in df1.sheet_names:
            # out to csv
            df2 = pd.read_excel(inputfile, sheetname=x, encoding='iso-8859-1')
            filename = os.path.join(name, x + '.csv')
            df2.to_csv(filename, index=False, encoding='utf-8')
            print(x + '.csv', 'Done!')
    print('\nAll Done!')


def get_sheet_names(inputfile):
    df = pd.ExcelFile(inputfile)
    for i, flavor in enumerate(df.sheet_names):
        print('{0:>3}: {1}'.format(i + 1, flavor))


def cli():
    '''Convert a Excel file with multiple sheets to several file with one sheet.
    Examples:
    \b
        getsheets filename
    \b
        getsheets -n filename
    '''

    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py /path/to/csv')
    parser.add_argument('input', nargs='+', help='input csv files')
    parser.add_argument('--sheet-names', '-n', action='store_true', default=False)
    args = parser.parse_args()

    if platform.system() == 'Windows':
        filenames = []
        for filename in args.input:
            if '*' in filename or '?' in filename or '[' in filename:
                filenames += glob.glob(filename)
            else:
                filenames.append(filename)
        args.input = filenames
    inputfiles = args.input


    # inputfiles = glob.glob(args.input)
    for inputfile in inputfiles:
        if args.sheet_names:
            get_sheet_names(inputfile)
        else:
            getsheets(inputfile)


if __name__ == "__main__":
    print (sys.argv)
    cli()
    sys.exit(0)
