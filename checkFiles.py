import argparse
import glob
import os
import pandas as pd
import platform
import sys


def listfiles(dirname):
    f = []
    for dirpath, dirnames, filenames in os.walk(dirname):
        for filename in [f for f in filenames if f.endswith(".ome.tif")]:
            f.append(filename)
            # print(filename)
            # print os.path.join(dirpath, filename)
    return f

def cli():
    parser = argparse.ArgumentParser(description='Process data set defined in input files')
    parser.add_argument('input', nargs='+', help='input files')
    parser.add_argument('--compare', help='directory to compare file list')
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

    dfs = []
    for inputfile in inputfiles:
        dfs.append(pd.read_excel(inputfile))

    # Concatenate all data into one DataFrame
    big_frame = pd.concat(dfs, ignore_index=True)
    s = big_frame.to_csv(None, index=False, encoding='utf-8')

    if args.compare:
        f = listfiles(args.compare)
        print(list(set(f) - set(s)))
        print(list(set(s) - set(f)))
    else:
        print(s)



if __name__ == "__main__":
    # print (sys.argv)
    cli()
    sys.exit(0)
