#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org

import argparse
import csv
import json
import os
import sys
import cellJob

test_local = True

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)

def generateShForRow(outdir, i, subdir, info):
    # dump row data into json
    jsonname = 'aicsCellJob_'+str(i)+'.json'
    pathjson = os.path.join(outdir, jsonname)
    with open(pathjson, 'w') as fp:
        json.dump(info.__dict__, fp)

    path = os.path.join(outdir, 'aicsCellJob_'+str(i)+'.sh')
    with open(path, 'w') as fp:
        fp.write('python ../../processImageWithSegmentation.py ' + jsonname)
        fp.write(os.linesep)

def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py /path/to/csv --outpath /path/to/destination/dir')
    parser.add_argument('input', nargs='+', help='input csv files')
    parser.add_argument('--outpath', help='output path', default='images')
    args = parser.parse_args()

    inputfiles = args.input

    i = 0
    for entry in inputfiles:
        fname = entry
        subdir = os.path.splitext(entry)[0]
        outdir = os.path.join(args.outpath, subdir)
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        fileList = os.path.join(outdir, 'filelist.csv')

        writeHeader = False
        if not os.path.isfile(fileList):
            writeHeader = True

        with open(fname, 'rU') as csvfile, open(fileList, 'a') as csvOutFile:

            reader = csv.DictReader(csvfile)
            first_field = reader.fieldnames[0]
            for row in reader:
                if row[first_field].startswith("#"):
                    continue

                info = cellJob.CellJob(row)
                if test_local:
                    info.cbrImageLocation = os.path.abspath(os.path.join('images', subdir))
                    info.cbrThumbnailLocation = os.path.abspath(os.path.join('images', subdir))
                else:
                    info.cbrImageLocation = '/data/aics/software_it/danielt/images/AICS/bisque/' + subdir
                    info.cbrThumbnailLocation = '/data/aics/software_it/danielt/demos/bisque/thumbnails/' + subdir

                info.cbrGenerateThumbnail = False
                info.cbrGenerateCellImage = False
                info.cbrAddToDb = True
                generateShForRow(outdir, i, subdir, info)
                i = i + 1
                # break  # only do it once!


if __name__ == "__main__":
    print sys.argv
    main()
    sys.exit(0)
