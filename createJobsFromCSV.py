#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org

import argparse
import cellJob
import csv
import json
import os
import sys
from processImageWithSegmentation import do_main
import jobScheduler


# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)

def generateShForRow(outdir, i, subdir, info, do_run):
    # dump row data into json
    current_dir = os.path.join(os.getcwd(), outdir)
    jsonname = os.path.join(current_dir, 'aicsCellJob_'+str(i)+'.json')
    pathjson = os.path.join(outdir, jsonname)
    with open(pathjson, 'w') as fp:
        json.dump(info.__dict__, fp)

    if do_run == "run":
        do_main(pathjson)
    else:
        script_string = open('stock_bash_script.txt').read()
        # script_string += "export PATH=$PATH:/data/aics/software/anaconda2/envs/cb-tools/bin\n"
        script_string = script_string.replace('{jsonobject}', jsonname)
        with open('preferences.json') as jsonreader:
            json_obj = json.load(jsonreader)
        logger = jobScheduler.get_logger('test/logs')
        path = os.path.join(outdir, 'aicsCellJob_' + str(i) + '.sh')
        with open(path, 'w') as fp:
            fp.write(script_string)
            fp.write(os.linesep)
        jobScheduler.submit_job(path, json_obj, logger)


def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py /path/to/csv --outpath /path/to/destination/dir')
    parser.add_argument('input', nargs='+', help='input csv files')
    parser.add_argument('--outpath', help='output path', default='test')
    parser.add_argument('--first', type=int, help='how many to process', default=-1)

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--dryrun', help='write only to local dir and do not add to db', action='store_true')
    group.add_argument('--dbonly', help='only write to db', action='store_true')

    parser.add_argument('--thumbnailsonly', help='only generate thumbnail', action='store_true')
    parser.add_argument('--fullfieldonly', help='only generate fullfield images', action='store_true')

    runner = parser.add_mutually_exclusive_group()
    runner.add_argument('--run', help='run the jobs locally!', action='store_true', default=False)
    runner.add_argument('--cluster', help='run jobs using the cluster!', action='store_true', default=False)

    args = parser.parse_args()

    inputfiles = args.input

    # TODO: recursively search through directories to sequentially parse through each csv file

    i = 0
    for entry in inputfiles:
        fname = entry
        subdir = os.path.splitext(os.path.basename(entry))[0]
        outdir = os.path.join(args.outpath, subdir)
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        with open(fname, 'rU') as csvfile:

            reader = csv.DictReader(csvfile)
            first_field = reader.fieldnames[0]
            for row in reader:
                if row[first_field].startswith("#"):
                    continue

                info = cellJob.CellJob(row)
                info.cbrAddToDb = True
                info.cbrImageLocation = '/data/aics/software_it/danielt/images/AICS/bisque/' + subdir
                info.cbrThumbnailLocation = '/data/aics/software_it/danielt/demos/bisque/thumbnails/' + subdir
                info.cbrThumbnailURL = 'http://stg-aics.corp.alleninstitute.org/danielt_demos/bisque/thumbnails/' + subdir
                if args.fullfieldonly:
                    info.cbrGenerateSegmentedImages = False
                if args.dryrun:
                    info.cbrImageLocation = os.path.abspath(os.path.join(args.outpath, 'images', subdir))
                    info.cbrThumbnailLocation = os.path.abspath(os.path.join(args.outpath, 'images', subdir))
                    info.cbrAddToDb = False
                    info.cbrGenerateThumbnail = True
                    info.cbrGenerateCellImage = True
                elif args.dbonly:
                    info.cbrAddToDb = True
                    info.cbrGenerateThumbnail = False
                    info.cbrGenerateCellImage = False
                elif args.thumbnailsonly:
                    info.cbrAddToDb = False
                    info.cbrGenerateThumbnail = True
                    info.cbrGenerateCellImage = False
                else:
                    info.cbrAddToDb = True
                    info.cbrGenerateThumbnail = True
                    info.cbrGenerateCellImage = True

                if args.run:
                    generateShForRow(outdir, i, subdir, info, "run")
                elif args.cluster:
                    generateShForRow(outdir, i, subdir, info, "cluster")
                i += 1
                if i == args.first:
                    break
        if i == args.first:
            break


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
