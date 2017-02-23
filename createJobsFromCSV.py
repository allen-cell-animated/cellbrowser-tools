#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

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

def generate_sh_for_row(outdir, i, subdir, info, do_run):
    # dump row data into json
    cell_job_postfix = subdir + "_" + str(i)
    current_dir = os.path.join(os.getcwd(), outdir)
    jsonname = os.path.join(current_dir, 'aicsCellJob_'+cell_job_postfix+'.json')
    pathjson = os.path.join(outdir, jsonname)
    with open(pathjson, 'w') as fp:
        json.dump(info.__dict__, fp)

    if do_run == "run":
        do_main(pathjson)
    else:
        script_string = ""
        script_string += "export PYTHONPATH=$PYTHONPATH$( find /data/aics/software/cellbrowser-tools/ " \
                         "-not -path '*/\.*' -type d -printf ':%p' )\n"
        script_string += "python /data/aics/software/cellbrowser-tools/processImageWithSegmentation.py "
        script_string += jsonname
        path = os.path.join(outdir, 'aicsCellJob_' + cell_job_postfix + '.sh')
        with open(path, 'w') as fp:
            fp.write(script_string)
            fp.write(os.linesep)
        if do_run == "cluster":
            with open('preferences.json') as jsonreader:
                json_obj = json.load(jsonreader)
            logger = jobScheduler.get_logger('test/logs')
            jobScheduler.submit_job(path, json_obj, logger)


def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py /path/to/csv')
    parser.add_argument('input', nargs='+', help='input csv files')
    parser.add_argument('--outpath', '-o', help='output path for job files', default='dryrun')
    parser.add_argument('--first', type=int, help='how many to process', default=-1)

    # assuming naming from CSV.  dataroot + dataset + csvname + image names
    # can this be inferred or provided from csv?
    parser.add_argument('--dataset', '-D', help='output directory name for whole batch', default='')

    # database location.  TODO: provide no default and force it to be explicit?
    parser.add_argument('--dburi', help='database url', default='http://10.128.62.104')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--dryrun', '-d', help='write only to local dir and do not add to db', action='store_true')
    group.add_argument('--notdb', '-n', help='write to the server dirs but do not add to db', action='store_true')
    group.add_argument('--dbonly', '-p', help='only post to db', action='store_true')

    generation = parser.add_mutually_exclusive_group()
    generation.add_argument('--thumbnailsonly', '-t', help='only generate thumbnail', action='store_true')
    generation.add_argument('--imagesonly', '-i', help='only generate images', action='store_true')

    cell_images = parser.add_mutually_exclusive_group()
    cell_images.add_argument('--fullfieldonly', '-f', help='only generate fullfield images', action='store_true')
    cell_images.add_argument('--segmentedonly', '-s', help='only generate segmented cell images', action='store_true')

    parser.add_argument('--all', '-a', action='store_true')

    runner = parser.add_mutually_exclusive_group()
    runner.add_argument('--run', '-r', help='run the jobs locally', action='store_true', default=False)
    runner.add_argument('--cluster', '-c', help='run jobs using the cluster', action='store_true', default=False)

    args = parser.parse_args()

    input_files = args.input

    i = 0
    for entry in input_files:
        file_name = entry
        subdir = os.path.splitext(os.path.basename(entry))[0]
        output_dir = os.path.join(args.outpath, subdir)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(file_name, 'rU') as csvfile:

            reader = csv.DictReader(csvfile)
            first_field = reader.fieldnames[0]
            for row in reader:
                if row[first_field].startswith("#"):
                    continue
                if row[first_field] == "":
                    continue

                info = cellJob.CellJob(row)
                info.cbrAddToDb = True

                info.cbrDataRoot = '/data/aics/software_it/danielt/images/AICS/bisque/'
                info.cbrThumbnailRoot = '/data/aics/software_it/danielt/demos/bisque/thumbnails/'
                info.cbrThumbnailWebRoot = 'http://stg-aics.corp.alleninstitute.org/danielt_demos/bisque/thumbnails/'

                info.cbrDatasetName = ''
                if args.dataset:
                    info.cbrDatasetName = args.dataset
                info.cbrImageLocation = info.cbrDataRoot + info.cbrDatasetName + '/' + subdir
                info.cbrThumbnailLocation = info.cbrThumbnailRoot + info.cbrDatasetName + '/' + subdir
                info.cbrThumbnailURL = info.cbrThumbnailWebRoot + info.cbrDatasetName + '/' + subdir

                if args.all:
                    info.cbrAddToDb = True
                    info.cbrGenerateThumbnail = True
                    info.cbrGenerateCellImage = True
                    info.cbrGenerateSegmentedImages = True
                    info.cbrGenerateFullFieldImages = True
                else:
                    if args.dryrun:
                        info.cbrImageLocation = os.path.abspath(os.path.join(args.outpath, 'images', subdir))
                        info.cbrThumbnailLocation = os.path.abspath(os.path.join(args.outpath, 'images', subdir))
                        info.cbrAddToDb = False
                    elif args.dbonly:
                        info.cbrAddToDb = True
                        info.cbrGenerateThumbnail = False
                        info.cbrGenerateCellImage = False
                    elif args.notdb:
                        info.cbrAddToDb = False

                    if args.thumbnailsonly:
                        info.cbrGenerateThumbnail = True
                        info.cbrGenerateCellImage = False
                    elif args.imagesonly:
                        info.cbrGenerateThumbnail = False
                        info.cbrGenerateCellImage = True
                    elif not args.dbonly:
                        info.cbrGenerateThumbnail = True
                        info.cbrGenerateCellImage = True

                    if args.fullfieldonly:
                        info.cbrGenerateSegmentedImages = False
                        info.cbrGenerateFullFieldImages = True
                    elif args.segmentedonly:
                        info.cbrGenerateSegmentedImages = True
                        info.cbrGenerateFullFieldImages = False
                    else:
                        info.cbrGenerateSegmentedImages = True
                        info.cbrGenerateFullFieldImages = True

                if args.run:
                    generate_sh_for_row(output_dir, i, subdir, info, "run")
                elif args.cluster:
                    generate_sh_for_row(output_dir, i, subdir, info, "cluster")
                else:
                    generate_sh_for_row(output_dir, i, subdir, info, "")

                i += 1
                if i == args.first:
                    break
        if i == args.first:
            break


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
