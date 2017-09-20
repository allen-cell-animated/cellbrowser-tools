#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import csv
import glob
import jobScheduler
import json
import os
import pandas as pd
import platform
import re
import sys
from cellNameDb import CellNameDatabase
from processImageWithSegmentation import do_main_image

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)


def generate_sh_for_row(outdir, jobname, info, do_run):
    # dump row data into json
    # Cell_job_postfix = subdir + "_" + str(jobnumber)
    cell_job_postfix = jobname
    current_dir = os.path.join(os.getcwd(), outdir)
    jsonname = os.path.join(current_dir, 'aicsCellJob_'+cell_job_postfix+'.json')
    pathjson = os.path.join(outdir, jsonname)
    with open(pathjson, 'w') as fp:
        json.dump(info.__dict__, fp)

    if do_run == "run":
        do_main_image(pathjson)
    else:
        script_string = ""
        script_string += "export PYTHONPATH=$PYTHONPATH$( find /allen/aics/apps/tools/cellbrowser-tools/ " \
                         "-not -path '*/\.*' -type d -printf ':%p' )\n"
        script_string += "source activate /home/danielt/.conda/envs/cellbrowser\n"
        script_string += "python /allen/aics/apps/tools/cellbrowser-tools/processImageWithSegmentation.py "
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


def read_excel(inputfilename):
    df1 = pd.ExcelFile(inputfilename)
    # only use first sheet.
    x = df1.sheet_names[0]
    df2 = pd.read_excel(inputfilename, sheetname=x, encoding='iso-8859-1')
    return df2.to_dict(orient='records')
    # else:
    #     dicts = []
    #     for x in df1.sheet_names:
    #         # out to csv
    #         df2 = pd.read_excel(inputfilename, sheetname=x, encoding='iso-8859-1')
    #         dicts.append(df2.to_dict(orient='records'))
    #     return dicts


def read_csv(inputfilename):
    df = pd.read_csv(inputfilename, comment='#')
    return df.to_dict(orient='records')
    # with open(csvfilename, 'rU') as csvfile:
    #     reader = csv.DictReader(csvfile)
    #     return [row for row in reader]


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py -c -n --dataset 2017_03_08_Struct_First_Pass_Seg')

    # python createJobsFromCSV.py -r -n --first 1 --dataset 2017_03_08_Struct_First_Pass_Seg
    # python createJobsFromCSV.py -r -n --first 1 --dataset 2017_03_08_Struct_First_Pass_Seg delivery_test.csv

    # to generate images:
    # python createJobs --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY --dataset 2017_05_15_tubulin -c -n
    # python createJobs --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY --dataset 2017_05_15_tubulin -r -n

    # to add images to bisque db:
    # python createJobs --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY --dataset 2017_05_15_tubulin -c -p
    # python createJobs --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY --dataset 2017_05_15_tubulin -r -p

    parser.add_argument('input', nargs='?', default='delivery_summary.csv', help='input csv files')
    parser.add_argument('--outpath', '-o', help='output path for job files', default='test')
    parser.add_argument('--first', type=int, help='how many to process', default=-1)

    # assuming naming from CSV.  dataroot + dataset + csvname + image names
    # can this be inferred or provided from csv?
    parser.add_argument('--dataset', '-D', help='output directory name for whole batch', default='')

    # sheets replaces input...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    # control what data to process.

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

    return args


def do_image_list(args, inputfilename, db, skip_structure_segmentation=False):
    # get the "current" max ids from this database.
    id_authority = db

    if inputfilename.endswith('.xlsx'):
        rows = read_excel(inputfilename)
    elif inputfilename.endswith('.csv'):
        rows = read_csv(inputfilename)
    else:
        return 0

    count = 0
    for row in rows:
        print("Processing Row " + str(count) + " in " + inputfilename)
        info = cellJob.CellJob(row)
        info.cbrAddToDb = True

        # drop images here
        info.cbrDataRoot = '/allen/aics/software/danielt/images/AICS/bisque/'
        # drop thumbnails here
        info.cbrThumbnailRoot = '/allen/aics/software/danielt/images/AICS/bisque/'
        # url to thumbnails
        info.cbrThumbnailWebRoot = 'http://stg-aics.corp.alleninstitute.org/danielt_demos/bisque/thumbnails/'

        info.cbrDatasetName = ''
        if args.dataset:
            info.cbrDatasetName = args.dataset

        aicscelllineid = info.cellLineId
        subdir = 'AICS-' + str(aicscelllineid)

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

        if skip_structure_segmentation:
            info.cbrSkipStructureSegmentation = True

        # does this cell already have a number?
        info.cbrCellName = id_authority.get_cell_name(aicscelllineid, info.inputFilename, info.inputFolder)

        # cellnamemapfile.write(info.cbrCellName + ',' + row['inputFilename'])
        # cellnamemapfile.write(os.linesep)

        output_dir = os.path.join(args.outpath, subdir)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        jobname = info.cbrCellName
        if args.run:
            generate_sh_for_row(output_dir, jobname, info, "run")
        elif args.cluster:
            # TODO: set arg to copy each indiv file to another output
            generate_sh_for_row(output_dir, jobname, info, "cluster")
        else:
            generate_sh_for_row(output_dir, jobname, info, "")

        count += 1
        if count == args.first:
            break
    return count  # len(rows)


def do_main(args):
    if platform.system() == 'Windows':
        filenames = []
        for filename in args.input:
            if '*' in filename or '?' in filename or '[' in filename:
                filenames += glob.glob(filename)
            else:
                filenames.append(filename)
        args.input = filenames
    input_files = args.input

    jobcounter = 0
    db = CellNameDatabase()

    # collect up the files to process
    files = []
    if os.path.isfile(args.sheets):
        files.append(args.sheets)
    else:
        for workingFile in os.listdir(args.sheets):
            if (workingFile.endswith('.xlsx') or workingFile.endswith('.csv')) and not workingFile.startswith('~'):
                fp = os.path.join(args.sheets, workingFile)
                if os.path.isfile(fp):
                    files.append(fp)

    # process each file
    for fp in files:
        jbs = do_image_list(args, fp, db, False)
        jobcounter += jbs

    # nothing should have changed, but just in case.
    db.writedb()


def main():
    args = parse_args()
    do_main(args)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
