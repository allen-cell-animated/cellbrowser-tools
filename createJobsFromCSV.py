#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import csv
import dataHandoffUtils as lkutils
import glob
import jobScheduler
import json
import labkey
import os
import platform
import re
import shutil
import sys

from processImageWithSegmentation import do_main_image_with_celljob


def load_cell_line_info():
    server_context = labkey.utils.create_server_context('aics.corp.alleninstitute.org', 'AICS', 'labkey', use_ssl=False)
    my_results = labkey.query.select_rows(
        columns='CellLineId/Name,ProteinId/DisplayName,StructureId/Name,GeneId/Name',
        server_context=server_context,
        schema_name='celllines',
        query_name='CellLineDefinition'
    )
    # organize into dictionary by cell line
    my_results = {
        d["CellLineId/Name"]: {
            "ProteinName": d["ProteinId/DisplayName"],
            "StructureName": d["StructureId/Name"],
            "GeneName": d["GeneId/Name"]
        } for d in my_results['rows']
    }
    return my_results

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailURL file:// uri to cellbrowser thumbnail
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)


def generate_sh_for_row(jobname, info, prefs):
    # dump row data into json
    # Cell_job_postfix = subdir + "_" + str(jobnumber)
    cell_job_postfix = jobname
    current_dir = os.path.join(prefs['out_status'], prefs['script_dir']) # os.path.join(os.getcwd(), outdir)
    jsonname = os.path.join(current_dir, 'aicsCellJob_'+cell_job_postfix+'.json')
    pathjson = jsonname
    with open(pathjson, 'w') as fp:
        json.dump(info.__dict__, fp)
    script_string = ""
    # script_string += "env > /allen/aics/animated-cell/Dan/env.txt\n"
    script_string += "export PATH=/bin:$PATH\n"
    # set anaconda install path.
    script_string += "export PATH=/allen/aics/animated-cell/Dan/anaconda3/bin:$PATH\n"
    # enable locating the source code of these scripts
    script_string += "export PYTHONPATH=$PYTHONPATH:/home/danielt/cellbrowserpipeline/cellbrowser-tools\n"
    # script_string += "source /allen/aics/animated-cell/Dan/venvs/ace/bin/activate\n"
    script_string += "source activate /allen/aics/animated-cell/Dan/venvs/ace\n"
    script_string += "python " + os.getcwd() + "/processImageWithSegmentation.py "
    script_string += jsonname

    path = os.path.join(current_dir, 'aicsCellJob_' + cell_job_postfix + '.sh')
    with open(path, 'w') as fp:
        fp.write(script_string)
        fp.write(os.linesep)
    return path


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py -c -n')

    # to generate images on cluster:
    # python createJobsFromCSV.py -c -n
    # to generate images serially:
    # python createJobsFromCSV.py -r -n

    # to add images to bisque db via cluster:
    # python createJobsFromCSV.py -c -p
    # to add images to bisque db serially:
    # python createJobsFromCSV.py -r -p

    # python createJobsFromCSV.py -c -n myprefs.json

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='prefs file')

    parser.add_argument('--first', type=int, help='how many to process', default=-1)

    # sheets overrides prefs file...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    # control what data to process.

    parser.add_argument('--fovid', '-f', help='select one specific fov id', type=int, default=-1)

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--notdb', '-n', help='write to the server dirs but do not add to db', action='store_true')
    group.add_argument('--dbonly', '-p', help='only post to db', action='store_true')


    generation = parser.add_mutually_exclusive_group()
    generation.add_argument('--thumbnailsonly', '-t', help='only generate thumbnail', action='store_true')
    generation.add_argument('--imagesonly', '-i', help='only generate images', action='store_true')
    generation.add_argument('--atlasonly', '-l', help='only generate texture atlases', action='store_true')

    cell_images = parser.add_mutually_exclusive_group()
    cell_images.add_argument('--fullfieldonly', '-d', help='only generate fullfield images', action='store_true')
    cell_images.add_argument('--segmentedonly', '-s', help='only generate segmented cell images', action='store_true')

    parser.add_argument('--all', '-a', action='store_true')

    runner = parser.add_mutually_exclusive_group()
    runner.add_argument('--run', '-r', help='run the jobs locally', action='store_true', default=False)
    runner.add_argument('--cluster', '-c', help='run jobs using the cluster', action='store_true', default=False)

    args = parser.parse_args()

    return args


def make_json(jobname, info, prefs):
    cell_job_postfix = jobname
    cellline = info.cells[0]['CellLine']
    current_dir = os.path.join(prefs['out_status'], prefs['script_dir'])  # os.path.join(os.getcwd(), outdir)
    dest_dir = os.path.join(current_dir, cellline)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    jsonname = os.path.join(dest_dir, f'FOV_{cell_job_postfix}.json')
    with open(jsonname, 'w') as fp:
        json.dump(info.__dict__, fp)
    return jsonname


def do_image(args, prefs, cell_lines_data, rows, index, total_jobs):
    # use row 0 as the "full field" row
    row = rows[0]

    jobname = row['FOV_3dcv_Name']

    # dataset is assumed to be in source_data = ....dataset_cellnuc_seg_curated/[DATASET]/spreadsheets_dir/sheet_name
    print("(" + str(index) + '/' + str(total_jobs) + ") : Processing " + ' : ' + jobname)

    aicscelllineid = str(row['CellLine'])
    celllinename = aicscelllineid  # 'AICS-' + str(aicscelllineid)
    subdir = celllinename

    cell_line_data = cell_lines_data[celllinename]
    if cell_line_data is None:
        raise('Can\'t find cell line ' + celllinename)

    info = cellJob.CellJob(rows)

    info.structureProteinName = cell_line_data['ProteinName']
    info.structureName = cell_line_data['StructureName']

    # drop images here
    info.cbrDataRoot = prefs['out_ometifroot']
    # drop thumbnails here
    info.cbrThumbnailRoot = prefs['out_thumbnailroot']
    # drop texture atlases here
    info.cbrTextureAtlasRoot = prefs['out_atlasroot']

    info.cbrImageRelPath = subdir
    info.cbrImageLocation = os.path.join(info.cbrDataRoot, info.cbrImageRelPath)
    info.cbrThumbnailLocation = os.path.join(info.cbrThumbnailRoot, info.cbrImageRelPath)
    info.cbrTextureAtlasLocation = os.path.join(info.cbrTextureAtlasRoot, info.cbrImageRelPath)
    info.cbrThumbnailURL = subdir

    info.cbrThumbnailSize = 128

    if args.all:
        info.cbrGenerateThumbnail = True
        info.cbrGenerateCellImage = True
        info.cbrGenerateTextureAtlas = True
        info.cbrGenerateSegmentedImages = True
        info.cbrGenerateFullFieldImages = True
    else:
        if args.dbonly:
            info.cbrGenerateThumbnail = False
            info.cbrGenerateCellImage = False
            info.cbrGenerateTextureAtlas = False
            info.cbrGenerateFullFieldImages = True
            info.cbrGenerateSegmentedImages = True

        if args.thumbnailsonly:
            info.cbrGenerateThumbnail = True
            info.cbrGenerateCellImage = False
            info.cbrGenerateTextureAtlas = False
        elif args.imagesonly:
            info.cbrGenerateThumbnail = False
            info.cbrGenerateCellImage = True
            info.cbrGenerateTextureAtlas = False
        elif args.atlasonly:
            info.cbrGenerateThumbnail = False
            info.cbrGenerateCellImage = False
            info.cbrGenerateTextureAtlas = True
        elif not args.dbonly:
            info.cbrGenerateThumbnail = True
            info.cbrGenerateCellImage = True
            info.cbrGenerateTextureAtlas = True

        info.cbrGenerateFullFieldImages = True
        info.cbrGenerateSegmentedImages = True
        if args.fullfieldonly:
            info.cbrGenerateSegmentedImages = False
            info.cbrGenerateFullFieldImages = True
        elif args.segmentedonly:
            info.cbrGenerateSegmentedImages = True
            info.cbrGenerateFullFieldImages = False
        elif not args.dbonly:
            info.cbrGenerateSegmentedImages = True
            info.cbrGenerateFullFieldImages = True

    if not os.path.exists(info.cbrImageLocation):
        os.makedirs(info.cbrImageLocation)
    if not os.path.exists(info.cbrThumbnailLocation):
        os.makedirs(info.cbrThumbnailLocation)

    if args.run:
        do_main_image_with_celljob(info)
    elif args.cluster:
        # TODO: set arg to copy each indiv file to another output
        return make_json(jobname, info, prefs)


def do_main(args, prefs):

    cell_lines_data = load_cell_line_info()

    # Read every cell image to be processed
    data = lkutils.collect_data_rows(fovids=prefs.get('fovs'))

    print('Number of total cell rows: ' + str(len(data)))
    # group by fov id
    data_grouped = data.groupby("FOVId")
    total_jobs = len(data_grouped)
    print('Number of total FOVs: ' + str(total_jobs))
    print('ABOUT TO CREATE ' + str(total_jobs) + ' JOBS')

    #
    # arrange into list of lists of dicts?

    # one_of_each = data_grouped.first().reset_index()
    # data = data.to_dict(orient='records')


    # process each file
    if args.cluster:
        # gather cluster commands and submit in batch
        json_list = []
        for index, (fovid, group) in enumerate(data_grouped):
            rows = group.to_dict(orient='records')
            json_file = do_image(args, prefs, cell_lines_data, rows, index, total_jobs)
            json_list.append(json_file)

        print('SUBMITTING ' + str(total_jobs) + ' JOBS')
        jobScheduler.slurp(json_list, prefs)

    else:
        # run serially
        for index, (fovid, group) in enumerate(data_grouped):
            rows = group.to_dict(orient='records')
            do_image(args, prefs, cell_lines_data, rows, index, total_jobs)


def setup_prefs(json_path):
    with open(json_path) as f:
        prefs = json.load(f)

    # make the output directories if it doesnt exist
    if not os.path.exists(prefs['out_status']):
        os.makedirs(prefs['out_status'])
    if not os.path.exists(prefs['out_ometifroot']):
        os.makedirs(prefs['out_ometifroot'])
    if not os.path.exists(prefs['out_thumbnailroot']):
        os.makedirs(prefs['out_thumbnailroot'])
    if not os.path.exists(prefs['out_atlasroot']):
        os.makedirs(prefs['out_atlasroot'])

    json_path_local = prefs['out_status'] + os.sep + 'prefs.json'
    shutil.copyfile(json_path, json_path_local)
    # if not os.path.exists(json_path_local):
    #     # make a copy of the json object in the parent directory
    #     shutil.copyfile(json_path, json_path_local)
    # else:
    #     # use the local copy
    #     print('Local copy of preference file already exists at ' + json_path_local)
    #     with open(json_path_local) as f:
    #         prefs = json.load(f)

    #record the location of the json object
    prefs['my_path'] = json_path_local
    #record the location of the data object
    prefs['save_log_path'] = prefs['out_status'] + os.sep + prefs['data_log_name']

    return prefs

def main():
    args = parse_args()

    prefs = setup_prefs(args.prefs)

    do_main(args, prefs)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
