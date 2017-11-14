#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import dataHandoffSpreadsheetUtils as utils
import json
import os
import re
import sys
import uploader.db_api as db_api


def do_image(row, prefs):
    batchname = row['source_data']
    jobname = row['inputFilename']
    info = cellJob.CellJob(row)

    imageName = info.cbrCellName
    segs = row["outputCellSegIndex"]
    segs = segs.split(";")
    # get rid of empty strings in segs
    segs = list(filter(None, segs))

    names = [imageName]
    for seg in segs:
        # str(int(seg)) removes leading zeros
        names.append(imageName + "_" + str(int(seg)))

    # check existence of ome.tif and png.

    data_dir = prefs['out_ometifroot']
    thumbs_dir = prefs['out_thumbnailroot']
    # assume that the file location has same name as this subdir name of where the spreadsheet lives:
    path_as_list = re.split(r'\\|/', batchname)
    data_subdir = path_as_list[-3]
    # data_subdir = '2017_03_08_Struct_First_Pass_Seg'
    cell_line = 'AICS-' + str(row["cell_line_ID"])
    for f in names:
        # check for thumbnail
        fullf = os.path.join(thumbs_dir, data_subdir, cell_line, f + '.png')
        if not os.path.isfile(fullf):
            print(batchname + ": " + jobname + ": Could not find file: " + fullf)

        # check for image
        fullf = os.path.join(data_dir, data_subdir, cell_line, f + '.ome.tif')
        if not os.path.isfile(fullf):
            print(batchname + ": " + jobname + ": Could not find file: " + fullf)

        xml = db_api.DbApi.getImagesByName(f)
        if len(xml.getchildren()) != 1:
            print('Retrieved ' + str(len(xml.getchildren())) + ' images with name ' + f)
        if len(xml.getchildren()) > 1:
            dbnames = []
            for i in xml:
                imname = i.get("name")
                if imname in dbnames:
                    imid = i.get("resource_uniq")
                    print("  Deleting: " + imid + " : " + i.get("name"))
                    db_api.DbApi.deleteImage(imid)
                else:
                    dbnames.append(imname)


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py -c -n --dataset 2017_03_08_Struct_First_Pass_Seg')

    # python validateDataHandoff --sheets D:\src\aics\dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='input prefs')

    # sheets replaces input...
    parser.add_argument('--sheets', help='directory containing *.xlsx', default='')

    args = parser.parse_args()

    return args


def report_db_stats():
    xml = db_api.DbApi.getImagesByTagValue("isCropped", "true")
    print('Retrieved ' + str(len(xml.getchildren())) + ' images with "isCropped" true.')
    srcdict = {}
    for element in xml:
        child_el = element.find('tag[@name="source"]')
        src = child_el.attrib['value']
        # TODO find a nicer way to get this grouping.
        # strip away all after the last underscore.
        src = src[:src.rindex('_')]
        # add to dict
        if src not in srcdict:
            srcdict[src] = 1
        else:
            srcdict[src] += 1
    for (key, value) in sorted(srcdict.items()):
        print(key+'\t'+str(value))

    xml = db_api.DbApi.getImagesByTagValue("isCropped", "false")
    print('Retrieved ' + str(len(xml.getchildren())) + ' images with "isCropped" false.')
    srcdict = {}
    for element in xml:
        child_el = element.find('tag[@name="name"]')
        src = child_el.attrib['value']
        # TODO find a nicer way to get this grouping.
        # strip away all after the last underscore.
        src = src[:src.rindex('_')]
        # add to dict
        if src not in srcdict:
            srcdict[src] = 1
        else:
            srcdict[src] += 1
    for (key, value) in sorted(srcdict.items()):
        print(key+'\t'+str(value))


def do_main(args, prefs):
    # Read every .csv file and concat them together
    data = utils.collect_data_rows(prefs['data_files'], db_path=prefs['imageIDs'])

    total_jobs = len(data)
    print('VALIDATING ' + str(total_jobs) + ' JOBS')

    # initialize bisque db.
    session_dict = {
        'root': prefs['out_bisquedb'],
        'user': 'admin',
        'password': 'admin'
    }
    db_api.DbApi.setSessionInfo(session_dict)

    # process each file
    # run serially
    for index, row in enumerate(data):
        do_image(row, prefs)

    report_db_stats()


def main():
    args = parse_args()
    with open(args.prefs) as f:
        prefs = json.load(f)
    do_main(args, prefs)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
