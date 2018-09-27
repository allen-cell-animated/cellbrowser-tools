#!/usr/bin/env python

# authors: Dan Toloudis danielt@alleninstitute.org
#          Zach Crabtree zacharyc@alleninstitute.org

import argparse
import cellJob
import dataHandoffUtils as utils
import json
import os
import re
import sys
import uploader.db_api as db_api

def verify_files(thumbs_dir, data_dir, cell_line, f, jobname):
    # check for thumbnail
    fullf = os.path.join(thumbs_dir, cell_line, f + '.png')
    if not os.path.isfile(fullf):
        print(jobname + ": Could not find file: " + fullf)

    # check for image
    fullf = os.path.join(data_dir, cell_line, f + '.ome.tif')
    if not os.path.isfile(fullf):
        print(jobname + ": Could not find file: " + fullf)

    # check for atlas meta
    fullaj = os.path.join(thumbs_dir, cell_line, f + '_atlas.json')
    if not os.path.isfile(fullaj):
        print(jobname + ": Could not find file: " + fullaj)

    # expect 3 atlas png files
    for i in ['0', '1', '2']:
        fullat = os.path.join(thumbs_dir, cell_line, f + '_atlas_'+i+'.png')
        if not os.path.isfile(fullat):
            print(jobname + ": Could not find file: " + fullat)

    # check for image meta
    fullmj = os.path.join(thumbs_dir, cell_line, f + '_meta.json')
    if not os.path.isfile(fullmj):
        print(jobname + ": Could not find file: " + fullmj)


def do_image(row, prefs, is_dryrun):
    info = cellJob.CellJob(row)
    jobname = info.SourceFilename

    imageName = info.FOV_3dcv_Name
    segs = info.CellId

    names = [imageName]
    for seg in segs:
        # str(int(seg)) removes leading zeros
        names.append(imageName + "_" + str(int(seg)))

    data_dir = prefs['out_ometifroot']
    thumbs_dir = prefs['out_thumbnailroot']
    cell_line = info.CellLineName

    # get associated images from db
    query_result = []
    xml = db_api.DbApi.getImagesByNameRoot(imageName)
    for i in xml:
        query_result.append(i.get("name"))

    names_in_db = []
    # make sure every segmented cell image is in the db
    ids_to_delete = []
    for f in names:
        expected_relpath = cell_line + '/' + f + '.ome.tif'

        found_in_db = False
        for i in xml:
            imgnameindb = i.get("value")
            imname = i.get("name")
            if f + '.ome.tif' == imname:
                found_in_db = True;
                if imname in names_in_db:
                    # repeated image in db.
                    imid = i.get("resource_uniq")
                    print("ERROR: DELETING {} : redundant db entry : {} : {} : {}".format(f, imid, imname, imgnameindb))
                    ids_to_delete.append(imid)
                elif imgnameindb != expected_relpath:
                    print("ERROR path mismatch for " + f + ": db has " + imgnameindb + ' but expected ' + expected_relpath)
                else:
                    names_in_db.append(imname)
        if not found_in_db:
            print('ERROR: {} not found in db ( {} )'.format(f, jobname))


    for i in xml:
        imname = i.get("name")
        # assumes 2 digit aics cell line number 'AICS_##-'
        if not imname[:-8] in names:
            print('ERROR: DELETING {} : not part of data set for {} ( {} ) and will be deleted'.format(imname, imageName, jobname))
            imid = i.get("resource_uniq")
            ids_to_delete.append(imid);
        else:
            # check pathing.
            imgnameindb = i.get("value")
            expected_relpath = cell_line + '/' + imname
            if imgnameindb != expected_relpath:
                print("ERROR path mismatch for " + f + ": db has " + imgnameindb + ' but expected ' + expected_relpath)

    # UNCOMMENT TO DO ACTUAL DELETIONS. DANGER THIS MAY HAVE SIDE EFFECT OF REMOVING OME TIF FILES.
    if not is_dryrun:
        for i in ids_to_delete:
            db_api.DbApi.deleteImage(i)

    for f in names:
        verify_files(thumbs_dir, data_dir, cell_line, f, jobname)


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, '
                                                 'and set up a job script for each row.'
                                                 'Example: python createJobsFromCSV.py -c -n --dataset 2017_03_08_Struct_First_Pass_Seg')

    parser.add_argument('prefs', nargs='?', default='prefs.json', help='input prefs')

    # sheets replaces input...
    parser.add_argument('--dryrun', help='perform deletions if switched off', default='True')

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
    data = utils.collect_data_rows(prefs['data_query'])
    data = data.to_dict(orient='records')

    total_jobs = len(data)
    print('VALIDATING ' + str(total_jobs) + ' JOBS')
    total_cells = 0
    for index, row in enumerate(data):
        segs = row["CellId"]
        # get rid of empty strings in segs
        total_cells += len(segs)
    print('EXPECTING ' + str(total_cells) + ' SINGLE CELLS')


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
        if index % 100 == 0:
            print(str(index))
        do_image(row, prefs, args.dryrun)

    print('**DB REPORT**')
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
