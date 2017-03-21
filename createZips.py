# import argparse
import csv
import glob
import os
import pandas as pd
# import platform
import zipfile

def make_zips(dataset_name):
    dataset_files = './data/%s/auxiliary_spreadsheets/imageID_ometif_*.xlsx' % dataset_name
    # image_dir = '\\\\aibsdata\\aics\\software_it\\danielt\\images\\AICS\\bisque\\%s\\' % dataset_name
    image_dir = '/data/aics/software_it/danielt/images/AICS/bisque/%s/' % dataset_name
    inputfiles = glob.glob(dataset_files)
    for inputfile in inputfiles:
        dfs = pd.read_excel(inputfile)
        names = dfs['ImageID'].tolist()
        cellline_name = os.path.basename(inputfile).replace('imageID_ometif_', '').replace('.xlsx', '')

        zf = zipfile.ZipFile(os.path.join(image_dir, cellline_name+'.zip'), "w", zipfile.ZIP_DEFLATED, True)
        for image in names:
            imgtozip = os.path.join(image_dir, cellline_name, image)
            zf.write(imgtozip, image)
        zf.close()

def make_zips2(dataset_name):
    # plan: read from delivery_summary based on "dataset" arg
    # delivery_summary contains rows listing all the csv files to load
    datadir = './data/' + dataset_name
    aicsnum_index = {}
    with open(datadir + '/delivery_summary.csv', 'rU') as summarycsvfile:

        # every cell I process will get a line in this file.
        cellnamemapfilename = datadir + '/cellnames.csv'
        cellnamemapfile = open(cellnamemapfilename, 'rU')
        cellnamemapreader = csv.reader(cellnamemapfile)
        cellnamemap = {rows[1]:rows[0] for rows in cellnamemapreader}

        summaryreader = csv.DictReader(summarycsvfile)
        summary_first_field = summaryreader.fieldnames[0]
        for summaryrow in summaryreader:
            if summaryrow[summary_first_field].startswith("#"):
                continue
            if summaryrow[summary_first_field] == "":
                continue

            # i will make one zip file for each eid.
            # the zip file's name will be the aics number plus a _partX index.
            eid = summaryrow['Experiment ID']
            aicsnum = summaryrow['AICS-#']

            # find all images with eid from the cellnamemap
            # eid+'_'
            imagenames = [(aicsnum + '/' + cellnamemap[key] + '.ome.tif') for key in cellnamemap if key.startswith(eid)]

            # output subdirectory name
            # subdir = aicsnum

            if aicsnum in aicsnum_index:
                aicsnum_index[aicsnum] += 1
            else:
                aicsnum_index[aicsnum] = 0

            thefile = open(datadir+'/'+aicsnum+'_part'+str(aicsnum_index[aicsnum])+'.txt', 'wb')
            thefile.write('\n'.join(imagenames))
            thefile.close()


make_zips2('2017_03_08_Struct_First_Pass_Seg')