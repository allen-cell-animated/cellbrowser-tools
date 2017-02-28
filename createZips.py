# import argparse
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


make_zips('nuc_cell_seg_delivery_20170210')
make_zips('nuc_cell_seg_delivery_20170217')
