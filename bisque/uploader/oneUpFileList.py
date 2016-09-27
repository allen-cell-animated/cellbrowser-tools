#!/usr/bin/env python

import os
import argparse

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

import uploader

# debug output
printSteps = False

# should the uploaded images be available to all?
uploadPublic = True

# command line args
parser = argparse.ArgumentParser()
# fileList = './files.txt'
parser.add_argument("fileList", help="the file containing names of files to upload, one per line")
# dataPath = 'file:///data/aics/software_it/danielt/images/AICS/bisque/Mito/'
parser.add_argument("dataPath", help="the directory containing the ome tiff files on the file store")
args = parser.parse_args()

path_csv = args.fileList


session = uploader.init()
with open(path_csv, 'rU') as csvfile, open('out.txt', 'w') as outfile:
    dataUrls = []
    contents = [rw.strip() for rw in csvfile.readlines() if not rw.startswith('#')]
    for fname in contents:
        fullpath = args.dataPath + fname + '.ome.tif'
        resource = etree.Element('image',
            name=fname,
            value=fullpath,
            permission='published')
        t = etree.SubElement(resource, 'tag', name='url', value=fullpath, type='link')
        etree.SubElement(resource, 'tag', name='name', value=fname)
        etree.SubElement(resource, 'tag', name='type', value='cellStructureModel')

        resource_uniq = uploader.uploadFileSpec(session, resource, None)
        print fname + ',' + resource_uniq

        dataUrls.append(uploader.dataServiceURL(session) + resource_uniq)

        outfile.write(fname + ',' + resource_uniq + os.linesep)
