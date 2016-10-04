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
# dataPath = 'file:///data/aics/software_it/danielt/images/AICS/bisque/Mito/'
parser.add_argument("dataPath", help="the directory containing the ome tiff files on the file store")
parser.add_argument("thumbnailUrlPath", help="the directory containing the thumbnail files as a url")
parser.add_argument("tagType", default="", help="the tag value to assign to tag name 'type' for all files in fileList")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--list", help="the file containing names of files to upload, one per line")
group.add_argument("--name", help="the file to upload, name only, no extension")
args = parser.parse_args()

thumbnailpath = args.thumbnailUrlPath

def oneUp(fname, tagType, outfile):
    fullpath = args.dataPath + fname + '.ome.tif'
    # assume thumbnail to be a png file and servable from thumbnailpath
    thumbnail = thumbnailpath + fname + '.png'
    resource = etree.Element('image',
                             name=fname,
                             value=fullpath,
                             permission='published')
    t = etree.SubElement(resource, 'tag', name='url', value=fullpath, type='link')
    etree.SubElement(resource, 'tag', name='name', value=fname)
    # if os.path.exists(thumbnail):
    etree.SubElement(resource, 'tag', name='thumbnail', value=thumbnail)
    etree.SubElement(resource, 'tag', name='type', value=tagType)

    resource_uniq = uploader.uploadFileSpec(session, resource, None)
    print fname + ',' + (resource_uniq if resource_uniq is not None else "None")

    outfile.write(fname + ',' + resource_uniq + ',' + fullpath + os.linesep)

session = uploader.init()
if args.list:
    with open(args.list, 'rU') as csvfile, open('out.txt', 'w') as outfile:
        contents = [rw.strip() for rw in csvfile.readlines() if not rw.startswith('#')]
        for fname in contents:
            oneUp(fname, args.tagType, outfile)
else:
    with open('out.txt', 'a') as outfile:
        oneUp(args.name, args.tagType, outfile)
