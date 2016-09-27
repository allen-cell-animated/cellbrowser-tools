#!/usr/bin/env python

import os
import csv
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
parser.add_argument("csv", help="the CSV file containing image data to upload")
args = parser.parse_args()

# folder & csv file for data upload
# put all tags in the csv file
path_csv = args.csv
#path_csv = os.path.join(path, 'myImages.csv')

# all entries in csv will be read into here
files = []


# Parse CSV file and define metadata fields
# We'll create a list of dictionaries with file names
# and their metadata, this way each file could have
# its own metadata fields
with open(path_csv, 'rU') as csvfile:
    print csvfile
    reader = csv.DictReader(rw for rw in csvfile if not rw.startswith('#'))
    for row in reader:
        f = {}
        for k, v in row.iteritems():
            f[k] = v
        if printSteps:
            print f
        files.append(f)

# Create XML resources per image
# Each file/image in the BisQue system is described
# with an XML document containing metadata, we'll create
# this document and post it to the system

ignore = ['url', 'filename', 'resource']
for f in files:
    resource = etree.Element('image', name=f['filename'])
    if 'url' in f.keys():
        resource.set('value', f['url'])
    if uploadPublic:
        resource.set('permission', 'published')
    for k, v in f.iteritems():
        if k not in ignore:
            t = etree.SubElement(resource, 'tag', name=k, value=v)
            if 'url' in k:
                t.set('type', 'link')
    if printSteps:
        print etree.tostring(resource, pretty_print=True)
    f['resource'] = resource




# <image name="image.composed">
#   <value>file://path/file1.tif</value>
#   <value>file://path/file2.tif</value>
#   <value>file://path/file3.tif</value>
#   <tag name="image_meta" type="image_meta" unid="image_meta">
#       <tag name="storage" value="multi_file_series" />
#       <tag name="image_num_z" value="3" type="number" />
#       <tag name="image_num_t" value="1" type="number" />
#       <tag name="image_num_c" value="1" type="number" />
#       <tag name="dimensions" value="XYCZT" />
#       <tag name="pixel_resolution_x" value="0.1" type="number" />
#       <tag name="pixel_resolution_y" value="0.1" type="number" />
#       <tag name="pixel_resolution_z" value="0.6" type="number" />
#       ...
#       <tag name="channel_0_name" value="GFP" />
#   </tag>
# </image>

# THE UPLOADER.

session = uploader.init()

# upload each file
for f in files:
    print '\nuploading %s' % f['filename']
    if not 'url' in f.keys():
        filepath = os.path.abspath(f['filename'])
        if not os.path.exists(filepath):
            print 'Can not find file (skipping) %s' % filepath
            continue
    else:
        filepath = None

    resource = f['resource']

    # use import service to /import/transfer activating import service

    resource_uniq = uploader.uploadFileSpec(session, resource, filepath)

    f['resource_uniq'] = resource_uniq


keys = []
for key in files[0]:
    keys.append(key)


path_csv = path_csv.replace('.csv', '_bisque.csv')

with open(path_csv, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=keys)
    writer.writeheader()
    for f in files:
        if printSteps:
            print 'writing ' + str(f)
        writer.writerow(f)
