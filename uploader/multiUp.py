#!/usr/bin/env python

import os
import csv
import argparse

try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

import uploader

# should the uploaded images be available to all?
uploadPublic = True

# # command line args
# parser = argparse.ArgumentParser()
# parser.add_argument("csv", help="the CSV file containing image data to upload")
# args = parser.parse_args()

# # folder & csv file for data upload
# # put all tags in the csv file
# path_csv = args.csv


# Create XML resources per image
# Each file/image in the BisQue system is described
# with an XML document containing metadata, we'll create
# this document and post it to the system

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

basepath = 'file:///data/aics/Software_IT/danielt/images/AICS/'

suffix = '_%s.tif'
channel = ['struct', 'dna', 'memb', 'seg_dna']

dataset = 'alphactinin/'
img = 'img40_1'
x = etree.Element('image', name=dataset+img, resource_type='image')
for c in channel:
    cx = etree.SubElement(x, 'value')
    cx.text = basepath+dataset+img+(suffix % c)
meta = etree.SubElement(x, 'tag', name='image_meta', type='image_meta', unid='image_meta')
etree.SubElement(meta, 'tag', name='storage', value='multi_file_series')
etree.SubElement(meta, 'tag', name='image_num_c', value=str(len(channel)), type='number')
# etree.SubElement(meta, 'tag', name='image_num_z', value='1', type='number')
# etree.SubElement(meta, 'tag', name='image_num_t', value='1', type='number')
etree.SubElement(meta, 'tag', name='dimensions', value='XYCZT')
# etree.SubElement(meta, 'tag', name='pixel_resolution_x', value='1', type='number')
# etree.SubElement(meta, 'tag', name='pixel_resolution_y', value='1', type='number')
# etree.SubElement(meta, 'tag', name='pixel_resolution_z', value='1', type='number')
etree.SubElement(meta, 'tag', name='channel_0_name', value='STRUCT')
etree.SubElement(meta, 'tag', name='channel_1_name', value='DNA')
etree.SubElement(meta, 'tag', name='channel_2_name', value='MEMB')
etree.SubElement(meta, 'tag', name='channel_3_name', value='SEG_DNA')

# THE UPLOADER.

session = uploader.init()
resource_uniq = uploader.uploadFileSpec(session, x, None)
