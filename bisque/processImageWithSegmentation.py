#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org

from aicsimagetools import *
import argparse
import json
# from xml.etree import cElementTree as etree
import numpy as np
import os
import re
import sys

import cellJob
import thumbnail2
from uploader import oneUp

def int32(x):
    if x > 0xFFFFFFFF:
        raise OverflowError
    if x > 0x7FFFFFFF:
        x = int(0x100000000-x)
        if x < 2147483648:
            return -x
        else:
            return -2147483648
    return x


def rgba255(r, g, b, a):
    assert 0 <= r <= 255
    assert 0 <= g <= 255
    assert 0 <= b <= 255
    assert 0 <= a <= 255
    # bit shift to compose rgba tuple
    x = r << 24 | g << 16 | b << 8 | a
    # now force x into a signed 32 bit integer for OME XML Channel Color.
    return int32(x)


# note that shape is expected to be z,y,x
def clamp(x, y, z, shape):
    # do not subtract 1 from max sizes because this will be used as an array range
    # in crop_to_segmentation below
    return max(0, min(x, shape[2])), max(0, min(y, shape[1])), max(0, min(z, shape[0]))


# assuming 3d segmentation image (ZYX)
def get_segmentation_bounds(segmentation_image, index, margin=5):
    # find bounding box
    b = np.argwhere(segmentation_image == index)
    (zstart, ystart, xstart), (zstop, ystop, xstop) = b.min(0), b.max(0) + 1

    # apply margins and clamp to image edges
    # TODO: margin in z is not the same as xy
    xstart, ystart, zstart = clamp(xstart-margin, ystart-margin, zstart-margin, segmentation_image.shape)
    xstop, ystop, zstop = clamp(xstop+margin, ystop+margin, zstop+margin, segmentation_image.shape)

    return [[xstart, xstop],[ystart, ystop],[zstart, zstop]]


# assuming 4d image (CZYX) and bounds as [[xmin,xmax],[ymin,ymax],[zmin,zmax]]
def crop_to_bounds(image, bounds):
    atrim = np.copy(image[:, bounds[2][0]:bounds[2][1], bounds[1][0]:bounds[1][1], bounds[0][0]:bounds[0][1]])
    return atrim


def image_to_mask(image3d, index, mask_positive_value=1):
    return np.where(image3d == index, mask_positive_value, 0).astype(image3d.dtype)


def normalizePath(p):
    # expects paths to start with \\aibsdata !!
    # windows: \\aibsdata
    # mac:     /Volumes (?)
    # linux:   /data

    outPath = os.path.join(*p.split('\\'))

    if sys.platform.startswith('darwin'):
        # mac default mount point
        outPath = re.sub('^%s' % 'aibsdata', '/Volumes', outPath)
    elif sys.platform.startswith('linux'):
        # linux default mount point
        outPath = re.sub('^%s' % 'aibsdata', '/data', outPath)
    else:
        # windows?
        outPath = "\\\\" + outPath

    # print(outPath)
    return outPath


def splitAndCrop(row):
    # row is expected to be a dictionary of:
    #  ,DeliveryDate,Version,inputFolder,inputFilename,
    #  xyPixelSize,zPixelSize,memChannel,nucChannel,structureChannel,structureProteinName,
    #  lightChannel,timePoint,
    #  outputSegmentationPath,outputNucSegWholeFilename,outputCellSegWholeFilename,
    #  structureSegOutputFolder,structureSegOutputFilename

    outdir = row.cbrImageLocation
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    thumbnaildir = row.cbrThumbnailLocation
    if not os.path.exists(thumbnaildir):
        os.makedirs(thumbnaildir)

    channels = ['memb', 'struct', 'dna', 'trans', 'seg_nuc', 'seg_cell', 'seg_struct']
    channel_colors = [
        rgba255(255, 255, 0, 255),
        rgba255(255, 0, 255, 255),
        rgba255(0, 255, 255, 255),
        rgba255(255, 255, 255, 255),
        rgba255(255, 0, 0, 255),
        rgba255(0, 0, 255, 255),
        rgba255(127, 127, 0, 255)
    ]

    # physical_size = [0.065, 0.065, 0.29]
    # note these are strings here.  it's ok for xml purposes but not for any math.
    physical_size = [row.xyPixelSize, row.xyPixelSize, row.zPixelSize]

    structureName = row.structureProteinName

    segPath = row.outputSegmentationPath
    segPath = normalizePath(segPath)
    print(segPath)

    # nucleus segmentation
    nucSegFile = os.path.join(segPath, row.outputNucSegWholeFilename)
    print(nucSegFile)

    # cell segmentation
    cellSegFile = os.path.join(segPath, row.outputCellSegWholeFilename)
    print(cellSegFile)

    structSegPath = row.structureSegOutputFolder
    structSegPath = normalizePath(structSegPath)

    # structure segmentation
    structSegFile = os.path.join(structSegPath, row.structureSegOutputFilename)
    print(structSegFile)

    imageFile = os.path.join(row.inputFolder, row.inputFilename)
    imageFile = normalizePath(imageFile)
    print(imageFile)

    # load the input files
    cellsegreader = TifReader(cellSegFile)
    cellseg = cellsegreader.load()

    nucsegreader = TifReader(nucSegFile)
    nucseg = nucsegreader.load()

    structsegreader = TifReader(structSegFile)
    structseg = structsegreader.load()

    imagereader = CziReader(imageFile)
    image = imagereader.load()
    # print etree.tostring(imagereader.get_metadata())

    # image shape assumed to be T,C,Z,Y,X,1
    # assume no T dimension for now.
    image = image[0, :, :, :, :, 0]
    # image is now CZYX

    # cellseg shape assumed to be Z,Y,X
    assert imagereader.size_z() == cellsegreader.size_z()
    assert imagereader.size_x() == cellsegreader.size_x()
    assert imagereader.size_y() == cellsegreader.size_y()
    assert imagereader.size_z() == nucsegreader.size_z()
    assert imagereader.size_x() == nucsegreader.size_x()
    assert imagereader.size_y() == nucsegreader.size_y()
    assert imagereader.size_z() == structsegreader.size_z()
    assert imagereader.size_x() == structsegreader.size_x()
    assert imagereader.size_y() == structsegreader.size_y()

    # append channels containing segmentations
    # axis=0 is the C axis, and nucseg, cellseg, and structseg are assumed to be of shape ZYX
    image = np.append(image, [nucseg], axis=0)
    nuc_seg_channel = image.shape[0]-1
    image = np.append(image, [cellseg], axis=0)
    cell_seg_channel = image.shape[0]-1
    image = np.append(image, [structseg], axis=0)
    struct_seg_channel = image.shape[0]-1

    base = os.path.basename(imageFile)
    base = os.path.splitext(base)[0]

    # assumption: less than 256 cells segmented in the file.
    # assumption: cell segmentation is a numeric index in the pixels
    h = np.histogram(cellseg, bins=range(0, 256))
    # which bins have segmented pixels?
    # note that this includes zeroes, which is to be ignored.
    h0 = np.nonzero(h[0])[0]
    # for each cell segmented from this image:
    for i in h0:
        if i == 0:
            continue
        print(i)
        outname = base + '_' + str(i)

        bounds = get_segmentation_bounds(cellseg, i)

        cropped = crop_to_bounds(image, bounds)

        # turn the seg channels into true masks
        cropped[nuc_seg_channel] = image_to_mask(cropped[nuc_seg_channel], i)
        cropped[cell_seg_channel] = image_to_mask(cropped[cell_seg_channel], i)
        # structure segmentation does not use same masking index rules(?)
        # cropped[struct_seg_channel] = image_to_mask(cropped[struct_seg_channel], i)

        if row.cbrGenerateThumbnail:
            # assumes cropped is CZYX
            thumbnail = thumbnail2.makeThumbnail(cropped, channel_indices=[int(row.nucChannel), int(row.memChannel), int(row.structureChannel)],
                                                 size=row.cbrThumbnailSize, seg_channel_index=cell_seg_channel)

            pngwriter = pngWriter.PngWriter(os.path.join(thumbnaildir, outname + '.png'))
            pngwriter.save(thumbnail)

        if row.cbrGenerateCellImage:
            # transpose CZYX to ZCYX
            cropped = cropped.transpose(1, 0, 2, 3)

            writer = OmeTifWriter(os.path.join(outdir, outname + '.ome.tif'))
            writer.save(cropped, channel_names=[x.upper() for x in channels],
                        pixels_physical_size=physical_size, channel_colors=channel_colors)

        if row.cbrAddToDb:
            row.cbrBounds = {'xmin': bounds[0][0], 'xmax': bounds[0][1],
                             'ymin': bounds[1][0], 'ymax': bounds[1][1],
                             'zmin': bounds[2][0], 'zmax': bounds[2][1]}
            row.cbrCellIndex = i
            row.cbrSourceImageName = base
            row.cbrCellName = outname
            row.cbrThumbnailURL = 'file://' + thumbnaildir + '/' + outname + '.png'
            dbkey = oneUp.oneUp(None, row.__dict__, None)


def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, and prepare for ingest into bisque db.'
                                                 'Example: python processImageWithSegmentation.py /path/to/csv --outpath /path/to/destination/dir')
    parser.add_argument('input', help='input json file')
    args = parser.parse_args()

    fname = args.input

    # extract json to dictionary.
    jobspec = {}
    with open(fname) as jobfile:
        jobspec = json.load(jobfile)
        info = cellJob.CellJob(jobspec)

    # jobspec is expected to be a dictionary of:
    #  ,DeliveryDate,Version,inputFolder,inputFilename,
    #  xyPixelSize,zPixelSize,memChannel,nucChannel,structureChannel,structureProteinName,
    #  lightChannel,timePoint,
    #  outputSegmentationPath,outputNucSegWholeFilename,outputCellSegWholeFilename,
    #  structureSegOutputFolder,structureSegOutputFilename,
    # image_db_location,
    # thumbnail_location

    splitAndCrop(info)


if __name__ == "__main__":
    print sys.argv
    main()
    sys.exit(0)
