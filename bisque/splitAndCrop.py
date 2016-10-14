#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org

from aicsimagetools import *
import argparse
import csv
# from xml.etree import cElementTree as etree
import numpy as np
import os
import re
import sys

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
    atrim = image[:, bounds[2][0]:bounds[2][1], bounds[1][0]:bounds[1][1], bounds[0][0]:bounds[0][1]]
    return atrim


def image_to_mask(image3d, index, mask_positive_value=1):
    return np.where(image3d == index, mask_positive_value, 0).astype(image3d.dtype)


def main():
    channels = ['memb', 'struct', 'dna', 'trans', 'seg_nuc', 'seg_cell']
    # todo: get physical size from CSV
    physical_size = [0.065, 0.065, 0.29]
    channel_colors = [
        rgba255(255, 255, 0, 255),
        rgba255(255, 0, 255, 255),
        rgba255(0, 255, 255, 255),
        rgba255(255, 255, 255, 255),
        rgba255(255, 0, 0, 255),
        rgba255(0, 0, 255, 255)
    ]

    fname = '/Users/danielt/src/aicsviztools/bisque/nuc_cell_seg_selection_info_for_loading_20160906_1.csv'
    with open(fname, 'rU') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            segPath = row['outputSegmentationPath']
            segPath = os.path.join(*segPath.split('\\'))
            # TODO: this only works for default mac mounts
            segPath = re.sub('^%s' % 'aibsdata', '/Volumes', segPath)
            print(segPath)

            nucSegFile = os.path.join(segPath, row['outputNucSegWholeFilename'])
            print(nucSegFile)
            cellSegFile = os.path.join(segPath, row['outputCellSegWholeFilename'])
            print(cellSegFile)


            imageFile = os.path.join(row['inputFolder'], row['inputFilename'])
            imageFile = os.path.join(*imageFile.split('\\'))
            # TODO: this only works for default mac mounts
            imageFile = re.sub('^%s' % 'aibsdata', '/Volumes', imageFile)
            print(imageFile)

            # nucSegFile = '/Users/danielt/src/aicsviztools/bisque/20160705_I01_001.czi_nucWholeIndex.tiff'
            nucsegreader = TifReader(nucSegFile)
            nucseg = nucsegreader.load()
            # cellSegFile = '/Users/danielt/src/aicsviztools/bisque/20160705_I01_001.czi_cellWholeIndex.tiff'
            cellsegreader = TifReader(cellSegFile)
            cellseg = cellsegreader.load()
            # imageFile = '/Users/danielt/src/aicsviztools/bisque/20160705_I01_001.czi'
            imagereader = CziReader(imageFile)
            image = imagereader.load()
            # print etree.tostring(imagereader.get_metadata())

            base = os.path.basename(imageFile)
            base = os.path.splitext(base)[0]

            # image shape assumed to be T,C,Z,Y,X,1
            image = image[0, :, :, :, :, 0]
            # cellseg shape assumed to be Z,Y,X
            assert imagereader.size_z() == cellsegreader.size_z()
            assert imagereader.size_x() == cellsegreader.size_x()
            assert imagereader.size_y() == cellsegreader.size_y()
            assert imagereader.size_z() == nucsegreader.size_z()
            assert imagereader.size_x() == nucsegreader.size_x()
            assert imagereader.size_y() == nucsegreader.size_y()

            # add channels for nucseg and cellseg
            image = np.append(image, [nucseg], axis=0)
            image = np.append(image, [cellseg], axis=0)

            # assumption: less than 256 cells segmented in the file.
            # assumption: cell segmentation is a numeric index in the pixels
            h = np.histogram(cellseg, bins=range(0,256))
            # which bins have segmented pixels?
            # note that this includes zeroes, which is to be ignored.
            h0 = np.nonzero(h[0])[0]
            # for each cell segmented from this image:
            for i in h0:
                if i == 0:
                    continue
                print(i)

                bounds = get_segmentation_bounds(cellseg, i)
                cropped = crop_to_bounds(image, bounds)

                # turn the seg channels into true masks
                cropped[4] = image_to_mask(cropped[4], i)
                cropped[5] = image_to_mask(cropped[5], i)

                cropped = cropped.transpose(1,0,2,3)

                writer = OmeTifWriter('/Users/danielt/src/aicsviztools/bisque/images/Mito/' + base + '_' + str(i) + '.ome.tif')
                writer.save(cropped, channel_names=[x.upper() for x in channels],
                            pixels_physical_size=physical_size, channel_colors=channel_colors)



if __name__ == "__main__":
    print sys.argv
    main()
    sys.exit(0)
