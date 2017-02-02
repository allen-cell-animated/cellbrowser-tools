#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org
#         Zach Crabtree zacharyc@alleninstitute.org

from __future__ import print_function
from aicsimagetools import *
import argparse
import json
import numpy as np
import os
import re
import sys
from processFullFieldWithSegmentation import _make_fullfield_thumbnail
import cellJob
import thumbnail2
from uploader import oneUp
import pprint



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

channels = ['MEMB', 'STRUCT', 'DNA', 'TRANS', 'SEG_DNA', 'SEG_MEMB', 'SEG_STRUCT']
channel_colors = [
    rgba255(255, 255, 0, 255),
    rgba255(255, 0, 255, 255),
    rgba255(0, 255, 255, 255),
    rgba255(255, 255, 255, 255),
    rgba255(255, 0, 0, 255),
    rgba255(0, 0, 255, 255),
    rgba255(127, 127, 0, 255)
]


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

    return [[xstart, xstop], [ystart, ystop], [zstart, zstop]]


# assuming 4d image (CZYX) and bounds as [[xmin,xmax],[ymin,ymax],[zmin,zmax]]
def crop_to_bounds(image, bounds):
    atrim = np.copy(image[:, bounds[2][0]:bounds[2][1], bounds[1][0]:bounds[1][1], bounds[0][0]:bounds[0][1]])
    return atrim


def image_to_mask(image3d, index, mask_positive_value=1):
    return np.where(image3d == index, mask_positive_value, 0).astype(image3d.dtype)


def normalize_path(path):
    # expects windows paths to start with \\aibsdata !!
    # windows: \\\\aibsdata\\aics
    windowsroot = '\\\\aibsdata\\aics\\'
    # mac:     /Volumes/aics (???)
    macroot = '/Volumes/aics/'
    # linux:   /data/aics
    linuxroot = '/data/aics/'

    # 1. strip away the root.
    if path.startswith(windowsroot):
        path = path[len(windowsroot):]
    elif path.startswith(linuxroot):
        path = path[len(linuxroot):]
    elif path.startswith(macroot):
        path = path[len(macroot):]
    else:
        # if the path does not reference a known root, don't try to change it.
        # it's probably a local path.
        return path

    # 2. split the path up into a list of dirs
    path_as_list = re.split(r'\\|/', path)

    # 3. insert the proper system root for this platform (without the trailing slash)
    dest_root = ''
    if sys.platform.startswith('darwin'):
        dest_root = macroot[:-1]
    elif sys.platform.startswith('linux'):
        dest_root = linuxroot[:-1]
    else:
        dest_root = windowsroot[:-1]

    path_as_list.insert(0, dest_root)

    outPath = os.path.join(*path_as_list)
    return outPath


def split_and_crop(row):
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

    print("loading segmentations...", end="")
    seg_path = row.outputSegmentationPath
    seg_path = normalize_path(seg_path)
    # print(seg_path)
    file_list = []
    # nucleus segmentation
    nuc_seg_file = os.path.join(seg_path, row.outputNucSegWholeFilename)
    # print(nuc_seg_file)
    file_list.append(nuc_seg_file)

    # cell segmentation
    cell_seg_file = os.path.join(seg_path, row.outputCellSegWholeFilename)
    # print(cell_seg_file)
    file_list.append(cell_seg_file)

    struct_seg_path = row.structureSegOutputFolder
    struct_seg_path = normalize_path(struct_seg_path)

    # structure segmentation
    struct_seg_file = os.path.join(struct_seg_path, row.structureSegOutputFilename)
    # print(struct_seg_file)
    file_list.append(struct_seg_file)

    image_file = os.path.join(row.inputFolder, row.inputFilename)
    image_file = normalize_path(image_file)
    # print(image_file)
    image = CziReader(image_file).load()
    assert len(image.shape) == 5
    assert image.shape[0] == 1
    # image shape from czi assumed to be ZCYX
    # assume no T dimension for now
    image = image[0, :, :, :, :].transpose(1, 0, 2, 3)

    seg_indices = []
    for f in file_list:
        file_ext = os.path.splitext(f)[1]
        if file_ext == '.tiff':
            seg = TifReader(f).load()
            assert seg.shape[0] == image.shape[1]
            assert seg.shape[1] == image.shape[2]
            assert seg.shape[2] == image.shape[3]
            # append channels containing segmentations
            # axis=0 is the C axis, and nucseg, cellseg, and structseg are assumed to be of shape ZYX
            image = np.append(image, [seg], axis=0)
            seg_indices.append(image.shape[0] - 1)
        else:
            raise ValueError("Image is not a tiff segmentation file!")

    print("done")
    print("generating full field images...", end="")

    base = os.path.basename(image_file)
    base = os.path.splitext(base)[0]

    # necessary for bisque metadata, this is the config for a fullfield image
    row.cbrBounds = None
    row.cbrCellIndex = 0
    row.cbrSourceImageName = None
    row.cbrCellName = os.path.splitext(row.inputFilename)[0]

    png_dir, ometif_dir, png_url = _generate_paths(row)
    memb_index, nuc_index, struct_index = row.memChannel - 1, row.nucChannel - 1, row.structureChannel - 1
    ffthumb = _make_fullfield_thumbnail(image, memb_index=memb_index, nuc_index=nuc_index, struct_index=struct_index)
    _save_and_post(row, image=image, thumbnail=ffthumb, thumb_dir=png_dir, out_dir=ometif_dir, thumb_url=png_url)

    if not row.cbrGenerateSegmentedImages:
        return

    print("done")

    # assumption: less than 256 cells segmented in the file.
    # assumption: cell segmentation is a numeric index in the pixels
    cell_segmentation_image = image[seg_indices[1], :, :, :]
    h = np.histogram(cell_segmentation_image, bins=range(0, 256))
    # which bins have segmented pixels?
    # note that this includes zeroes, which is to be ignored.
    h0 = np.nonzero(h[0])[0]
    # for each cell segmented from this image:
    print("generating segmented images...", end="")
    for i in h0:
        if i == 0:
            continue
        print(i, end=" ")

        bounds = get_segmentation_bounds(cell_segmentation_image, i)

        cropped = crop_to_bounds(image, bounds)

        # turn the seg channels into true masks
        cropped[seg_indices[0]] = image_to_mask(cropped[seg_indices[0]], i)
        cropped[seg_indices[1]] = image_to_mask(cropped[seg_indices[1]], i)
        # structure segmentation does not use same masking index rules(?)
        # cropped[struct_seg_channel] = image_to_mask(cropped[struct_seg_channel], i)

        png_dir, ometif_dir, png_url = _generate_paths(row, seg_cell_index=i)
        if row.cbrGenerateThumbnail:
            thumbnail = thumbnail2.makeThumbnail(cropped.copy(), channel_indices=[int(row.nucChannel), int(row.memChannel),
                                                                       int(row.structureChannel)],
                                             size=row.cbrThumbnailSize, seg_channel_index=seg_indices[1])
        # making it CYX for the png writer
        thumb = thumbnail.transpose(2, 0, 1)
        row.cbrCellIndex = i
        row.cbrSourceImageName = base
        row.cbrCellName = base + '_' + str(i)
        row.cbrBounds = {'xmin': bounds[0][0], 'xmax': bounds[0][1],
                         'ymin': bounds[1][0], 'ymax': bounds[1][1],
                         'zmin': bounds[2][0], 'zmax': bounds[2][1]}
        _save_and_post(row, image=cropped, thumbnail=thumb, thumb_dir=png_dir, out_dir=ometif_dir, thumb_url=png_url)
    print("done")


def _save_and_post(row, image, thumbnail, thumb_dir, out_dir, thumb_url):
    # physical_size = [0.065, 0.065, 0.29]
    # note these are strings here.  it's ok for xml purposes but not for any math.
    physical_size = [row.xyPixelSize, row.xyPixelSize, row.zPixelSize]
    if row.cbrGenerateThumbnail:
        if thumbnail is not None:
            with PngWriter(file_path=thumb_dir, overwrite_file=True) as writer:
                writer.save(thumbnail)
        else:
            raise ValueError("Thumbnail is not provided for segmented cell output")

    if row.cbrGenerateCellImage:
        transposed_image = image.transpose(1, 0, 2, 3)
        with OmeTifWriter(file_path=out_dir, overwrite_file=True) as writer:
            writer.save(transposed_image, channel_names=channels, channel_colors=channel_colors, pixels_physical_size=physical_size)

    if row.cbrAddToDb:
        row.cbrThumbnailURL = thumb_url
        session_info = {
            'root': 'http://10.128.62.104',
            'user': 'admin',
            'password': 'admin'
        }
        dbkey = oneUp.oneUp(session_info, row.__dict__, None)


def _generate_paths(row, seg_cell_index=0):
    # full fields need different directories than segmented cells do
    file_name = str(os.path.splitext(row.inputFilename)[0])
    png_dir = os.path.join(normalize_path(row.cbrThumbnailLocation), file_name)
    ometif_dir = os.path.join(normalize_path(row.cbrImageLocation), file_name)
    png_url = row.cbrThumbnailURL + "/"

    if seg_cell_index != 0:
        png_dir += '_' + str(seg_cell_index)
        ometif_dir += '_' + str(seg_cell_index)
        png_url += file_name + '_' + str(seg_cell_index)
    else:
        png_url += file_name

    png_dir += '.png'
    ometif_dir += '.ome.tif'
    png_url += '.png'

    return png_dir, ometif_dir, png_url


def do_main(fname):
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

    # TODO: If info is invalid, print to stderr here
    if info.cbrParseError:
        sys.stderr.write("\n\nEncountered parsing error!\n\n###\nCell Job Object\n###\n")
        pprint.pprint(jobspec, stream=sys.stderr)
        return

    split_and_crop(info)


def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, and prepare for ingest into bisque db.'
                                                 'Example: python processImageWithSegmentation.py /path/to/csv --outpath /path/to/destination/dir')
    parser.add_argument('input', help='input json file')
    args = parser.parse_args()

    do_main(args.input)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
