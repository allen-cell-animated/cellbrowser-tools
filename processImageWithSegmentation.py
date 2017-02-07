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
from processFullFieldWithSegmentation import make_fullfield_thumbnail
import cellJob
import thumbnail2
from uploader import oneUp
import pprint


def do_main(fname):
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

    if info.cbrParseError:
        sys.stderr.write("\n\nEncountered parsing error!\n\n###\nCell Job Object\n###\n")
        pprint.pprint(jobspec, stream=sys.stderr)
        return

    processor = ImageProcessor(info)
    processor.generate_and_save()


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


class ImageProcessor:

    # to clarify my reasoning for these specific methods and variables in this class...
    # These methods required a large number of redundant parameters
    # These params would not change throughout the lifecycle of a cellJob object
    # These methods use a cellJob object as one of their params
    # The functions outside of this class do not rely on a cellJob object
    def __init__(self, info):
        self.row = info

        self.channels = ['MEMB', 'STRUCT', 'DNA', 'TRANS', 'SEG_DNA', 'SEG_MEMB', 'SEG_STRUCT']
        self.channel_colors = [
            _rgba255(255, 255, 0, 255),
            _rgba255(255, 0, 255, 255),
            _rgba255(0, 255, 255, 255),
            _rgba255(255, 255, 255, 255),
            _rgba255(255, 0, 0, 255),
            _rgba255(0, 0, 255, 255),
            _rgba255(127, 127, 0, 255)
        ]

        # Setting up directory paths for images
        self.image_file = normalize_path(os.path.join(self.row.inputFolder, self.row.inputFilename))
        self.file_name = str(os.path.splitext(self.row.inputFilename)[0])
        self._generate_paths()

        # Setting up segmentation channels for full image
        self.seg_indices = []
        self.image = self.add_segs_to_img()

    def _generate_paths(self):
        # full fields need different directories than segmented cells do
        self.png_dir = os.path.join(normalize_path(self.row.cbrThumbnailLocation), self.file_name)
        self.ometif_dir = os.path.join(normalize_path(self.row.cbrImageLocation), self.file_name)
        self.png_url = self.row.cbrThumbnailURL + "/" + self.file_name

    def add_segs_to_img(self):
        file_name = os.path.splitext(os.path.basename(self.row.inputFilename))[0]

        outdir = self.row.cbrImageLocation
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        thumbnaildir = self.row.cbrThumbnailLocation
        if not os.path.exists(thumbnaildir):
            os.makedirs(thumbnaildir)

        print("loading segmentations for " + file_name + "...", end="")
        seg_path = self.row.outputSegmentationPath
        seg_path = normalize_path(seg_path)
        # print(seg_path)
        file_list = []
        # nucleus segmentation
        nuc_seg_file = os.path.join(seg_path, self.row.outputNucSegWholeFilename)
        # print(nuc_seg_file)
        file_list.append(nuc_seg_file)

        # cell segmentation
        cell_seg_file = os.path.join(seg_path, self.row.outputCellSegWholeFilename)
        # print(cell_seg_file)
        file_list.append(cell_seg_file)

        struct_seg_path = self.row.structureSegOutputFolder
        struct_seg_path = normalize_path(struct_seg_path)

        # structure segmentation
        struct_seg_file = os.path.join(struct_seg_path, self.row.structureSegOutputFilename)
        # print(struct_seg_file)
        file_list.append(struct_seg_file)

        image_file = os.path.join(self.row.inputFolder, self.row.inputFilename)
        image_file = normalize_path(image_file)
        # print(image_file)
        image = CziReader(image_file).load()
        assert len(image.shape) == 5
        assert image.shape[0] == 1
        # image shape from czi assumed to be ZCYX
        # assume no T dimension for now
        image = image[0, :, :, :, :].transpose(1, 0, 2, 3)

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
                self.seg_indices.append(image.shape[0] - 1)
            else:
                raise ValueError("Image is not a tiff segmentation file!")

        print("done")
        return image

    def generate_and_save(self):
        base = os.path.basename(self.image_file)
        base = os.path.splitext(base)[0]

        if self.row.cbrGenerateFullFieldImages:
            print("generating full field images...", end="")
            # necessary for bisque metadata, this is the config for a fullfield image
            self.row.cbrBounds = None
            self.row.cbrCellIndex = 0
            self.row.cbrSourceImageName = None
            self.row.cbrCellName = os.path.splitext(self.row.inputFilename)[0]

            memb_index, nuc_index, struct_index = self.row.memChannel - 1, self.row.nucChannel - 1, self.row.structureChannel - 1

            if self.row.cbrGenerateThumbnail:
                ffthumb = make_fullfield_thumbnail(self.image, memb_index=memb_index, nuc_index=nuc_index, struct_index=struct_index)
            else:
                ffthumb = None

            if self.row.cbrGenerateCellImage:
                im_to_save = self.image
            else:
                im_to_save = None

            self._save_and_post(image=im_to_save, thumbnail=ffthumb)

            print("done")

        if self.row.cbrGenerateSegmentedImages:
            # assumption: less than 256 cells segmented in the file.
            # assumption: cell segmentation is a numeric index in the pixels
            cell_segmentation_image = self.image[self.seg_indices[1], :, :, :]
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

                if self.row.cbrGenerateCellImage:
                    cropped = crop_to_bounds(self.image, bounds)

                    # turn the seg channels into true masks
                    cropped[self.seg_indices[0]] = image_to_mask(cropped[self.seg_indices[0]], i)
                    cropped[self.seg_indices[1]] = image_to_mask(cropped[self.seg_indices[1]], i)
                    # structure segmentation does not use same masking index rules(?)
                    # cropped[struct_seg_channel] = image_to_mask(cropped[struct_seg_channel], i)
                else:
                    cropped = None

                if self.row.cbrGenerateThumbnail:
                    thumbnail = thumbnail2.makeThumbnail(cropped.copy(), channel_indices=[int(self.row.nucChannel),
                                                                                          int(self.row.memChannel),
                                                                                          int(self.row.structureChannel)],
                                                         size=self.row.cbrThumbnailSize, seg_channel_index=self.seg_indices[1])
                    # making it CYX for the png writer
                    thumb = thumbnail.transpose(2, 0, 1)
                else:
                    thumb = None

                self.row.cbrCellIndex = i
                self.row.cbrSourceImageName = base
                self.row.cbrCellName = base + '_' + str(i)
                self.row.cbrBounds = {'xmin': bounds[0][0], 'xmax': bounds[0][1],
                                      'ymin': bounds[1][0], 'ymax': bounds[1][1],
                                      'zmin': bounds[2][0], 'zmax': bounds[2][1]}
                self._save_and_post(image=cropped, thumbnail=thumb, seg_cell_index=i)
            print("done")

    def _save_and_post(self, image, thumbnail, seg_cell_index=0):
        # physical_size = [0.065, 0.065, 0.29]
        # note these are strings here.  it's ok for xml purposes but not for any math.
        physical_size = [self.row.xyPixelSize, self.row.xyPixelSize, self.row.zPixelSize]
        png_dir, ometif_dir, png_url = self.png_dir, self.ometif_dir, self.png_url
        if seg_cell_index != 0:
            png_dir += '_' + str(seg_cell_index) + '.png'
            ometif_dir += '_' + str(seg_cell_index) + '.ome.tif'
            png_url += '_' + str(seg_cell_index) + '.png'
        else:
            png_dir += '.png'
            ometif_dir += '.ome.tif'
            png_url += '.png'

        if thumbnail is not None:
            with PngWriter(file_path=png_dir, overwrite_file=True) as writer:
                writer.save(thumbnail)

        if image is not None:
            transposed_image = image.transpose(1, 0, 2, 3)
            with OmeTifWriter(file_path=ometif_dir, overwrite_file=True) as writer:
                writer.save(transposed_image, channel_names=self.channels, channel_colors=self.channel_colors,
                            pixels_physical_size=physical_size)

        if self.row.cbrAddToDb:
            self.row.cbrThumbnailURL = png_url
            session_info = {
                'root': 'http://10.128.62.104',
                'user': 'admin',
                'password': 'admin'
            }
            dbkey = oneUp.oneUp(session_info, self.row.__dict__, None)


def _int32(x):
    if x > 0xFFFFFFFF:
        raise OverflowError
    if x > 0x7FFFFFFF:
        x = int(0x100000000-x)
        if x < 2147483648:
            return -x
        else:
            return -2147483648
    return x


def _rgba255(r, g, b, a):
    assert 0 <= r <= 255
    assert 0 <= g <= 255
    assert 0 <= b <= 255
    assert 0 <= a <= 255
    # bit shift to compose rgba tuple
    x = r << 24 | g << 16 | b << 8 | a
    # now force x into a signed 32 bit integer for OME XML Channel Color.
    return _int32(x)


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

    out_path = os.path.join(*path_as_list)
    return out_path
