#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org
#         Zach Crabtree zacharyc@alleninstitute.org

from __future__ import print_function

from aicsimage.io.cziReader import CziReader
from aicsimage.io.tifReader import TifReader
from aicsimage.io.omeTifWriter import OmeTifWriter
from aicsimage.io.pngWriter import PngWriter
from aicsimage.io.omexml import OMEXML
from aicsimage.io.omexml import qn
import cellJob
import dataHandoffSpreadsheetUtils as utils
from aicsimage.processing import aicsImage
from aicsimage.processing import thumbnailGenerator
from aicsimage.processing import textureAtlas
from uploader import oneUp

import argparse
import copy
import errno
import json
import numpy as np
import os
import pprint
import subprocess
import sys
import traceback


def _int32(x):
    if x > 0xFFFFFFFF:
        raise OverflowError
    if x > 0x7FFFFFFF:
        x = int(0x100000000 - x)
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
    # ths plus-1 means these bounds are min-inclusive and max-exclusive.
    # in other words, looping for i = start; i < stop; ++i
    (zstart, ystart, xstart), (zstop, ystop, xstop) = b.min(0), b.max(0) + 1

    # apply margins and clamp to image edges
    # TODO: margin in z is not the same as xy
    xstart, ystart, zstart = clamp(xstart - margin, ystart - margin, zstart - margin, segmentation_image.shape)
    xstop, ystop, zstop = clamp(xstop + margin, ystop + margin, zstop + margin, segmentation_image.shape)

    return [[xstart, xstop], [ystart, ystop], [zstart, zstop]]


# assuming 4d image (CZYX) and bounds as [[xmin,xmax],[ymin,ymax],[zmin,zmax]]
def crop_to_bounds(image, bounds):
    atrim = np.copy(image[:, bounds[2][0]:bounds[2][1], bounds[1][0]:bounds[1][1], bounds[0][0]:bounds[0][1]])
    return atrim


def image_to_mask(image3d, index, mask_positive_value=1):
    return np.where(image3d == index, mask_positive_value, 0).astype(image3d.dtype)


def make_dir(dirname):
    if not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
        except (OSError) as e:
            if e.errno != errno.EEXIST:
                raise
            pass


def omexmlfind(obj, parent, tag):
    return parent.findall(qn(obj.ns['ome'], tag))


class ImageProcessor:

    # to clarify my reasoning for these specific methods and variables in this class...
    # These methods required a large number of redundant parameters
    # These params would not change throughout the lifecycle of a cellJob object
    # These methods use a cellJob object as one of their params
    # The functions outside of this class do not rely on a cellJob object
    def __init__(self, info):
        self.row = info

        # Setting up directory paths for images
        self.image_file = utils.normalize_path(os.path.join(self.row.inputFolder, self.row.inputFilename))
        self.file_name = self.row.cbrCellName  # str(os.path.splitext(self.row.inputFilename)[0])
        self._generate_paths()

        # Setting up segmentation channels for full image
        self.seg_indices = []
        self.channels_to_mask = []
        self.omexml = None
        # try:
        #     with OmeTifReader(self.ometif_dir + ".ome.tif") as reader:
        #         print("\nloading pre-made image for " + self.file_name + "...", end="")
        #         self.image = reader.load()
        #         if len(self.image.shape) == 5:
        #             self.image = self.image[0]
        #         self.image = self.image.transpose((1, 0, 2, 3))
        #         self.omexml = reader.get_metadata()
        #         self.seg_indices = [4, 5, 6]
        #         print("done")
        # except AssertionError:
        self.image = self.add_segs_to_img()

    def _generate_paths(self):
        # full fields need different directories than segmented cells do
        thumbnaildir = utils.normalize_path(self.row.cbrThumbnailLocation)
        make_dir(thumbnaildir)
        self.png_dir = os.path.join(thumbnaildir, self.file_name)
        self.png_url = self.row.cbrThumbnailURL + "/" + self.file_name

        ometifdir = utils.normalize_path(self.row.cbrImageLocation)
        make_dir(ometifdir)
        self.ometif_dir = os.path.join(ometifdir, self.file_name)

        atlasdir = utils.normalize_path(self.row.cbrTextureAtlasLocation)
        make_dir(atlasdir)
        self.atlas_dir = atlasdir

    def build_file_list(self):
        # start with 4 channels for membrane, structure, nucleus and transmitted light
        self.channel_names = [
            "OBS_Memb",
            "OBS_STRUCT",
            "OBS_DNA",
            "OBS_Trans"
        ]
        self.channel_colors = [
            _rgba255(128, 0, 128, 255),
            _rgba255(128, 128, 0, 255),
            _rgba255(0, 128, 128, 255),
            _rgba255(128, 128, 128, 255)
        ]

        self.channels_to_mask = []

        print("loading segmentations for " + self.file_name + "...", end="")
        seg_path = self.row.outputSegmentationPath
        seg_path = utils.normalize_path(seg_path)
        con_path = self.row.outputSegmentationContourPath
        con_path = utils.normalize_path(con_path)
        # print(seg_path)
        file_list = []

        # structure segmentation
        struct_seg_path = self.row.structureSegOutputFolder
        if struct_seg_path != '' and not struct_seg_path.startswith('N/A') and not self.row.cbrSkipStructureSegmentation:
            struct_seg_path = utils.normalize_path(struct_seg_path)

            # structure segmentation
            struct_seg_file = os.path.join(struct_seg_path, self.row.structureSegOutputFilename)
            # print(struct_seg_file)
            file_list.append(struct_seg_file)
            self.channel_names.append("SEG_STRUCT")
            self.channel_colors.append(_rgba255(255, 0, 0, 255))

        # cell segmentation
        cell_seg_file = os.path.join(seg_path, self.row.outputCellSegWholeFilename)
        # print(cell_seg_file)
        file_list.append(cell_seg_file)
        self.channel_names.append("SEG_Memb")
        self.channel_colors.append(_rgba255(0, 0, 255, 255))
        self.channels_to_mask.append(len(self.channel_names) - 1)

        # nucleus segmentation
        nuc_seg_file = os.path.join(seg_path, self.row.outputNucSegWholeFilename)
        # print(nuc_seg_file)
        file_list.append(nuc_seg_file)
        self.channel_names.append("SEG_DNA")
        self.channel_colors.append(_rgba255(0, 255, 0, 255))
        self.channels_to_mask.append(len(self.channel_names) - 1)

        # cell contour segmentation (good for viz in the volume viewer)
        cell_con_file = os.path.join(con_path, self.row.outputCellSegContourFilename)
        # print(cell_seg_file)
        file_list.append(cell_con_file)
        self.channel_names.append("CON_Memb")
        self.channel_colors.append(_rgba255(255, 255, 0, 255))
        self.channels_to_mask.append(len(self.channel_names) - 1)

        # nucleus contour segmentation (good for viz in the volume viewer)
        nuc_con_file = os.path.join(con_path, self.row.outputNucSegContourFilename)
        # print(nuc_seg_file)
        file_list.append(nuc_con_file)
        self.channel_names.append("CON_DNA")
        self.channel_colors.append(_rgba255(0, 255, 255, 255))
        self.channels_to_mask.append(len(self.channel_names) - 1)

        # # structure contour segmentation
        # struct_con_path = struct_seg_path
        # if struct_con_path != '' and not struct_con_path.startswith('N/A'):
        #     struct_con_path = utils.normalize_path(struct_con_path)
        #
        #     # structure segmentation
        #     struct_con_file = os.path.join(struct_con_path, self.row.structureSegContourFilename)
        #     # print(struct_con_file)
        #     file_list.append(struct_con_file)
        #     self.channel_names.append("CON_STRUCT")
        #     self.channel_colors.append(_rgba255(255, 0, 255, 255))

        return file_list

    def add_segs_to_img(self):
        outdir = self.row.cbrImageLocation
        make_dir(outdir)

        thumbnaildir = self.row.cbrThumbnailLocation
        make_dir(thumbnaildir)

        file_list = self.build_file_list()

        # ALERT no images will be loaded!!!!
        if self.row.cbrAddToDb:
            return None

        image_file = os.path.join(self.row.inputFolder, self.row.inputFilename)
        image_file = utils.normalize_path(image_file)
        # print(image_file)

        # 1. obtain OME XML metadata from original microscopy image
        showinf = 'showinf'
        if sys.platform.startswith('win'):
            showinf += '.bat'
        bfconvert = 'bfconvert'
        if sys.platform.startswith('win'):
            bfconvert += '.bat'

        dir_path = os.path.dirname(os.path.realpath(__file__))

        omexmlstring = subprocess.check_output([os.path.join(dir_path, 'bftools', showinf), '-omexml-only', '-nopix', '-nometa',
                                                image_file],
                                               stdin=None, stderr=None, shell=False)
        # omexml = ET.fromstring(omexmlstring, ET.XMLParser(encoding='ISO-8859-1'))
        self.omexml = OMEXML(xml=omexmlstring)
        # TODO dump this to a file someplace! (use cmd line args in bftools showinf above?)

        # 2. obtain relevant channels from original image file
        cr = CziReader(image_file)
        image = cr.load()
        # image = CziReader(image_file).load()
        if len(image.shape) == 5 and image.shape[0] == 1:
            image = image[0, :, :, :, :]
        assert len(image.shape) == 4
        # image shape from czi assumed to be ZCYX
        # assume no T dimension for now
        # convert to CZYX, so that shape[0] is number of channels:
        image = image.transpose(1, 0, 2, 3)
        # assumption: channel indices are one-based.
        self.channel_indices = [
            self.row.memChannel - 1,
            self.row.structureChannel - 1,
            self.row.nucChannel - 1,
            self.row.lightChannel - 1
        ]
        # image.shape[0] is num of channels.
        assert(image.shape[0] > max(self.channel_indices))
        orig_num_channels = image.shape[0]
        image = np.array([
            image[self.channel_indices[0]],
            image[self.channel_indices[1]],
            image[self.channel_indices[2]],
            image[self.channel_indices[3]]
        ])
        channels_to_remove = [x for x in range(orig_num_channels) if x not in self.channel_indices]

        # 3. fix up XML to reorder channels
        # we want to preserve all channel and plane data for the channels we are keeping!
        # rename:
        #   channel_indices[0] to channel0
        #   channel_indices[1] to channel1
        #   channel_indices[2] to channel2
        #   channel_indices[3] to channel3
        pix = self.omexml.image().Pixels
        chxml = [pix.Channel(channel) for channel in self.channel_indices]
        planes = [pix.get_planes_of_channel(channel) for channel in self.channel_indices]
        # remove channels in reverse order to preserve indices for next removal!!
        # this assumes that channels_to_remove is in ascending order.
        for i in reversed(channels_to_remove):
            pix.remove_channel(i)
        # reset all plane indices
        for i in range(len(planes)):
            for j in planes[i]:
                j.set("TheC", str(i))
        pix.set_SizeC(4)
        chxml = [pix.Channel(channel) for channel in range(0, 4)]
        chxml[0].set_ID('Channel:0:0')
        chxml[1].set_ID('Channel:0:1')
        chxml[2].set_ID('Channel:0:2')
        chxml[3].set_ID('Channel:0:3')

        nch = 4
        self.seg_indices = []
        i = 0
        for f in file_list:
            file_ext = os.path.splitext(f)[1]
            if file_ext == '.tiff' or file_ext == '.tif':
                seg = TifReader(f).load()
                # seg is expected to be TZCYX where T and C are 1
                # image is expected to be CZYX
                assert seg.shape[1] == image.shape[1], f + ' has shape mismatch ' + str(seg.shape[1]) + ' vs ' + str(image.shape[1])
                assert seg.shape[3] == image.shape[2], f + ' has shape mismatch ' + str(seg.shape[3]) + ' vs ' + str(image.shape[2])
                assert seg.shape[4] == image.shape[3], f + ' has shape mismatch ' + str(seg.shape[4]) + ' vs ' + str(image.shape[3])
                # append channels containing segmentations
                self.omexml.image().Pixels.append_channel(nch + i, self.channel_names[nch + i])
                # axis=0 is the C axis, and nucseg, cellseg, and structseg are assumed to be of shape ZYX
                image = np.append(image, [seg[0, :, 0, :, :]], axis=0)
                self.seg_indices.append(image.shape[0] - 1)
                i += 1
            else:
                raise ValueError("Image is not a tiff segmentation file!")

        print("done")
        return image

    def generate_meta(self, a_im, row):
        m = {}
        names = a_im.get_channel_names()
        for i, n in enumerate(names):
            m["channel_"+str(i)+"_name"] = n

        pix = a_im.metadata.image().Pixels
        m["image_num_x"] = pix.get_SizeX()
        m["image_num_y"] = pix.get_SizeY()
        m["image_num_z"] = pix.get_SizeZ()
        m["image_num_c"] = pix.get_SizeC()
        m["image_num_t"] = pix.get_SizeT()

        m["format"] = "OME-TIFF"

        m['date_time'] = a_im.metadata.image().get_AcquisitionDate()

        phys = a_im.get_physical_pixel_size()
        m["pixel_resolution_x"] = phys[0]
        m["pixel_resolution_y"] = phys[1]
        m["pixel_resolution_z"] = phys[2]
        m["pixel_resolution_t"] = 0
        # fix this (get from metadata)
        m["pixel_resolution_unit_x"] = "micrometers"
        m["pixel_resolution_unit_y"] = "micrometers"
        m["pixel_resolution_unit_z"] = "micrometers"
        m["pixel_resolution_unit_t"] = "seconds"

        m["image_dimensions"] = a_im.metadata.image().Pixels.get_DimensionOrder()

        pixeltypes = {
            'uint8':  ('unsigned integer', 8),
            'uint16': ('unsigned integer', 16),
            'uint32': ('unsigned integer', 32),
            'uint64': ('unsigned integer', 64),
            'int8':   ('signed integer', 8),
            'int16':  ('signed integer', 16),
            'int32':  ('signed integer', 32),
            'int64':  ('signed integer', 64),
            'float':  ('floating point', 32),
            'double': ('floating point', 64),
        }
        try:
            t = pixeltypes[a_im.metadata.image().Pixels.get_PixelType().lower()]
            m['image_pixel_format'] = t[0]
            m['image_pixel_depth'] = t[1]
        except KeyError:
            pass

        instrument = omexmlfind(a_im.metadata, a_im.metadata.root_node, "Instrument")
        if len(instrument) > 0:
            instrument = instrument[0]
            objective = omexmlfind(a_im.metadata, instrument, "Objective")
            if len(objective) > 0:
                objective = objective[0]
                m['objective'] = objective.get("NominalMagnification")
                m["numerical_aperture"] = objective.get("LensNA")

        if row.cbrBounds is not None:
            m["bounds"] = [row.cbrBounds['xmin'], row.cbrBounds['xmax'], row.cbrBounds['ymin'], row.cbrBounds['ymax'], row.cbrBounds['zmin'], row.cbrBounds['zmax']]

        if row.cbrSourceImageName is not None:
            m["source"] = row.cbrSourceImageName
            m["isCropped"] = True
        else:
            m["isCropped"] = False
        m["isModel"] = False

        m["cell_line"] = row.cellLineId
        m["cellSegmentationVersion"] = row.VersionNucMemb
        m["structureSegmentationMethod"] = row.StructureSegmentationMethod
        m["structureSegmentationVersion"] = row.VersionStructure
        m["inputFilename"] = row.inputFilename  # czi
        m["name"] = row.cbrCellName

        return m

    def generate_and_save(self):
        base = self.row.cbrCellName

        # indices of channels in the original image
        # before this, indices have been re-organized in add_segs_to_img (in __init__)
        memb_index = 0
        nuc_index = 2
        struct_index = 1

        if self.row.cbrGenerateFullFieldImages:
            print("generating full fields...")
            # necessary for bisque metadata, this is the config for a fullfield image
            self.row.cbrBounds = None
            self.row.cbrCellIndex = 0
            self.row.cbrSourceImageName = None
            # self.row.cbrCellName = os.path.splitext(self.row.inputFilename)[0]

            if self.row.cbrGenerateThumbnail:
                print("making thumbnail...", end="")
                generator = thumbnailGenerator.ThumbnailGenerator(channel_indices=[memb_index, nuc_index, struct_index],
                                                                  size=self.row.cbrThumbnailSize,
                                                                  mask_channel_index=self.seg_indices[1],
                                                                  colors=[[1.0, 0.0, 1.0], [1.0, 1.0, 0.0], [0.0, 1.0, 1.0]],
                                                                  old_alg=True)
                ffthumb = generator.make_thumbnail(self.image.transpose(1, 0, 2, 3), apply_cell_mask=False)
                print("done")
            else:
                ffthumb = None

            if self.row.cbrGenerateCellImage:
                print("making image...", end="")
                im_to_save = self.image
                print("done")
            else:
                im_to_save = None

            if self.image is not None:
                # do texture atlas here
                aimage = aicsImage.AICSImage(self.image, dims="CZYX")
                aimage.metadata = self.omexml
                print('generating atlas ...')
                atlas = textureAtlas.generate_texture_atlas(aimage, name=self.file_name, max_edge=2048, pack_order=None)
                # grab metadata for display
                static_meta = self.generate_meta(aimage, self.row)
            else:
                atlas = None
                static_meta = None

            self._save_and_post(image=im_to_save, thumbnail=ffthumb, textureatlas=atlas, omexml=self.omexml, other_data=static_meta)

        if self.row.cbrGenerateSegmentedImages:
            segs = self.row.outputCellSegIndex
            segs = segs.split(";")
            # get rid of empty strings in segs
            segs = list(filter(None, segs))
            # convert to ints
            segs = list(map(int, segs))

            if self.image is not None:
                # assumption: less than 256 cells segmented in the file.
                # assumption: cell segmentation is a numeric index in the pixels
                cell_segmentation_image = self.image[self.seg_indices[1], :, :, :]
                # which bins have segmented pixels?
                # note that this includes zeroes, which is to be ignored.
                h0 = np.unique(cell_segmentation_image)
                h0 = h0[h0 > 0]

            for i in segs:
                # for each cell segmented from this image:
                print("generating segmented cells...", end="")
                if i == 0:
                    continue
                print(i, end=" ")

                if self.image is not None:
                    bounds = get_segmentation_bounds(cell_segmentation_image, i)
                    cropped = crop_to_bounds(self.image, bounds)
                    # Turn the seg channels into true masks
                    # by zeroing out all elements != i.
                    # Note that structure segmentation and contour does not use same masking index rules -
                    # the values stored are not indexed by cell number.
                    for mi in self.channels_to_mask:
                        cropped[mi] = image_to_mask(cropped[mi], i, 255)
                    self.row.cbrBounds = {'xmin': int(bounds[0][0]), 'xmax': int(bounds[0][1]),
                                          'ymin': int(bounds[1][0]), 'ymax': int(bounds[1][1]),
                                          'zmin': int(bounds[2][0]), 'zmax': int(bounds[2][1])}
                    for bn in self.row.cbrBounds:
                        print(bn, self.row.cbrBounds[bn])
                else:
                    cropped = None

                if self.row.cbrGenerateThumbnail:
                    print("making thumbnail...", end="")
                    generator = thumbnailGenerator.ThumbnailGenerator(channel_indices=[memb_index, nuc_index, struct_index],
                                                                      size=self.row.cbrThumbnailSize,
                                                                      mask_channel_index=self.seg_indices[1],
                                                                      colors=[[1.0, 0.0, 1.0], [1.0, 1.0, 0.0], [0.0, 1.0, 1.0]],
                                                                      old_alg=True)
                    thumb = generator.make_thumbnail(cropped.copy().transpose(1, 0, 2, 3), apply_cell_mask=True)
                    # thumb = thumbnailGenerator.make_segmented_thumbnail(cropped.copy(), channel_indices=[nuc_index, memb_index, struct_index],
                    #                                                     size=self.row.cbrThumbnailSize, seg_channel_index=self.seg_indices[1])
                    print("done")
                else:
                    thumb = None

                self.row.cbrCellIndex = i
                self.row.cbrSourceImageName = base
                self.row.cbrCellName = base + '_' + str(i)


                # copy self.omexml for output
                copyxml = None
                if self.image is not None:
                    print("making image...", end="")
                    copied = copy.deepcopy(self.omexml.dom)
                    copyxml = OMEXML(rootnode=copied)
                    # now fix it up
                    pixels = copyxml.image().Pixels
                    pixels.set_SizeX(cropped.shape[3])
                    pixels.set_SizeY(cropped.shape[2])
                    pixels.set_SizeZ(cropped.shape[1])
                    # if sizeZ changed, then we have to use bounds to fix up the plane elements
                    minz = bounds[2][0]
                    maxz = bounds[2][1]
                    planes = []
                    for pi in range(pixels.get_plane_count()):
                        planes.append(pixels.Plane(pi))
                    for p in planes:
                        pz = p.get_TheZ()
                        # TODO: CONFIRM THAT THIS IS CORRECT!!
                        if pz >= maxz or pz < minz:
                            pixels.node.remove(p.node)
                        else:
                            p.set_TheZ(pz - minz)
                    print("done")

                    # do texture atlas here
                    aimage_cropped = aicsImage.AICSImage(cropped, dims="CZYX")
                    aimage_cropped.metadata = copyxml
                    print('generating atlas ...')
                    atlas_cropped = textureAtlas.generate_texture_atlas(aimage_cropped, name=self.file_name+'_'+str(i), max_edge=2048, pack_order=None)

                    static_meta_cropped = self.generate_meta(aimage_cropped, self.row)
                else:
                    atlas_cropped = None
                    static_meta_cropped = None

                self._save_and_post(image=cropped, thumbnail=thumb, textureatlas=atlas_cropped, seg_cell_index=i, omexml=copyxml, other_data=static_meta_cropped)
            print("done")


    def _save_and_post(self, image, thumbnail, textureatlas, seg_cell_index=0, omexml=None, other_data=None):
        # physical_size = [0.065, 0.065, 0.29]
        # note these are strings here.  it's ok for xml purposes but not for any math.
        physical_size = [self.row.xyPixelSize, self.row.xyPixelSize, self.row.zPixelSize]
        png_dir, ometif_dir, png_url, atlas_dir, meta_dir = self.png_dir, self.ometif_dir, self.png_url, self.atlas_dir, self.png_dir
        if seg_cell_index != 0:
            meta_dir += '_' + str(seg_cell_index) + '_meta.json'
            png_dir += '_' + str(seg_cell_index) + '.png'
            ometif_dir += '_' + str(seg_cell_index) + '.ome.tif'
            png_url += '_' + str(seg_cell_index) + '.png'
        else:
            meta_dir += '_meta.json'
            png_dir += '.png'
            ometif_dir += '.ome.tif'
            png_url += '.png'

        if thumbnail is not None:
            print("saving thumbnail...", end="")
            with PngWriter(file_path=png_dir, overwrite_file=True) as writer:
                writer.save(thumbnail)
            print("done")

        if image is not None:
            transposed_image = image.transpose(1, 0, 2, 3)
            print("saving image...", end="")
            with OmeTifWriter(file_path=ometif_dir, overwrite_file=True) as writer:
                writer.save(transposed_image, omexml=omexml,
                            # channel_names=self.channel_names, channel_colors=self.channel_colors,
                            pixels_physical_size=physical_size)
            print("done")

        if textureatlas is not None:
            print("saving texture atlas...", end="")
            textureatlas.save(self.atlas_dir)
            print("done")

        if other_data is not None:
            print("saving metadata json...", end="")
            with open(meta_dir, 'w') as fp:
                json.dump(other_data, fp)
            print("done")

        if self.row.cbrAddToDb:
            print("adding to db...", end="")
            self.row.channelNames = self.channel_names
            self.row.cbrThumbnailURL = png_url
            session_info = {
                'root': self.row.dbUrl,
                'user': 'admin',
                'password': 'admin'
            }
            dbkey = oneUp.oneUp(session_info, self.row.__dict__, None)
            print("done")


def do_main_image_with_celljob(info):
    processor = ImageProcessor(info)
    processor.generate_and_save()


def do_main_image(fname):
    with open(fname) as jobfile:
        jobspec = json.load(jobfile)
        info = cellJob.CellJob(jobspec)
        if info.cbrParseError:
            sys.stderr.write("\n\nEncountered parsing error!\n\n###\nCell Job Object\n###\n")
            pprint.pprint(jobspec, stream=sys.stderr)
            return
    return do_main_image_with_celljob(info)


def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, and prepare for ingest into bisque db.'
                                                 'Example: python processImageWithSegmentation.py /path/to/csv --outpath /path/to/destination/dir')
    parser.add_argument('input', help='input json file')
    args = parser.parse_args()

    do_main_image(args.input)


if __name__ == "__main__":
    try:
        print(sys.argv)
        main()
        sys.exit(0)

    except Exception as e:
        print(str(e), file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        sys.exit(1)
