#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org

from __future__ import print_function
from aicsimagetools import *
import argparse
import numpy as np
import os
import scipy
import sys
import skimage.transform as t
import math as m

z_axis_index = 0
_cmy = [[0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]]
# TODO change all functions and return statements to TZCYX ordering


def get_thresholds(image, border_percent=0.1):
    # expects CYX
    # using this allows us to ignore the bright corners of a cell image
    im_width = image.shape[2]
    im_height = image.shape[1]
    left_bound = int(m.floor(border_percent * im_width))
    right_bound = int(m.ceil((1 - border_percent) * im_width))
    bottom_bound = int(m.floor(border_percent * im_height))
    top_bound = int(m.ceil((1 - border_percent) * im_height))

    cut_border = image[:, left_bound:right_bound, bottom_bound:top_bound]
    nonzeros = cut_border[np.nonzero(cut_border)]
    upper_threshold = np.max(cut_border) * .998
    # TODO should users be able to adjust this arbitrary constant?
    lower_threshold = np.mean(nonzeros) - (np.median(nonzeros) / 3)

    return lower_threshold, upper_threshold


def resize_image(im, new_size):
    try:
        im = im.transpose((2, 1, 0))
        downscale_factor = (float(im.shape[1]) / new_size[1])
        im_out = t.pyramid_reduce(im, downscale=downscale_factor)
        im_out = np.transpose(im_out, (2, 0, 1))
    except ValueError:
        new_size = np.array(new_size).astype('double')
        old_size = np.array(im.shape).astype('double')

        zoom_size = np.divide(new_size, old_size)
        # precision?
        im_out = scipy.ndimage.interpolation.zoom(im, zoom_size)

    return im_out


def mask_image(im, mask):
    im_masked = np.multiply(im, mask > 0)
    return im_masked


def create_projection(im, dim, method='max', slice_index=0, sections=3):
    if method == 'max':
        im = np.max(im, dim)
    elif method == 'mean':
        im = np.mean(im, dim)
    elif method == 'sum':
        im = np.sum(im, dim)
    elif method == 'slice':
        im = im[slice_index]
    elif method == 'sections':
        separator = int(m.floor(im.shape[0] / sections))
        # stack is a 2D YX im
        stack = np.zeros(im[0].shape)
        for i in range(sections - 1):
            bottom_bound = separator * i
            top_bound = separator * (i + 1)
            section = np.max(im[bottom_bound:top_bound], dim)
            stack += section
        stack += np.max(im[separator*sections-1:])

        return stack
    # returns 2D image, YX
    return im


def arrange(projz, projx, projy, sx, sy, sz, rescale_inten=True):
    # assume all images are shape [x,y,3]
    # do stuff and return big image
    shZ = projz.shape
    shX = projx.shape
    shY = projy.shape
    assert (len(shZ) == len(shY) == len(shX) == 3)

    im_all = np.zeros(np.hstack((sx + sz, sy + sz, 3)))
    # imz is xXy
    im_all[0:sx, sz:] = projz
    # imy is zXx (so transpose it)
    im_all[0:sx, 0:sz] = np.transpose(projy, (1, 0, 2))
    # imx is zXy
    im_all[sx:, sz:] = projx

    if rescale_inten:
        im_all /= np.max(im_all.flatten())

    return im_all


def subtract_noise_floor(image, bins=256):
    # image is a 3D ZYX image
    immin = image.min()
    immax = image.max()
    hi, bin_edges = np.histogram(image, bins=bins, range=(max(1, immin), immax))
    # index of tallest peak in histogram
    peakind = np.argmax(hi)
    # subtract this out
    thumb = image
    thumb -= bin_edges[peakind]
    # don't go negative
    thumb[thumb < 0] = 0
    return thumb


class ThumbnailGenerator:
    """

    This class is used to generate thumbnails for 4D CZYX images.

    Example:
        generator = ThumbnailGenerator()
        for image in image_array:
            thumbnail = generator.make_thumbnail(image)

    """

    def __init__(self, colors=_cmy, size=128,
                 memb_index=0, struct_index=1, nuc_index=2,
                 memb_seg_index=5, struct_seg_index=6, nuc_seg_index=4,
                 layering="superimpose", projection="slice", proj_sections=-1):
        """
        :param colors: The color palette that will be used to color each channel. The default palette
                       colors the membrane channel cyan, structure with magenta, and nucleus with yellow.
                       Keep color-blind acccessibility in mind.

        :param size: This constrains the image to have the X or Y dims max out at this value, but keep
                     the original aspect ratio of the image.

        :param memb_index: The index in the image that contains the membrane channel

        :param struct_index: The index in the image that contains the structure channel

        :param nuc_index: The index in the image that contains the nucleus channel

        :param memb_seg_index: The index in the image that contains the membrane segmentation channel

        :param struct_seg_index: The index in the image that contains the structure segmentation channel

        :param nuc_seg_index: The index in the image that contains the nucleus segmentation channel

        :param layering: The method that will be used to layer each channel's projection over each other.
                         Options: ["superimpose", "alpha-blend"]
                         - superimpose will overwrite pixels on the final image as it layers each channel
                         - alpha-blend will blend the final image's pixels with each new channel layer

        :param projection: The method that will be used to generate each channel's projection. This is done
                           for each pixel, through the z-axis
                           Options: ["max", "mean", "sum", "slice", "sections"]
                           - max will look through each z-slice, and determine the max value for each pixel
                           - mean will get the mean of all pixels through the z-axis
                           - sum will sum all pixels through the z-axis
                           - slice will take the pixel values from the middle slice of the z-stack
                           - sections will split the zstack into proj_sections number of sections, and take a
                             max projection for each.

        :param proj_sections: The number of sections that will be used to determine projections, if projection="sections"
        """

        assert len(colors) == 3 and len(colors[0]) == 3
        self.colors = colors

        self.size = size
        self.memb_index, self.struct_index, self.nuc_index = memb_index, struct_index, nuc_index
        self.memb_seg_index, self.struct_seg_index, self.nuc_seg_index = memb_seg_index, struct_seg_index, nuc_seg_index
        self.channel_indices = [self.memb_index, self.struct_index, self.nuc_index]
        self.seg_indices = [self.nuc_seg_index, self.memb_seg_index, self.struct_seg_index]

        assert layering == "superimpose" or layering == "alpha-blend"
        self.layering_mode = layering

        assert projection == "slice" or projection == "max" or projection == "sections"
        self.projection_mode = projection
        self.proj_sections = proj_sections

    def _get_output_shape(self, im_size):
        """
        This method will take in a 3D ZYX shape and return a 3D XYC of the final thumbnail

        :param im_size: 3D ZYX shape of original image
        :return: CYX dims for a resized thumbnail where the maximum X or Y dimension is the one specified in the constructor.
        """
        # size down to this edge size, maintaining aspect ratio.
        max_edge = self.size
        # keep same number of z slices.
        shape_out = np.hstack((im_size[0],
                               max_edge if im_size[1] > im_size[2] else max_edge * im_size[1] / im_size[2],
                               max_edge if im_size[1] < im_size[2] else max_edge * im_size[2] / im_size[1]
                               ))
        return 3, shape_out[2], shape_out[1]

    def _layer_projections(self, projection_array):
        """
        This method will take in a list of 2D XY projections and layer them according to the method specified in the constructor

        :param projection_array: list of 2D XY projections (for each channel of a cell image)
        :return: single 3D XYC image where C is RGB values for each pixel
        """
        # array cannot be empty or have more channels than the color array
        assert projection_array
        assert len(projection_array) == len(self.colors)
        layered_image = np.zeros((projection_array[0].shape[0], projection_array[0].shape[1], 4))

        for i in range(len(projection_array)):
            projection = projection_array[i]
            projection /= np.max(projection)
            assert projection.shape == projection_array[0].shape
            # 4 channels - rgba
            rgb_out = np.expand_dims(projection, 2)
            rgb_out = np.repeat(rgb_out, 4, 2).astype('float')
            # inject color.  careful of type mismatches.
            rgb_out *= self.colors[i] + [1.0]
            # normalize contrast
            rgb_out /= np.max(rgb_out)
            rgb_out = rgb_out.transpose((2, 1, 0))
            lower_threshold, upper_threshold = get_thresholds(rgb_out)
            # ignore bright spots
            rgb_out = rgb_out.transpose((2, 1, 0))

            def superimpose(source_pixel, dest_pixel):
                pixel_weight = np.mean(source_pixel)
                if lower_threshold < pixel_weight < upper_threshold:
                    return source_pixel
                else:
                    return dest_pixel

            def alpha_blend(source_pixel, dest_pixel):
                pixel_weight = np.mean(source_pixel)
                if lower_threshold < pixel_weight < upper_threshold:
                    # this alpha value is based on the intensity of the pixel in the channel's original projection
                    alpha = projection[x, y]
                    # premultiplied alpha
                    return source_pixel + (1 - alpha) * dest_pixel
                else:
                    return dest_pixel

            if self.layering_mode == "superimpose":
                layering_method = superimpose
            else:
                layering_method = alpha_blend

            for x in range(rgb_out.shape[0]):
                for y in range(rgb_out.shape[1]):
                    # these slicing methods in C channel are getting the rgb data and ignoring the alpha values
                    src_px = rgb_out[x, y, 0:3]
                    dest_px = layered_image[x, y, 0:3]
                    layered_image[x, y, 0:3] = layering_method(source_pixel=src_px, dest_pixel=dest_px)
                    # temporary to assure alpha is one for all pixels
                    layered_image[x, y, 3] = 1.0

        return layered_image.transpose((2, 1, 0))

    def make_thumbnail(self, image, apply_cell_mask=False):
        """
        This method is the primary interface with the ThumbnailGenerator. It can be used many times with different images,
        in order to save the configuration that was specified at the beginning of the generator.

        :param image: single ZCYX image that is the source of the thumbnail
        :param apply_cell_mask: boolean value that designates whether the image is a fullfield or segmented cell
                                False -> fullfield, True -> segmented cell
        :return: a single CYX image, scaled down to the size designated in the constructor
        """

        # check to make sure there are 6 or more channels
        assert image.shape[1] >= 6
        assert max(self.memb_index, self.struct_index, self.nuc_index) <= image.shape[1] - 1

        im_size = np.array(image[:, 0].shape)
        assert len(im_size) == 3
        shape_out_rgb = self._get_output_shape(im_size)

        image = image.astype(np.float32)

        if apply_cell_mask:
            # apply the cell segmentation mask.  bye bye to data outside the cell
            for i in self.channel_indices:
                image[:, i] = mask_image(image[:, i], image[:, self.seg_indices[1]])

        # ignore trans-light channel and seg channels
        image = image[:, 0:3]
        num_noise_floor_bins = 256
        projection_array = []
        projection_type = self.projection_mode
        for i in self.channel_indices:
            # don't use max projections on the fullfield images... they get too messy
            if not apply_cell_mask:
                projection_type = 'slice'
            # subtract out the noise floor.
            thumb = subtract_noise_floor(image[:, i], bins=num_noise_floor_bins)
            thumb = np.asarray(thumb).astype('double')
            # TODO thresholding is too high for the max projection of membrane
            im_proj = create_projection(thumb, 0, projection_type, slice_index=int(thumb.shape[0] // 2), sections=self.proj_sections)
            projection_array.append(im_proj)

        layered_image = self._layer_projections(projection_array)
        comp = resize_image(layered_image, shape_out_rgb)
        comp /= np.max(comp)
        comp[comp < 0] = 0
        # returns a CYX array for the png writer
        return comp


def main():
    # python interleave.py --path /Volumes/aics/software_it/danielt/images/AICS/alphactinin/ --prefix img40_1
    parser = argparse.ArgumentParser(description='Generate thumbnail from a cell image. '
                                                 'Example: python thumbnailGenerator.py /path/to/images/myImg.ome.tif 0 1 2 3')
    parser.add_argument('--path', required=True, help='input file path')
    parser.add_argument('--dna', required=True, type=int, help='dna channel index')
    parser.add_argument('--mem', required=True, type=int, help='membrane channel index')
    parser.add_argument('--str', required=True, type=int, help='structure channel index')

    # assume segmentation mask is last channel, by default .
    parser.add_argument('--seg', default=-1, type=int, help='segmentation channel index')

    parser.add_argument('--size', default=128, type=int, help='maximum edge size of image')
    parser.add_argument('--outpath', default='./', help='output file path (directory only)')
    # parser.add_argument('--prefix', nargs=1, help='input file name prefix')
    args = parser.parse_args()

    inpath = args.path

    seg_channel_index = args.seg

    if os.path.isfile(inpath):
        reader = omeTifReader.OmeTifReader(inpath)
        im1 = reader.load()
    else:
        raise 'Bad file path ' + inpath

    base = os.path.basename(inpath)
    # strips away .tif
    base = os.path.splitext(base)[0]
    # strips away .ome (?)
    base = os.path.splitext(base)[0]

    if not os.path.exists(args.outpath):
        os.makedirs(args.outpath)
    image_out = os.path.join(args.outpath, base + '.png')

    # transpose xycz to xyzc
    assert len(im1.shape) == 4
    im1 = np.transpose(im1, (1, 0, 2, 3))

    comp = make_segmented_thumbnail(im1, channel_indices=[args.dna, args.mem, args.str], size=args.size,
                                    seg_channel_index=seg_channel_index)

    pngwriter = pngWriter.PngWriter(image_out)
    pngwriter.save(comp)


if __name__ == "__main__":
    print(" ".join(sys.argv))
    main()
    sys.exit(0)
