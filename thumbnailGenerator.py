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


def get_luminance(array):
    assert len(array) == 3
    return np.sum(array * [.299, .587, .114])


def _get_threshold(image):
    # using this allows us to ignore the bright corners of a cell image
    border_percent = 0.1
    im_width = image.shape[0]
    im_height = image.shape[1]
    left_bound = int(m.floor(border_percent * im_width))
    right_bound = int(m.ceil((1 - border_percent) * im_width))
    bottom_bound = int(m.floor(border_percent * im_height))
    top_bound = int(m.ceil((1 - border_percent) * im_height))

    cut_border = image[left_bound:right_bound, bottom_bound:top_bound]
    nonzeros = cut_border[np.nonzero(cut_border)]
    print("\nMax: " + str(np.max(nonzeros)))
    print("Min: " + str(np.min(nonzeros)))
    upper_threshold = np.max(cut_border) * .998

    print("Median: " + str(np.median(nonzeros)))
    print("Mean: " + str(np.mean(nonzeros)))
    lower_threshold = np.mean(nonzeros) - (np.median(nonzeros) / 3)

    return lower_threshold, upper_threshold


def imresize(im, new_size):
    new_size = np.array(new_size).astype('double')
    old_size = np.array(im.shape).astype('double')

    zoom_size = np.divide(new_size, old_size)
    # precision?
    im_out = scipy.ndimage.interpolation.zoom(im, zoom_size)

    return im_out


def mask_image(im, mask):
    im_masked = np.multiply(im, mask > 0)
    return im_masked


def matproj(im, dim, method='max', slice_index=0):
    if method == 'max':
        im = np.max(im, dim)
    elif method == 'mean':
        im = np.mean(im, dim)
    elif method == 'sum':
        im = np.sum(im, dim)
    elif method == 'slice':
        im = im[slice_index, :, :]

    # returns 2D image, YX
    return im


def make_rgb_proj(imxyz, axis, color, method='max', rescale_inten=True, slice_index=0):
    imdbl = np.asarray(imxyz).astype('double')
    # do projection
    im_proj = matproj(imdbl, axis, method, slice_index=slice_index)

    # turn into RGB
    im_proj = np.expand_dims(im_proj, 2)
    im_proj = np.repeat(im_proj, 3, 2)

    # inject color.  careful of type mismatches.
    im_proj[:, :, 0] *= color[0]
    im_proj[:, :, 1] *= color[1]
    im_proj[:, :, 2] *= color[2]

    # if rescale_inten:
    #     maxval = np.max(im_proj.flatten())
    #     im_proj = im_proj / maxval

    return im_proj


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


class ThumbnailGenerator:

    def __init__(self, colors=_cmy, size=128,
                 memb_index=0, struct_index=1, nuc_index=2,
                 memb_seg_index=5, struct_seg_index=6, nuc_seg_index=4,
                 layering="superimpose"):

        assert len(colors) == 3 and len(colors[0]) == 3
        self.colors = colors

        self.size = size
        self.memb_index, self.struct_index, self.nuc_index = memb_index, struct_index, nuc_index
        self.channel_indices = [self.memb_index, self.struct_index, self.nuc_index]
        self.seg_indices = [nuc_seg_index, memb_seg_index, struct_seg_index]

        assert layering == "superimpose" or layering == "alpha-blend"
        self.layering_mode = layering

    def _get_output_shape(self, im_size):
        # size down to this edge size, maintaining aspect ratio.
        max_edge = self.size
        # keep same number of z slices.
        shape_out = np.hstack((im_size[0],
                               max_edge if im_size[1] > im_size[2] else max_edge * im_size[1] / im_size[2],
                               max_edge if im_size[1] < im_size[2] else max_edge * im_size[2] / im_size[1]
                               ))
        return shape_out[1], shape_out[2], 3

    def _layer_projections(self, projection_array):
        # array cannot be empty or have more channels than the color array
        assert projection_array
        assert len(projection_array) == len(self.colors)
        layered_image = np.zeros((projection_array[0].shape[0], projection_array[0].shape[1], 3))
        print("layering channels...", end=" ")
        for i in range(len(projection_array)):
            print(i, end=" ")
            projection = projection_array[i]
            assert projection.shape == projection_array[0].shape
            if self.layering_mode == "alpha-blend":
                # 4 channels - rgba
                projection /= np.max(projection)
                rgba_out = np.repeat(np.expand_dims(projection, 2), 4, 2).astype('float')
                # rgb values for the color palette + initial alpha value
                rgba_vals = self.colors[i] + [1.0]
                rgba_out *= rgba_vals

                lower_threshold, upper_threshold = _get_threshold(projection)
                cutout = 0
                total = float(layered_image.shape[0] * layered_image.shape[1])
                # blending step
                for x in range(layered_image.shape[0]):
                    for y in range(layered_image.shape[1]):
                        rgb_new = rgba_out[x, y, 0:3]
                        pixel_weight = np.mean(rgb_new)
                        if lower_threshold < pixel_weight < upper_threshold or i == 2:
                            rgb_old = layered_image[x, y]
                            alpha = projection[x, y]
                            if alpha > 1:
                                alpha = 1
                            elif alpha < 0:
                                alpha = 0
                            # premultiplied alpha
                            final_val = rgb_new + (1 - alpha) * rgb_old
                            layered_image[x, y] = final_val
                        else:
                            cutout += 1.0
                            continue

                print("Total cut out: " + str((cutout / total) * 100.0) + "%")

            elif self.layering_mode == "superimpose":
                # 3 channels - rgb
                rgb_out = np.expand_dims(projection, 2)
                rgb_out = np.repeat(rgb_out, 3, 2).astype('float')
                # inject color.  careful of type mismatches.
                rgb_out *= self.colors[i]
                # normalize contrast
                rgb_out /= np.max(rgb_out)
                lower_threshold, upper_threshold = _get_threshold(rgb_out)
                # ignore bright spots
                print("Thresholds: " + str((lower_threshold, upper_threshold)))

                total = float((rgb_out.shape[0] * rgb_out.shape[1]))
                cutout, low_cut, high_cut = 0.0, 0.0, 0.0
                for x in range(rgb_out.shape[0]):
                    for y in range(rgb_out.shape[1]):
                        pixel_weight = np.mean(rgb_out[x, y])
                        if lower_threshold < pixel_weight < upper_threshold or i == 2:
                            layered_image[x, y] = rgb_out[x, y]
                        else:
                            cutout += 1.0

                print("Total cut out: " + str((cutout / total) * 100.0) + "%")

        print("done")
        return layered_image

    def make_thumbnail(self, image, apply_cell_mask=False):

        assert image.shape[0] >= 6
        assert max(self.memb_index, self.struct_index, self.nuc_index) <= image.shape[0] - 1

        im_size = np.array(image[0].shape)
        assert len(im_size) == 3
        shape_out_rgb = self._get_output_shape(im_size)

        image = image.astype('float')

        if apply_cell_mask:
            # apply the cell segmentation mask.  bye bye to data outside the cell
            for i in self.channel_indices:
                image[i] = mask_image(image[i], image[self.seg_indices[1]])

        image = image[0:3]
        num_noise_floor_bins = 256
        downscale_factor = (image.shape[3] / self.size) if image.shape[3] > image.shape[2] else (image.shape[2] / self.size)
        projection_array = []
        projection_type = 'slice'
        for i in self.channel_indices:
            if apply_cell_mask and (i == self.memb_index or i == self.struct_index):
                projection_type = 'max'
            # subtract out the noise floor.
            immin = image[i].min()
            immax = image[i].max()
            hi, bin_edges = np.histogram(image[i], bins=num_noise_floor_bins, range=(max(1, immin), immax))
            # index of tallest peak in histogram
            peakind = np.argmax(hi)
            # subtract this out
            thumb = image[i].astype(np.float32)
            thumb -= bin_edges[peakind]
            # don't go negative
            thumb[thumb < 0] = 0

            imdbl = np.asarray(thumb).astype('double')
            # TODO implement max proj of three sections of the cell
            # TODO thresholding is too high for the max projection of membrane
            im_proj = matproj(imdbl, 0, projection_type, slice_index=int(thumb.shape[0] // 2))
            projection_array.append(im_proj)

        layered_image = self._layer_projections(projection_array)

        try:
            # if images need to get bigger instead of smaller, this will fail
            comp = t.pyramid_reduce(layered_image, downscale=downscale_factor)
        except ValueError:
            # TODO some segmented images are too large because they are not square
            comp = imresize(layered_image, shape_out_rgb)
        comp /= np.max(comp)
        comp[comp < 0] = 0
        # returns a CYX array for the png writer
        return comp.transpose((2, 0, 1))


# colors = [
#     [0.0/255.0, 109.0/255.0, 219.0/255.0],
#     [36.0/255.0, 255.0/255.0, 36.0/255.0],
#     [255.0/255.0, 109.0/255.0, 182.0/255.0]
# ]
# pass in a xyzc image!
def make_segmented_thumbnail(im1, channel_indices=[0, 1, 2], colors=_cmy,
                             seg_channel_index=-1, size=128):

    return ThumbnailGenerator(memb_index=channel_indices[0], struct_index=channel_indices[1], nuc_index=channel_indices[2],
                              size=size, colors=colors).make_thumbnail(im1)

    #
    # # assume all images have same shape!
    # imsize = np.array(im1[0].shape)
    # assert len(imsize) == 3
    #
    # # size down to this edge size, maintaining aspect ratio.
    # # note that this resizing results in all cell thumbnails being about the same size
    # max_edge = size
    # # keep same number of z slices.
    # shape_out = np.hstack((imsize[0],
    #                        max_edge if imsize[1] > imsize[2] else max_edge * imsize[1] / imsize[2],
    #                        max_edge if imsize[1] < imsize[2] else max_edge * imsize[2] / imsize[1]
    #                        ))
    # shape_out_rgb = (shape_out[1], shape_out[2], 3)
    #
    # # apply the cell segmentation mask.  bye bye to data outside the cell
    # for i in range(im1.shape[0]):
    #     im1[i, :, :, :] = mask_image(im1[i, :, :, :], im1[seg_channel_index, :, :, :])
    # # im1 = [mask_image(im, im1[seg_channel_index]) for im in im1]
    # mask = matproj(im1[seg_channel_index], z_axis_index)
    # # pngwriter = pngWriter.PngWriter('test/oMask.png')
    # # pngwriter.save(mask)
    #
    # num_noise_floor_bins = 16
    # comp = np.zeros(shape_out_rgb)
    # for i in range(3):
    #     ch = channel_indices[i]
    #     # try to subtract out the noise floor.
    #     # range is chosen to ignore zeros due to masking.  alternative is to pass mask image as weights=im1[-1]
    #     immin = im1[ch].min()
    #     immax = im1[ch].max()
    #     hi, bin_edges = np.histogram(im1[ch], bins=num_noise_floor_bins, range=(max(1, immin), immax))
    #     # hi, bin_edges = np.histogram(im1[0], bins=16, weights=im1[-1])
    #     # index of tallest peak in histogram
    #     peakind = np.argmax(hi)
    #     # subtract this out
    #     thumb = im1[ch].astype(np.float32)
    #     # channel 0 seems to have a zero noise floor and so the peak of histogram is real signal.
    #     if i != 0:
    #         thumb -= bin_edges[peakind]
    #     # don't go negative
    #     thumb[thumb < 0] = 0
    #     # renormalize
    #     thmax = thumb.max()
    #     thumb /= thmax
    #
    #     # resize before projection?
    #     # thumb = imresize(thumb, shape_out)
    #     rgbproj = make_rgb_proj(thumb, z_axis_index, colors[i])
    #     rgbproj = imresize(rgbproj, shape_out_rgb)
    #     comp += rgbproj
    # # renormalize
    # # comp /= comp.max()
    # return comp


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
