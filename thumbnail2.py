#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org

from aicsimagetools import *
import argparse
import numpy as np
import os
import scipy
import sys
from skimage.measure import block_reduce
import skimage.transform as t

z_axis_index = 0
_cmy = [[0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]]


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

    im_all = np.zeros(np.hstack((sx+sz, sy+sz, 3)))
    # imz is xXy
    im_all[0:sx, sz:] = projz
    # imy is zXx (so transpose it)
    im_all[0:sx, 0:sz] = np.transpose(projy, (1, 0, 2))
    # imx is zXy
    im_all[sx:, sz:] = projx

    if rescale_inten:
        im_all = im_all / np.max(im_all.flatten())

    return im_all

# # max, sum, min, mean, inv_sum
# def generate_thumbnail(w,h, src_img, colors, slices, projection_axis, projection_type='max'):
#     shape = src_img.shape
#
#     make_rgb_proj(imxyz, axis, color, method='max', rescale_inten=True):
#
#     # do resizing last!
#
#     return img


# see http://www.somersault1824.com/tips-for-designing-scientific-figures-for-color-blind-readers/
# or http://mkweb.bcgsc.ca/biovis2012/color-blindness-palette.png
# colors = [
#     [0.0/255.0, 109.0/255.0, 219.0/255.0],
#     [36.0/255.0, 255.0/255.0, 36.0/255.0],
#     [255.0/255.0, 109.0/255.0, 182.0/255.0]
# ]
# pass in a xyzc image!
def make_segmented_thumbnail(im1, channel_indices=[0, 1, 2], colors=_cmy,
                             seg_channel_index=-1, size=128):

    # assume all images have same shape!
    imsize = np.array(im1[0].shape)
    assert len(imsize) == 3

    # size down to this edge size, maintaining aspect ratio.
    # note that this resizing results in all cell thumbnails being about the same size
    max_edge = size
    # keep same number of z slices.
    shape_out = np.hstack((imsize[0],
                           max_edge if imsize[1] > imsize[2] else max_edge*imsize[1]/imsize[2],
                           max_edge if imsize[1] < imsize[2] else max_edge*imsize[2]/imsize[1]
                           ))
    shape_out_rgb = (shape_out[1], shape_out[2], 3)

    # apply the cell segmentation mask.  bye bye to data outside the cell
    for i in range(im1.shape[0]):
        im1[i, :, :, :] = mask_image(im1[i, :, :, :], im1[seg_channel_index, :, :, :])
    # im1 = [mask_image(im, im1[seg_channel_index]) for im in im1]
    mask = matproj(im1[seg_channel_index], z_axis_index)
    # pngwriter = pngWriter.PngWriter('test/oMask.png')
    # pngwriter.save(mask)

    num_noise_floor_bins = 16
    comp = np.zeros(shape_out_rgb)
    for i in range(3):
        ch = channel_indices[i]
        # try to subtract out the noise floor.
        # range is chosen to ignore zeros due to masking.  alternative is to pass mask image as weights=im1[-1]
        immin = im1[ch].min()
        immax = im1[ch].max()
        hi, bin_edges = np.histogram(im1[ch], bins=num_noise_floor_bins, range=(max(1, immin), immax))
        # hi, bin_edges = np.histogram(im1[0], bins=16, weights=im1[-1])
        # index of tallest peak in histogram
        peakind = np.argmax(hi)
        # subtract this out
        thumb = im1[ch].astype(np.float32)
        # channel 0 seems to have a zero noise floor and so the peak of histogram is real signal.
        if i != 0:
            thumb -= bin_edges[peakind]
        # don't go negative
        thumb[thumb < 0] = 0
        # renormalize
        thmax = thumb.max()
        thumb /= thmax

        # resize before projection?
        # thumb = imresize(thumb, shape_out)
        rgbproj = make_rgb_proj(thumb, z_axis_index, colors[i])
        rgbproj = imresize(rgbproj, shape_out_rgb)
        comp += rgbproj
    # renormalize
    # comp /= comp.max()
    return comp


def make_fullfield_thumbnail(im1, memb_index=0, struct_index=1, nuc_index=2,
                             colors=_cmy, size=128):
    # assume all images have same shape!
    imsize = np.array(im1[0].shape)
    im1 = im1[0:3, :, :, :]

    assert len(imsize) == 3
    assert max(memb_index, struct_index, nuc_index) <= im1.shape[0] - 1

    # size down to this edge size, maintaining aspect ratio.
    max_edge = size
    # keep same number of z slices.
    shape_out = np.hstack((imsize[0],
                           max_edge if imsize[1] > imsize[2] else max_edge*imsize[1]/imsize[2],
                           max_edge if imsize[1] < imsize[2] else max_edge*imsize[2]/imsize[1]
                           ))
    shape_out_rgb = (shape_out[1], shape_out[2], 3)

    num_noise_floor_bins = 7

    channel_indices = [memb_index, struct_index, nuc_index]
    rgb_image = im1[:, 0, :, :].astype('float')
    for i in channel_indices:
        # subtract out the noise floor.
        immin = im1[i].min()
        immax = im1[i].max()
        hi, bin_edges = np.histogram(im1[i], bins=num_noise_floor_bins, range=(max(1, immin), immax))
        # index of tallest peak in histogram
        peakind = np.argmax(hi)
        # subtract this out
        thumb = im1[i].astype(np.float32)
        # channel 0 seems to have a zero noise floor and so the peak of histogram is real signal.
        if i != 0:
            thumb -= bin_edges[peakind]
        # don't go negative
        thumb[thumb < 0] = 0
        # renormalize
        thmax = thumb.max()
        thumb /= thmax

        # thresh = np.mean(thumb)
        # thumb[thumb < thresh] = 0

        imdbl = np.asarray(thumb).astype('double')
        im_proj = matproj(imdbl, 0, 'slice', slice_index=int(thumb.shape[0] // 2))

        rgb_image[i] = im_proj

    output_channels = []
    channel_name = ["MEM", "STRUCT", "NUC"]
    downscale_factor = (im1.shape[3] / size)
    inter = np.zeros([1024, 1024, 3])
    for i in range(rgb_image.shape[0] - 1, -1, -1):
        # turn into RGB
        rgb_out = np.expand_dims(rgb_image[i], 2)
        rgb_out = np.repeat(rgb_out, 3, 2).astype('float')

        # inject color.  careful of type mismatches.
        rgb_out *= colors[i]

        # normalize contrast
        rgb_out /= np.max(rgb_out)

        nonzeros = rgb_out[np.nonzero(rgb_out)]
        print("Max: " + str(np.max(nonzeros)))
        print("Min: " + str(np.min(nonzeros)))
        print("Median: " + str(np.median(nonzeros)))
        print("Mean: " + str(np.mean(nonzeros)))
        threshold = np.mean(nonzeros) - (np.median(nonzeros) / 2)
        print("Threshold: " + str(threshold))

        channel_inter = np.zeros([1024, 1024, 3])
        for x in range(rgb_out.shape[0]):
            for y in range(rgb_out.shape[1]):
                summation = rgb_out[x, y].sum() / rgb_out.shape[2]
                if summation > threshold:
                    inter[x, y] = rgb_out[x, y]
                    channel_inter[x, y] = rgb_out[x, y]

        channel = t.pyramid_reduce(channel_inter, downscale=downscale_factor)
        # TODO: This assumes the image is always square, is this actually the case?
        output_channels.append((np.transpose(channel, (2, 0, 1)), channel_name[i]))
    # returns a CYX array for the pngwriter
    comp = t.pyramid_reduce(inter, downscale=downscale_factor)
    return comp.transpose((2, 0, 1)), output_channels


def main():
    # python interleave.py --path /Volumes/aics/software_it/danielt/images/AICS/alphactinin/ --prefix img40_1
    parser = argparse.ArgumentParser(description='Generate thumbnail from a cell image. '
                                     'Example: python thumbnail2.py /path/to/images/myImg.ome.tif 0 1 2 3')
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

    im1 = []

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
    im1 = np.transpose(im1, (1,0,2,3))

    comp = make_segmented_thumbnail(im1, channel_indices=[args.dna, args.mem, args.str], size=args.size, seg_channel_index=seg_channel_index)

    pngwriter = pngWriter.PngWriter(image_out)
    pngwriter.save(comp)

if __name__ == "__main__":
    print " ".join(sys.argv)
    main()
    sys.exit(0)
