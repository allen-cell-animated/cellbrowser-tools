#!/usr/bin/env python

# author: Zach Crabtree zacharyc@alleninstitute.org
from thumbnail2 import *

_cmy = [[0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]]

# makes the nucleus a more solid image, less holes in picture
# def make_consistent_nucleus_image(row):
#     print('helloworld')
#     # get segmentation channel from row(passed in)
#     # pass in slice of nucleus channel (zyx image)
#     # mask image with segmentation objects (obliterate all noise data)
#     # scale all remaining data up by some ratio - perhaps add avg to values that aren't 0


def make_fullfield_thumbnail(im1, memb_index=0, struct_index=1, nuc_index=2,
                             colors=_cmy, size=128):
    # assume all images have same shape!
    imsize = np.array(im1[0].shape)
    im1 = im1[0:3, :, :, :]

    # TODO: Are these the only asserts we want to try?
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

    num_noise_floor_bins = 16
    comp = np.zeros(shape_out_rgb)
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

        imdbl = np.asarray(thumb).astype('double')
        im_proj = matproj(imdbl, 0, 'slice', slice_index=int(thumb.shape[0] // 2))
        if i == nuc_index:
            average = np.average(im_proj)
            peaks = im_proj > average
            # TODO: GET THIS TO WORK
            impmax = im_proj.max()
            im_proj[peaks] *= 4.5
        # elif i == struct_index:
        #     average = np.average(im_proj)
        #     im_proj -= average
        #     im_proj[im_proj < 0] = 0

        rgb_image[i] = im_proj

    # TODO: Should these be parameters for this function?
    channel_contrasts = [15.0, 10.0, 10.0]

    # TODO: Possibly mask out background noise by finding middle point between min and avg and zeroing lower values
    # TODO: Can these for loops be condensed?
    for channel in channel_indices:
        # normalize the channel to values from 0 to 1
        rgb_image[channel] /= np.max(rgb_image[channel])
        # scale the whole channel to a max equivalent to the correct contrast ratio
        rgb_image[channel] *= channel_contrasts[channel]

    for i in range(rgb_image.shape[0]):
        # turn into RGB
        rgb_out = np.expand_dims(rgb_image[i], 2)
        rgb_out = np.repeat(rgb_out, 3, 2).astype('float')

        # inject color.  careful of type mismatches.
        rgb_out *= colors[i]

        rgb_out = imresize(rgb_out, shape_out_rgb)
        comp += rgb_out

    # returns a CXY array for the pngwriter
    return comp.transpose((2, 0, 1))
