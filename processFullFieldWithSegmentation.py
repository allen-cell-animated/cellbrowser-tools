#!/usr/bin/env python

# author: Zach Crabtree zacharyc@alleninstitute.org

import json
import cellJob
from thumbnail2 import *
from aicsimagetools import *

_colors = [[1.0, 1.0, 0.0], [1.0, 0.0, 1.0], [0.0, 1.0, 1.0]]


# makes the nucleus a more solid image, less holes in picture
def make_consistent_nucleus_image(row):
    print('helloworld')
    # get segmentation channel from row(passed in)
    # pass in slice of nucleus channel (zyx image)
    # mask image with segmentation objects (obliterate all noise data)
    # scale all remaining data up by some ratio - perhaps add avg to values that aren't 0


def make_full_field_thumbnail(im1, memb_index=0, struct_index=1, nuc_index=2,
                              colors=_colors, size=128):
    # assume all images have same shape!
    imsize = np.array(im1[0].shape)

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
        # if the membrane channel is being manipulated
        im_proj = matproj(imdbl, 0, 'slice', slice_index=int(thumb.shape[0] // 2))
        if i == nuc_index:
            average = np.average(im_proj)
            peaks = im_proj > average
            # TODO: GET THIS TO WORK
            # impmax = im_proj.max()
            # im_proj[peaks] *= 4.5

        rgb_image[i] = im_proj

    # TODO: Should these be parameters for this function?
    channel_contrasts = [15.0, 10.0, 10.0]
    mem_avg = np.average(rgb_image[memb_index])
    nuc_avg = np.average(rgb_image[nuc_index])
    struct_avg = np.average(rgb_image[struct_index])

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

    return comp


# TODO: Should these indices really be passed like this?
# On one hand, this clears up which indices are being passed per function
# If they're in an array, it's a little confusing to have indices[1] = 2 or something.
def _generate_png(image, memb_index=0, nuc_index=1, struct_index=2, image_path="test.png"):
    # r,g,b = [1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]
    c, m, y = [0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]
    thumbnail = make_full_field_thumbnail(image,
                                          memb_index=memb_index, nuc_index=nuc_index, struct_index=struct_index,
                                          colors=[c, m, y], size=128)
    thumbnail = np.transpose(thumbnail, (2, 0, 1))
    with pngWriter.PngWriter(image_path, overwrite_file=True) as writer:
        writer.save(thumbnail)


def _generate_ome_tif(image, image_path="test.ome.tif"):
    with omeTifWriter.OmeTifWriter(file_path=image_path, overwrite_file=True) as writer:
        writer.save(image)


# TODO: Change these parameters to make this method more versatile
def generate_images(image, row):
    # This assumes T = 1
    # This omits the transmitted light channel
    no_tlight = image[0:3, :, :, :]
    image = image.transpose(1, 0, 2, 3)
    # this generates a file name identical to the original czi with a png extension
    png_extension = os.path.splitext(row.inputFilename)[0] + ".png"
    output_path = os.path.join(row.cbrThumbnailLocation, png_extension)
    print("generating png...")
    _generate_png(no_tlight,
                  memb_index=row.memChannel-1, nuc_index=row.nucChannel-1, struct_index=row.structureChannel-1,
                  image_path=output_path)
    ome_tif_extension = os.path.splitext(row.inputFilename)[0] + ".ome.tif"
    output_path = os.path.join(row.cbrThumbnailLocation, ome_tif_extension)
    print("generating ometif...")
    _generate_ome_tif(image, output_path)
