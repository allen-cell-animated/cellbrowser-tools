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
        if i == memb_index or struct_index:
            # the slicing projection for the memb_chan keeps it from smearing out from the top and bottom slices
            # the slicing projection for struct_chan keeps it from becoming too scattered and dim
            im_proj = matproj(imdbl, 0, 'slice', slice_index=int(thumb.shape[0] // 2))
        elif i == nuc_index:
            # the max projection for the dna_chan keeps the nuclei from appearing holey
            im_proj = matproj(imdbl, 0, 'max')

        rgb_image[i] = im_proj

    # TODO: Should these be parameters for this function?
    channel_contrasts = [15.0, 15.0, 10.0]

    # TODO: Can these for loops be condensed?
    for channel in channel_indices:
        # normalize the channel to values from 0 to 1
        rgb_image[channel] /= np.max(rgb_image[channel])
        # scale the whole channel to a max equivalent the correct contrast ratio
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
def generate_png(image, memb_index=0, nuc_index=1, struct_index=2, image_path="test.png"):
    # r,g,b = [1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]
    c, m, y = [0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]
    thumbnail = make_full_field_thumbnail(image,
                                          memb_index=memb_index, nuc_index=nuc_index, struct_index=struct_index,
                                          colors=[c, m, y], size=512)
    thumbnail = np.transpose(thumbnail, (2, 0, 1))
    PngWriter(image_path).save(thumbnail, overwrite_file=True)


def generate_images(row):
    full_path = os.path.join(row.inputFolder, row.inputFilename)
    with cziReader.CziReader(full_path) as reader:
        image = reader.load()
    # This assumes T = 1
    image = image.squeeze(0).transpose(1, 0, 2, 3)
    # This omits the transmitted light channel
    image = image[0:3, :, :, :]
    # this generates a file name identical to the original czi with a png extension
    png_extension = os.path.splitext(row.inputFilename)[0] + ".png"
    output_path = os.path.join(row.cbrThumbnailLocation, png_extension)
    generate_png(image,
                 memb_index=row.memChannel-1, nuc_index=row.nucChannel-1, struct_index=row.structureChannel-1,
                 image_path=output_path)


def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, and prepare for ingest into bisque db.'
                                                 'Example: python processImageWithSegmentation.py /path/to/csv --outpath /path/to/destination/dir')
    parser.add_argument('input', help='input json file')
    args = parser.parse_args()
    # extract json to dictionary.
    with open(args.input) as jobfile:
        jobspec = json.load(jobfile)
        info = cellJob.CellJob(jobspec)

    """
        jobspec is expected to be a dictionary of:
         ,DeliveryDate,Version,inputFolder,inputFilename,
         xyPixelSize,zPixelSize,memChannel,nucChannel,structureChannel,structureProteinName,
         lightChannel,timePoint,
         outputSegmentationPath,outputNucSegWholeFilename,outputCellSegWholeFilename,
         structureSegOutputFolder,structureSegOutputFilename,
        image_db_location,
        thumbnail_location
    """

    generate_images(info)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
