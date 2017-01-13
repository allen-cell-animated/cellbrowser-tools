#!/usr/bin/env python

# author: Zach Crabtree zacharyc@alleninstitute.org

import json
import cellJob
from thumbnail2 import *
from aicsimagetools import *


def make_full_field_thumbnail(im1, channel_indices=[0, 1, 2],
                              colors=[[1.0, 1.0, 0.0], [1.0, 0.0, 1.0], [0.0, 1.0, 1.0]], size=128):

    # assume all images have same shape!
    imsize = np.array(im1[0].shape)
    assert len(imsize) == 3
    memb_chan = channel_indices[0]
    dna_chan = channel_indices[1]
    struct_chan = channel_indices[2]

    # size down to this edge size, maintaining aspect ratio.
    # note that this resizing results in all cell thumbnails being about the same size
    max_edge = size
    # keep same number of z slices.
    shape_out = np.hstack((imsize[0],
                           max_edge if imsize[1] > imsize[2] else max_edge*imsize[1]/imsize[2],
                           max_edge if imsize[1] < imsize[2] else max_edge*imsize[2]/imsize[1]
                           ))
    shape_out_rgb = (shape_out[1], shape_out[2], 3)

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

        imdbl = np.asarray(thumb).astype('double')

        # if the membrane channel is being manipulated
        if i == memb_chan:
            # do projection
            # the slicing projection for the memb_chan keeps it from smearing out due to the top and bottom slices
            im_proj = matproj(imdbl, 0, 'slice', slice_index=int(thumb.shape[0] // 2))
            # take the average of all values
            average = np.average(im_proj)
            # print(average)
            # find each index where the value is greater than the index
            # (this allows the background to be excluded from the contrast manipulation)
            peaks = im_proj > average
            # multiply each peak by an arbitrary scalar value
            im_proj[peaks] *= (average + im_proj.max() * 2)
        elif i == dna_chan:
            # the max projection for the other channels keeps the nuclei from appearing holey
            # and the structures from being too tiny
            im_proj = matproj(imdbl, 0, 'max')
        elif i == struct_chan:
            im_proj = matproj(imdbl, 0, 'slice', slice_index=int(thumb.shape[0] // 2))

        # turn into RGB
        im_proj = np.expand_dims(im_proj, 2)
        im_proj = np.repeat(im_proj, 3, 2).astype('float')

        # inject color.  careful of type mismatches.
        im_proj *= colors[i]

        # resize before projection?
        # thumb = imresize(thumb, shape_out)
        rgbproj = imresize(im_proj, shape_out_rgb)
        comp += rgbproj

    # comp += np.average(comp)
    # comp[:, :, 2] /= average
    return comp


def generate_png(image, channel_indices=[0, 1, 2], image_path="test.png"):
    # r,g,b = [1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]
    c, m, y = [0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]
    thumbnail = make_full_field_thumbnail(image, channel_indices=channel_indices, colors=[c, y, m], size=512)
    thumbnail = np.transpose(thumbnail, (2, 0, 1))
    PngWriter(image_path).save(thumbnail, overwrite_file=True)


def generate_images(row):
    full_path = os.path.join(row.inputFolder, row.inputFilename)
    channel_indices = (np.array([row.memChannel, row.nucChannel, row.structureChannel]) - 1).tolist()

    with cziReader.CziReader(full_path) as reader:
        image = reader.load()

    # This assumes T = 1
    image = image.squeeze(0).transpose(1, 0, 2, 3)
    image = image[0:3, :, :, :]
    output_path = os.path.join(row.cbrThumbnailLocation, row.DeliveryDate + ".png")
    generate_png(image, channel_indices, image_path=output_path)


def do_main(fname):
    # extract json to dictionary.
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

    generate_images(info)


def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, and prepare for ingest into bisque db.'
                                                 'Example: python processImageWithSegmentation.py /path/to/csv --outpath /path/to/destination/dir')
    parser.add_argument('input', help='input json file')
    args = parser.parse_args()

    do_main(args.input)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)