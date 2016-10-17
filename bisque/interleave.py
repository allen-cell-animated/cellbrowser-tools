#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org

from aicsimagetools import *
import argparse
import numpy as np
import os
import sys


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


def display(image, display_min, display_max):  # copied from Bi Rico
    # Here I set copy=True in order to ensure the original image is not
    # modified. If you don't mind modifying the original image, you can
    # set copy=False or skip this step.
    image = np.array(image, copy=True)
    image.clip(display_min, display_max, out=image)
    image -= display_min
    image = np.floor_divide(image, (display_max - display_min + 1) / 256.)
    return image.astype(np.uint8)


def lut_16_to_8(image, display_min, display_max):
    lut = np.arange(2**16, dtype='uint16')
    lut = display(lut, display_min, display_max)
    return np.take(lut, image)


def main():
    # python interleave.py --path /Volumes/aics/software_it/danielt/images/AICS/alphactinin/ --prefix img40_1
    parser = argparse.ArgumentParser(description='Interleave tiff files as channels in a single tiff. '
                                     'Example: python interleave.py --path /path/to/images/alphactinin/ '
                                     '--prefix img40_1')
    parser.add_argument('--path', required=True, help='input path (directory only)')
    parser.add_argument('--prefix', required=True, help='input file name prefix. Expects prefix_channelname.tif')
    parser.add_argument('--outpath', default='./', help='output file path (directory only)')
    # parser.add_argument('--prefix', nargs=1, help='input file name prefix')
    args = parser.parse_args()

    inpath = args.path
    inseries = args.prefix
    channels = ['dna', 'memb', 'struct', 'seg_nuc', 'seg_cell']
    tifext = '.tif'
    physical_size = [0.065, 0.065, 0.29]
    channel_colors = [
        rgba255(255, 255, 255, 255),
        rgba255(255, 0, 255, 255),
        rgba255(0, 255, 255, 255),
        rgba255(255, 0, 0, 255),
        rgba255(0, 0, 255, 255)
    ]

    # dictionary of channelname:fullpath
    image_paths_in = {}
    for i in channels:
        fullpath = os.path.join(inpath, inseries + '_' + i + tifext)
        if os.path.isfile(fullpath):
            image_paths_in[i] = fullpath

    image_out = os.path.join(args.outpath, inseries + '.ome' + tifext)

    try:
        readers = []

        assert len(image_paths_in) == len(channels)
        # open each file that we are going to interleave
        for i, channelName in enumerate(channels):
            image_path_in = image_paths_in.get(channelName)
            if image_path_in is None:
                raise Exception('Missing channel file for channel ' + channelName)

            readers.append(TifReader(image_path_in))

        assert len(readers) == len(channels)

        # do the interleaving, reading one slice at a time from each of the single channel tifs
        d = np.ndarray([readers[0].size_z(), len(channels), readers[0].size_y(), readers[0].size_x()],
                       dtype=readers[0].dtype())
        for i in range(readers[0].size_z()):
            for j in range(len(readers)):
                # convert to 8 bit here!
                a = readers[j].load_image(z=i)
                if a.dtype.name == 'uint16':
                    d[i, j, :, :] = lut_16_to_8(a, a.min(), a.max())
                else:
                    # assumes 8 bit here.
                    d[i, j, :, :] = a
                # d[i, j, :, :] = readers[j].load_image(z=i)

        try:
            os.remove(image_out)
            print('previous output file deleted')
        except:
            print('no output file to delete')

        writer = omeTifWriter.OmeTifWriter(image_out)
        writer.save(d, channel_names=[x.upper() for x in channels],
                    pixels_physical_size=physical_size, channel_colors=channel_colors)
        writer.close()
        print('Completed ' + image_out)

    except Exception as inst:
        print(inst)

if __name__ == "__main__":
    print sys.argv
    main()
    sys.exit(0)
