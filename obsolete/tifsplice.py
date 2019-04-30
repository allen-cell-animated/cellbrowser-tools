#!/usr/bin/env python

# author: Dan Toloudis danielt@alleninstitute.org

from __future__ import print_function

from aicsimageio.tifReader import TifReader
from aicsimageio.omeTifWriter import OmeTifWriter

import numpy as np
import os
import sys

def do_main_image():
    # ASSUMES ALL TIFS HAVE SAME SHAPE

    inputdir = '\\\\allen\\aics\\modeling\\cheko\\projects\\nucleus_predictor\\test_output\\2017-08-16_graham\\'
    file_list = [
        'img_00_bf.tif',
        'img_00_fibrillarin.tif',
        'img_00_lamin_b1.tif',
        'img_00_tom_20.tif'
    ]
    colors = [
        [128, 128, 128],
        [255, 0, 0],
        [0, 255, 0],
        [0, 0, 255]
    ]
    pixel_size = [0.29, 0.29, 0.29]

    image = None
    for f in file_list:
        file_ext = os.path.splitext(f)[1]
        if file_ext == '.tiff' or file_ext == '.tif':
            seg = TifReader(inputdir + f).load()
            # seg is expected to be TZCYX where T and C are 1
            # append channels
            # axis=0 is the C axis
            dat = seg[0,:,0,:,:]

            if image is None:
                image = np.empty(shape=(0, *dat.shape))
            image = np.append(image, [dat], axis=0)
        else:
            raise ValueError("Image is not a tiff segmentation file!")

    transposed_image = image.transpose(1, 0, 2, 3)
    print("saving image...", end="")
    with OmeTifWriter(file_path=inputdir+'full_image.ome.tif', overwrite_file=True) as writer:
        writer.save(transposed_image,
                    channel_names=file_list, channel_colors=colors,
                    pixels_physical_size=pixel_size)


def main():
    do_main_image()


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
