import aicsimageio
from aicsimageprocessing import textureAtlas
from aicsimageprocessing import thumbnailGenerator
import datasetdatabase as dsdb

import argparse
import glob
import json
import labkey
import numpy
import os
import pandas as pd
from PIL import Image
from PIL import ImageFont
from PIL import ImageDraw
import errno


def make_one_thumbnail(infile, outfile, channels, colors, size, projection='max', axis=2, apply_mask=False, mask_channel=0, label=''):
    axistranspose = (1, 0, 2, 3)
    if axis == 2:  # Z
        axistranspose = (1, 0, 2, 3)
    elif axis == 0:  # X
        axistranspose = (2, 0, 1, 3)
    elif axis == 1:  # Y
        axistranspose = (3, 0, 2, 1)

    image = aicsimageio.AICSImage(infile)
    imagedata = image.get_image_data()
    generator = thumbnailGenerator.ThumbnailGenerator(channel_indices=channels,
                                                      size=size,
                                                      mask_channel_index=mask_channel,
                                                      colors=colors,
                                                      projection=projection)
    # take zeroth time, and transpose z and c
    ffthumb = generator.make_thumbnail(imagedata[0].transpose(axistranspose), apply_cell_mask=apply_mask)
    if label:
        # font_path = "/Windows/Fonts/consola.ttf"
        font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
        font = ImageFont.truetype(font_path, 12)
        img = Image.fromarray(ffthumb.transpose((1, 2, 0)))
        draw = ImageDraw.Draw(img)
        draw.text((2, 2), label, (255, 255, 255), font=font)
        ffthumb = numpy.array(img)
        ffthumb = ffthumb.transpose(2, 0, 1)

    with aicsimageio.PngWriter(file_path=outfile, overwrite_file=True) as writer:
        writer.save(ffthumb)
    return ffthumb


def parse_args():
    parser = argparse.ArgumentParser(description='Make a thumbnail from a ome-tiff')

    parser.add_argument('infile', type=argparse.FileType('r'), help='input zstack')
    parser.add_argument('outfile', type=argparse.FileType('w'), help='output png')

    # assume square for now
    parser.add_argument('--size', type=int, help='size', default=128)
    parser.add_argument('--mask', type=int, help='mask channel', default=-1)
    parser.add_argument('--axis', type=int, help='axis 0, 1, or 2', default=2)
    parser.add_argument('--channels', type=int, nargs='+', help='channels to composite', default=[0])
    parser.add_argument('--colors', type=str, nargs='+', help='colors to composite, one per channel', default=['ffffff'])
    parser.add_argument('--projection', type=str, help='projection type max or slice', default='max')
    parser.add_argument('--label', type=str, help='string label on image', default='')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    colors = [(tuple(int(h[i:i+2], 16)/255.0 for i in (0, 2, 4))) for h in args.colors]
    make_one_thumbnail(
        infile=args.infile,
        outfile=args.outfile,
        channels=args.channels,
        colors=colors,
        size=args.size,
        projection=args.projection,
        axis=args.axis,
        apply_mask=(args.mask != -1),
        mask_channel=args.mask)
