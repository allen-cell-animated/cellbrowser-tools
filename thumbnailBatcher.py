import argparse
import sys
from os import listdir, makedirs
from os.path import isfile, join, exists
import re
import random
import thumbnailGenerator
from glob import iglob
from aicsimagetools import pngWriter, omeTifReader

# Author: Zach Crabtree zacharyc@alleninstitute.org

_cmy = ([[0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]], "cmy")
_cym = ([[0.0, 1.0, 1.0], [1.0, 1.0, 0.0], [1.0, 0.0, 1.0]], "cym")
_rgb = ([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]], "rgb")
_rbg = ([[1.0, 0.0, 0.0], [0.0, 0.0, 1.0], [0.0, 1.0, 0.0]], "rbg")
_brg = ([[0.0, 0.0, 1.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]], "brg")
color_choices = [_cym, _cmy, _rgb, _rbg, _brg]


def full_fields_color(ome_tif_files, color):

    print("\nprocessing images with " + color[1] + " palette.")

    for file_name in ome_tif_files:
        with omeTifReader.OmeTifReader(file_name) as reader:
            # converts to CZYX
            image = reader.load()[0].transpose((1, 0, 2, 3))
        thumb = thumbnailGenerator.make_fullfield_thumbnail(image, colors=color[0])
        path_as_list = re.split(r'\\|/', file_name)
        new_path = path_as_list[:-2]
        new_path.append(color[1])
        new_path.append(path_as_list[len(path_as_list) - 1][:-8] + ".png")
        new_path = join(*new_path)
        if not exists(new_path[:new_path.rfind('/')]):
            makedirs(new_path[:new_path.rfind('/')])
        with pngWriter.PngWriter(new_path, overwrite_file=True) as writer:
            writer.save(thumb)


def main():
    parser = argparse.ArgumentParser(description="Create new batches of thumbnails for testing")

    parser.add_argument('input', help="directory to search in")
    parser.add_argument('--random', '-r', type=int, help="generate thumbnails for random ometifs", default=5)

    args = parser.parse_args()
    rand_max = args.random

    only_ome_tif = [f for f in iglob(join(args.input, "**", "*.ome.tif"))]
    print(only_ome_tif)

    if rand_max is not None:
        if len(only_ome_tif) <= rand_max:
            random_file_list = only_ome_tif
        else:
            random_file_list = only_ome_tif[:rand_max]
            k = 1
            for ome_tif in only_ome_tif:
                if random.uniform(0, rand_max) > rand_max / 2:
                    random_file_list [int(random.uniform(0, rand_max))] = ome_tif
                k += 1
        file_list = random_file_list
    else:
        file_list = only_ome_tif
    # TODO: RGB palettes get too much cut out, the threshold is much too high
    for color in color_choices:
        full_fields_color(file_list, color=color)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
