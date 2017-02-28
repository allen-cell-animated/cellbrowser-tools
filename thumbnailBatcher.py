import argparse
import sys
from os import listdir, makedirs
from os.path import isfile, join, exists
import re
import random
from thumbnailGenerator import ThumbnailGenerator
from glob import iglob
from aicsimagetools.pngWriter import PngWriter
from aicsimagetools.omeTifReader import OmeTifReader

# Author: Zach Crabtree zacharyc@alleninstitute.org

# see http://www.somersault1824.com/tips-for-designing-scientific-figures-for-color-blind-readers/
# or http://mkweb.bcgsc.ca/biovis2012/color-blindness-palette.png
_cmy = ([[0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]], "cmy")
_cym = ([[0.0, 1.0, 1.0], [1.0, 1.0, 0.0], [1.0, 0.0, 1.0]], "cym")
_ymc = ([[1.0, 1.0, 0.0], [1.0, 0.0, 1.0], [0.0, 1.0, 1.0]], "ymc")
_rgb = ([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]], "rgb")
_rbg = ([[1.0, 0.0, 0.0], [0.0, 0.0, 1.0], [0.0, 1.0, 0.0]], "rbg")
_brg = ([[0.0, 0.0, 1.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]], "brg")
color_choices = [_cym, _cmy, _ymc]


def full_fields_color(ome_tif_files, color):

    print("\nprocessing images with " + color[1] + " palette.\n")

    for file_name in ome_tif_files:
        with OmeTifReader(file_name) as reader:
            # converts to CZYX
            image = reader.load()[0].transpose((1, 0, 2, 3))
        print("processing " + file_name + "...")
        # TODO maybe pass in functions instead of parameters for branching blocks
        thumb = ThumbnailGenerator(colors=color[0], threshold="luminance", layering="alpha-blend").make_full_field_thumbnail(image)
        path_as_list = re.split(r'\\|/', file_name)
        new_path = path_as_list[:-2]
        new_path.append(color[1])
        new_path.append(path_as_list[len(path_as_list) - 1][:-8] + ".png")
        new_path = "/home/zacharyc/Development/cellbrowser-tools/dryrun/images/" + color[1] + "/" + path_as_list[len(path_as_list) - 1][:-8] + ".png"
        if not exists(new_path[:new_path.rfind('/')]):
            makedirs(new_path[:new_path.rfind('/')])
        with PngWriter(new_path, overwrite_file=True) as writer:
            writer.save(thumb)


def main():
    parser = argparse.ArgumentParser(description="Create new batches of thumbnails for testing")

    parser.add_argument('input', help="directory to search in")

    number = parser.add_mutually_exclusive_group()
    number.add_argument('--random', '-r', type=int, help="generate thumbnails for random ometifs", default=0)
    number.add_argument('--first', '-f', type=int, help="generate x number of thumbnails for ometifs", default=0)

    args = parser.parse_args()
    rand_max = args.random

    only_ome_tif = [f for f in iglob(join(args.input, "**", "*.ome.tif"))]

    if not only_ome_tif:
        only_files = [join(args.input, file_name) for file_name in listdir(args.input) if
                      isfile(join(args.input, file_name))]
        only_ome_tif = [f for f in only_files if f.endswith(".ome.tif")]

    if rand_max is not 0:
        if len(only_ome_tif) <= rand_max:
            random_file_list = only_ome_tif
        else:
            random_file_list = only_ome_tif[:rand_max]
            k = 1
            for ome_tif in only_ome_tif:
                if random.uniform(0, rand_max) > rand_max / 2:
                    random_file_list [int(random.uniform(0, rand_max))] = ome_tif
                k += 1
        file_list = sorted(random_file_list)
    elif args.first is not 0:
        file_list = sorted(only_ome_tif)
        if args.first <= len(file_list):
            file_list = file_list[:args.first]
    else:
        file_list = sorted(only_ome_tif)

    print (file_list)

    for color in color_choices:
        full_fields_color(file_list, color=color)


if __name__ == "__main__":
    print (sys.argv)
    main()
    sys.exit(0)
