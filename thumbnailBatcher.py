import argparse
import sys
import os
import random
from thumbnailGenerator import ThumbnailGenerator
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


def is_segmented_image(file_name):
    if file_name.count('_') == 3:
        return True
    else:
        return False


def full_fields_color(ome_tif_files, color):

    print("\nprocessing images with " + color[1] + " palette.\n")

    generator = ThumbnailGenerator(colors=color[0], layering="alpha-blend")
    for file_name in ome_tif_files:
        with OmeTifReader(file_name) as reader:
            # converts to CZYX
            image = reader.load()[0].transpose((1, 0, 2, 3))
        base_file_name = os.path.basename(file_name)
        print("processing " + file_name + "...")
        thumb = generator.make_thumbnail(image, apply_cell_mask=is_segmented_image(base_file_name))
        # TODO convert this to use the current directory with os.path
        # path_as_list = re.split(r'\\|/', file_name)
        # new_path = path_as_list[:-2]
        # new_path.append(color[1])
        # new_path.append(path_as_list[len(path_as_list) - 1][:-8] + ".png")
        new_path = os.path.join("/home/zacharyc/Development/cellbrowser-tools/dryrun/images/", color[1], base_file_name[:-8] + ".png")
        if not os.path.exists(new_path[:new_path.rfind('/')]):
            os.path.makedirs(new_path[:new_path.rfind('/')])
        with PngWriter(new_path, overwrite_file=True) as writer:
            writer.save(thumb)


def main():
    parser = argparse.ArgumentParser(description="Create new batches of thumbnails for testing")

    parser.add_argument('input', help="directory to search in")

    number = parser.add_mutually_exclusive_group()
    number.add_argument('--random', '-r', type=int, help="generate thumbnails for random ometifs", default=0)
    number.add_argument('--first', '-f', type=int, help="generate x number of thumbnails for ometifs", default=0)

    args = parser.parse_args()

    only_ome_tif = []

    for root, dirs, files in os.walk(args.input):
        for file_name in files:
            if file_name.endswith('.ome.tif'):
                only_ome_tif.append(os.path.join(root, file_name))

    if args.random is not 0:
        rand_max = args.random
        if len(only_ome_tif) <= rand_max:
            random_file_list = only_ome_tif
        else:
            random_file_list = []
            while len(random_file_list) != rand_max:
                rand_index = random.randint(0, len(only_ome_tif))
                while only_ome_tif[rand_index] in random_file_list:
                    rand_index = random.randint(0, len(only_ome_tif))
                random_file_list.append(only_ome_tif[rand_index])
        file_list = sorted(random_file_list)
    elif args.first is not 0:
        file_list = sorted(only_ome_tif)
        if args.first <= len(file_list):
            file_list = file_list[:args.first]
    else:
        file_list = sorted(only_ome_tif)

    for color in color_choices:
        full_fields_color(file_list, color=color)


if __name__ == "__main__":
    main()
    sys.exit(0)
