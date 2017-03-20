from __future__ import print_function
import argparse
import sys
import os
import random
from thumbnailGenerator import ThumbnailGenerator
from aics.image.io.pngWriter import PngWriter
from aics.image.io.omeTifReader import OmeTifReader

# Author: Zach Crabtree zacharyc@alleninstitute.org

# see http://www.somersault1824.com/tips-for-designing-scientific-figures-for-color-blind-readers/
# or http://mkweb.bcgsc.ca/biovis2012/color-blindness-palette.png
_cmy = ([[0.0, 1.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 0.0]], "cmy")
_cym = ([[0.0, 1.0, 1.0], [1.0, 1.0, 0.0], [1.0, 0.0, 1.0]], "cym")
_ymc = ([[1.0, 1.0, 0.0], [1.0, 0.0, 1.0], [0.0, 1.0, 1.0]], "ymc")
_ycm = ([[1.0, 1.0, 0.0], [0.0, 1.0, 1.0], [1.0, 0.0, 1.0]], "ycm")
_myc = ([[1.0, 0.0, 1.0], [1.0, 1.0, 0.0], [0.0, 1.0, 1.0]], "myc")
_mcy = ([[1.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 0.0]], "mcy")
_rgb = ([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]], "rgb")
_rbg = ([[1.0, 0.0, 0.0], [0.0, 0.0, 1.0], [0.0, 1.0, 0.0]], "rbg")
_brg = ([[0.0, 0.0, 1.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]], "brg")
_cwm = ([[0.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 0.0, 1.0]], "cwm")
_mwc = ([[1.0, 0.0, 1.0], [1.0, 1.0, 1.0], [0.0, 1.0, 1.0]], "mwc")
color_choices = [_cym, _myc] # [_cmy, _cwm, _cym, _mwc, _ycm, _ymc]
layering_choices = ["alpha-blend"] #, "superimpose"]
projection_choices = [("sections", 5)] # [("sections", 3), ("sections", 4), ("sections", 5), ("slice", -1), ("max", -1)]


def is_segmented_image(file_name):
    # TODO: find a better way to determine this... because the underscore count doesn't work in all cases.
    file_name = file_name[file_name.rfind('/')+1:-8]
    underscore_count = file_name.count('_')
    if underscore_count == 3 or underscore_count == 5:
        return True
    else:
        return False

def full_field_batcher(ome_tif_files):
    for projection in projection_choices:
        for palette in color_choices:
            for layering in layering_choices:
                print("\nProcessing images with " + palette[1] + " palette, " + layering + " layering, and " + str(projection) + " projections.")
                generator = ThumbnailGenerator(colors=palette[0], layering=layering, projection=projection[0], proj_sections=projection[1])
                for file_name in ome_tif_files:
                    with OmeTifReader(file_name) as reader:
                        # converts to ZCYX
                        image = reader.load()[0]
                    base_file_name = os.path.basename(file_name)
                    print("processing " + base_file_name + "...", end="")
                    thumb = generator.make_thumbnail(image, apply_cell_mask=is_segmented_image(base_file_name))
                    if projection[1] != -1:
                        proj_dir = os.path.join(projection[0], str(projection[1]))
                    else:
                        proj_dir = projection[0]
                    new_path = os.path.join("/home/zacharyc/Development/cellbrowser-tools/dryrun/images/",
                                            palette[1], layering, proj_dir,
                                            base_file_name[:-8] + ".png")
                    if not os.path.exists(new_path[:new_path.rfind('/')]):
                        os.makedirs(new_path[:new_path.rfind('/')])
                    with PngWriter(new_path, overwrite_file=True) as writer:
                        writer.save(thumb)
                    print("done")


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
                rand_index = random.randint(0, len(only_ome_tif) - 1)
                ome_tif_file = only_ome_tif[rand_index]
                if not ome_tif_file in random_file_list:
                    random_file_list.append(only_ome_tif[rand_index])
        file_list = sorted(random_file_list)
    elif args.first is not 0:
        file_list = sorted(only_ome_tif)
        if args.first <= len(file_list):
            file_list = file_list[:args.first]
    else:
        file_list = sorted(only_ome_tif)

    full_field_batcher(file_list)


if __name__ == "__main__":
    main()
    sys.exit(0)
