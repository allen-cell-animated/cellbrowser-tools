#!/usr/bin/env python

# author: Zach Crabtree zacharyc@alleninstitute.org

import json
import cellJob
from thumbnail2 import *

def generatePng(row):
    full_path = os.path.join(row.inputFolder, row.inputFilename)
    with cziReader.CziReader(full_path) as reader:
        image = reader.load()
    image = image.squeeze(0).transpose(1, 0, 2, 3)
    cell_seg_channel = image.shape[0] - 1
    # TODO: Membrane channel isn't coming through clearly
    thumbnail = np.transpose(makeThumbnail(image, channel_indices=[int(row.nucChannel), int(row.memChannel), int(row.structureChannel)],
                              size=row.cbrThumbnailSize, seg_channel_index=cell_seg_channel),(2, 1, 0))
    pngwriter = pngWriter.PngWriter(os.path.join(row.cbrThumbnailLocation, 'test.png'))
    pngwriter.save(thumbnail, overwrite_file=True)


def generate(row):
    generatePng(row)


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

    generate(info)


def main():
    parser = argparse.ArgumentParser(description='Process data set defined in csv files, and prepare for ingest into bisque db.'
                                                 'Example: python processImageWithSegmentation.py /path/to/csv --outpath /path/to/destination/dir')
    parser.add_argument('input', help='input json file')
    args = parser.parse_args()

    do_main(args.input)


if __name__ == "__main__":
    print sys.argv
    main()
    sys.exit(0)