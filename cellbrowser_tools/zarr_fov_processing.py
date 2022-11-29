from aicsimageio.writers import OmeZarrWriter
from aicsimageio.writers.two_d_writer import TwoDWriter
from aicsimageio import AICSImage
from aicsimageio.types import PhysicalPixelSizes
from . import cellJob
from . import dataHandoffUtils as utils
from .dataset_constants import AugmentedDataField, DataField
from aicsimageprocessing import thumbnailGenerator
from aicsimageprocessing import textureAtlas

# import copy
import argparse
import collections
from copy import deepcopy
import dask.array as da
from distributed import LocalCluster, Client
import errno
import json
import logging
import numpy as np
from ome_types import from_xml, to_xml
from ome_types.model import Channel, TiffData, Plane
import os
import re
import sys
from tifffile import TiffFile
import traceback
import xml.etree.ElementTree as ET
import s3fs


log = logging.getLogger()


###############################################################################


def check_num_planes(omepixels):
    if len(omepixels.planes) != omepixels.size_c * omepixels.size_t * omepixels.size_z:
        raise ValueError(
            f"number of planes {len(omepixels.planes)} not consistent with sizeC*sizeZ*sizeT {omepixels.size_c}*{omepixels.size_t}*{omepixels.size_z}"
        )


def retrieve_file(read_path, file_name):
    """
    Copy a file to a temporary directory, assign it the given name, and return the full destination path.
    """
    return read_path
    # output_directory = Path(tempfile.gettempdir())
    # if not output_directory.is_dir():
    #     raise Exception(f"Output directory {output_directory} does not exist!")

    # destination = output_directory / file_name
    # shutil.copyfile(read_path, destination)
    # return destination


def unretrieve_file(localpath):
    # os.remove(localpath)
    return


def _int32(x):
    if x > 0xFFFFFFFF:
        raise OverflowError
    if x > 0x7FFFFFFF:
        x = int(0x100000000 - x)
        if x < 2147483648:
            return -x
        else:
            return -2147483648
    return x


def _rgba255(r, g, b, a):
    if not (0 <= r <= 255 and 0 <= g <= 255 and 0 <= b <= 255 and 0 <= a <= 255):
        raise ValueError("rgba values out of 0..255 range")
    # bit shift to compose rgba tuple
    x = r << 24 | g << 16 | b << 8 | a
    # now force x into a signed 32 bit integer for OME XML Channel Color.
    return _int32(x)


# note that shape is expected to be z,y,x
def clamp(x, y, z, shape):
    # do not subtract 1 from max sizes because this will be used as an array range
    # in crop_to_segmentation below
    return max(0, min(x, shape[2])), max(0, min(y, shape[1])), max(0, min(z, shape[0]))


# assuming 3d segmentation image (ZYX)
def get_segmentation_bounds(segmentation_image, index, margin=5):
    # find bounding box
    b = np.argwhere(segmentation_image == index)
    # ths plus-1 means these bounds are min-inclusive and max-exclusive.
    # in other words, looping for i = start; i < stop; ++i
    (zstart, ystart, xstart), (zstop, ystop, xstop) = b.min(0), b.max(0) + 1

    # apply margins and clamp to image edges
    # TODO: margin in z is not the same as xy
    xstart, ystart, zstart = clamp(
        xstart - margin, ystart - margin, zstart - margin, segmentation_image.shape
    )
    xstop, ystop, zstop = clamp(
        xstop + margin, ystop + margin, zstop + margin, segmentation_image.shape
    )

    return [[xstart, xstop], [ystart, ystop], [zstart, zstop]]


# assuming 4d image (CZYX) and bounds as [[xmin,xmax],[ymin,ymax],[zmin,zmax]]
def crop_to_bounds(image, bounds):
    atrim = np.copy(
        image[
            :,
            bounds[2][0] : bounds[2][1],
            bounds[1][0] : bounds[1][1],
            bounds[0][0] : bounds[0][1],
        ]
    )
    return atrim


def image_to_mask(image3d, index, mask_positive_value=1):
    return np.where(image3d == index, mask_positive_value, 0).astype(image3d.dtype)


def make_dir(dirname):
    if not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
        except (OSError) as e:
            if e.errno != errno.EEXIST:
                raise
            pass


CellMeta = collections.namedtuple("CellMeta", "bounds parent_image index")


class ImageProcessor:
    def __init__(self, info):
        self.do_thumbnails = True

        self.job = info
        if isinstance(info, cellJob.CellJob):
            self.row = info.cells[0]
            self.do_thumbnails = info.do_thumbnails
        elif "cells" in info:
            self.row = info.cells[0]
        else:
            self.row = info

        # Setting up directory paths for images
        readpath = self.row[DataField.AlignedImageReadPath]
        if not readpath:
            readpath = self.row[DataField.SourceReadPath]
        self.image_file = utils.normalize_path(readpath)
        self.file_name = utils.get_fov_name_from_row(self.row)
        self._generate_paths()

        # Setting up segmentation channels for full image
        self.seg_indices = []
        self.channels_to_mask = []
        self.omexml = None

        self.recipe = self.build_recipe_variance_hipsc(self.row)
        self.image = self.build_combined_image(self.recipe)

    def _generate_paths(self):
        if self.job.cbrThumbnailLocation:
            thumbnaildir = utils.normalize_path(self.job.cbrThumbnailLocation)
        else:
            thumbnaildir = "."
        if self.job.cbrImageLocation:
            ometifdir = utils.normalize_path(self.job.cbrImageLocation)
        else:
            ometifdir = "."
        if self.job.cbrTextureAtlasLocation:
            atlasdir = utils.normalize_path(self.job.cbrTextureAtlasLocation)
        else:
            atlasdir = "."

        make_dir(thumbnaildir)
        self.png_dir = thumbnaildir

        make_dir(ometifdir)
        self.ometif_dir = ometifdir

        make_dir(atlasdir)
        self.atlas_dir = atlasdir

    def build_combined_image(self, recipe):
        result = da.array([])
        for index, channel_spec in enumerate(recipe):
            fpath = retrieve_file(
                channel_spec["file"], os.path.basename(channel_spec["file"])
            )
            image = AICSImage(fpath)
            data = image.get_image_dask_data(
                "ZYX", T=0, C=channel_spec["channel_index"]
            )
            if index > 0:
                # handle bad data:
                # general ZYX shape mismatch
                if data.shape != result[0].shape:
                    raise ValueError(
                        "Image shapes do not match: {} != {}".format(
                            data.shape, result[0].shape
                        )
                    )
                result = da.append(result, [data], axis=0)
            else:
                result = da.array([data])
            unretrieve_file(fpath)
        return result

    def build_recipe_variance_hipsc(self, data_row):
        readpath = data_row[DataField.AlignedImageReadPath]
        if not readpath:
            readpath = data_row[DataField.SourceReadPath]
        image_file = utils.normalize_path(readpath)

        recipe = [
            {
                "channel_name": "Membrane",
                "file": image_file,
                "channel_color": _rgba255(255, 255, 255, 255),
                "channel_index": int(data_row[DataField.ChannelNumber638]),
            },
            {
                "channel_name": "Labeled Structure",
                "file": image_file,
                "channel_color": _rgba255(255, 255, 255, 255),
                "channel_index": int(data_row[DataField.ChannelNumberStruct]),
            },
            {
                "channel_name": "DNA",
                "file": image_file,
                "channel_color": _rgba255(255, 255, 255, 255),
                "channel_index": int(data_row[DataField.ChannelNumber405]),
            },
            {
                "channel_name": "Bright field",
                "file": image_file,
                "channel_color": _rgba255(255, 255, 255, 255),
                "channel_index": int(data_row[DataField.ChannelNumberBrightfield]),
            },
        ]
        readpath = data_row[DataField.StructureSegmentationReadPath]
        if readpath != "" and readpath is not None:
            struct_seg_file = utils.normalize_path(readpath)
            # print(struct_seg_file)
            recipe.append(
                {
                    "channel_name": "SEG_STRUCT",
                    "file": struct_seg_file,
                    "channel_color": _rgba255(255, 255, 255, 255),
                    # structure segmentation assumed in channel 0 always?
                    "channel_index": 0,
                }
            )

        # cell segmentation
        readpath = data_row[DataField.MembraneSegmentationReadPath]
        if readpath != "" and readpath is not None:
            cell_seg_file = utils.normalize_path(readpath)
            # print(cell_seg_file)
            recipe.append(
                {
                    "channel_name": "SEG_Memb",
                    "file": cell_seg_file,
                    "channel_color": _rgba255(255, 255, 255, 255),
                    "channel_index": data_row[
                        DataField.MembraneSegmentationChannelIndex
                    ],
                }
            )

        # nucleus segmentation
        readpath = data_row[DataField.NucleusSegmentationReadPath]
        if readpath != "" and readpath is not None:
            nuc_seg_file = utils.normalize_path(readpath)
            # print(nuc_seg_file)
            recipe.append(
                {
                    "channel_name": "SEG_DNA",
                    "file": nuc_seg_file,
                    "channel_color": _rgba255(255, 255, 255, 255),
                    "channel_index": data_row[
                        DataField.NucleusSegmentationChannelIndex
                    ],
                }
            )

        if data_row[DataField.MembraneContourReadPath] is None:
            data_row[DataField.MembraneContourReadPath] = data_row[
                DataField.MembraneSegmentationReadPath
            ]

        # cell contour segmentation (good for viz in the volume viewer)
        readpath = data_row[DataField.MembraneContourReadPath]
        if readpath != "" and readpath is not None:
            cell_con_file = utils.normalize_path(readpath)
            # print(cell_seg_file)
            recipe.append(
                {
                    "channel_name": "Con_Memb",
                    "file": cell_con_file,
                    "channel_color": _rgba255(255, 255, 255, 255),
                    "channel_index": data_row[DataField.MembraneContourChannelIndex],
                }
            )

        if data_row[DataField.NucleusContourReadPath] is None:
            data_row[DataField.NucleusContourReadPath] = data_row[
                DataField.NucleusSegmentationReadPath
            ]

        # nucleus contour segmentation (good for viz in the volume viewer)
        readpath = data_row[DataField.NucleusContourReadPath]
        if readpath != "" and readpath is not None:
            nuc_con_file = utils.normalize_path(readpath)
            # print(nuc_seg_file)
            recipe.append(
                {
                    "channel_name": "Con_DNA",
                    "file": nuc_con_file,
                    "channel_color": _rgba255(255, 255, 255, 255),
                    "channel_index": data_row[DataField.NucleusContourChannelIndex],
                }
            )

        return recipe

    def generate_meta(self, metadata, row, cell_meta: CellMeta = None):
        m = {}

        m["date_time"] = metadata.images[0].acquisition_date.strftime("%c")

        if len(metadata.instruments) > 0:
            instrument = metadata.instruments[0]
            if len(instrument.objectives) > 0:
                objective = instrument.objectives[0]
                m["objective"] = objective.nominal_magnification
                m["numerical_aperture"] = objective.lens_na

        if cell_meta is not None:
            m["bounds"] = [
                cell_meta.bounds["xmin"],
                cell_meta.bounds["xmax"],
                cell_meta.bounds["ymin"],
                cell_meta.bounds["ymax"],
                cell_meta.bounds["zmin"],
                cell_meta.bounds["zmax"],
            ]
            m["CellIndex"] = cell_meta.index
            m["CellId"] = row[DataField.CellId]
            m["source"] = cell_meta.parent_image
            m["isCropped"] = True
            m["mitoticPhase"] = row["MitoticStateId/Name"]
            m["isMitotic"] = row["MitoticStateId/Name"] == "M0"
        else:
            m["isCropped"] = False
            if self.job.cells:
                m["CellId"] = [r[DataField.CellId] for r in self.job.cells]
                m["CellIndex"] = [r[DataField.CellIndex] for r in self.job.cells]

        m["isModel"] = False

        m["cellLine"] = row[DataField.CellLine]
        m["colonyPosition"] = row[DataField.ColonyPosition]
        m["cellSegmentationVersion"] = row[
            DataField.NucMembSegmentationAlgorithmVersion
        ]
        m["cellSegmentationMethod"] = row[DataField.NucMembSegmentationAlgorithm]
        m["structureSegmentationVersion"] = row[
            DataField.StructureSegmentationAlgorithmVersion
        ]
        m["structureSegmentationMethod"] = row[DataField.StructureSegmentationAlgorithm]
        m["inputFilename"] = row[DataField.SourceFilename]
        m["protein"] = row[DataField.ProteinDisplayName]
        m["structure"] = row[DataField.StructureDisplayName]
        m["gene"] = row[DataField.Gene]
        m["FOVId"] = row[DataField.FOVId]

        # TODO: any preset viewing / slider values go here, including contrast presets

        return m

    def generate_and_save(self):
        recipe = self.recipe
        data = self.image

        os.environ["AWS_PROFILE"] = "animatedcell"
        os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

        # channel_colors = [0xff0000, 0x00ff00, 0x0000ff, 0xffff00, 0xff00ff, 0x00ffff, 0x880000, 0x008800, 0x000088]

        # note need aws creds locally for this to work
        s3 = s3fs.S3FileSystem(anon=False, config_kwargs={"connect_timeout": 60})

        pps = PhysicalPixelSizes(
            self.row[DataField.PixelScaleZ],
            self.row[DataField.PixelScaleY],
            self.row[DataField.PixelScaleX],
        )
        cn = [i["channel_name"] for i in recipe]
        channel_colors = [0xFFFFFFFF for i in recipe]

        # print(data.shape)
        destination = (
            "s3://animatedcell-test-data/variance/"
            # + self.job.cbrImageRelPath
            # + "/"
            + self.file_name
            + ".zarr/"
        )
        # destination = os.path.join(self.ometif_dir, self.file_name + ".zarr/")

        writer = OmeZarrWriter(destination)

        if len(data.shape) < 5:
            for i in range(5 - len(data.shape)):
                data = np.expand_dims(data, axis=0)

        writer.write_image(
            image_data=data,  # : types.ArrayLike,  # must be 5D TCZYX
            image_name="",  #: str,
            physical_pixel_sizes=pps,  # : Optional[types.PhysicalPixelSizes],
            channel_names=cn,  # : Optional[List[str]],
            channel_colors=channel_colors,  # : Optional[List[int]],
            scale_num_levels=3,  # : int = 1,
            scale_factor=2.0,  #  : float = 2.0,
        )


def do_main_image_with_celljob(info):
    processor = ImageProcessor(info)
    processor.generate_and_save()


def do_main_image(fname):
    with open(fname) as jobfile:
        jobspec = json.load(jobfile)
        info = cellJob.CellJob(jobspec["cells"])
        for key in jobspec:
            setattr(info, key, jobspec[key])
        # if info.cbrParseError:
        #     sys.stderr.write("\n\nEncountered parsing error!\n\n###\nCell Job Object\n###\n")
        #     pprint.pprint(jobspec, stream=sys.stderr)
        #     return
    return do_main_image_with_celljob(info)


def main():
    parser = argparse.ArgumentParser(
        description="Process data set defined in csv files, and prepare for ingest into bisque db."
        "Example: python processImageWithSegmentation.py /path/to/csv --outpath /path/to/destination/dir"
    )
    parser.add_argument("input", help="input json file")
    args = parser.parse_args()

    do_main_image(args.input)


if __name__ == "__main__":
    try:
        print(sys.argv)
        main()
        sys.exit(0)

    except Exception as e:
        print(str(e), file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        sys.exit(1)
