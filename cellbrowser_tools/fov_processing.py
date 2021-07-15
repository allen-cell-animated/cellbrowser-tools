from aicsimageio.writers import OmeTiffWriter
from aicsimageio.writers import PngWriter
from aicsimageio import AICSImage
from . import cellJob
from . import dataHandoffUtils as utils
from .dataset_constants import AugmentedDataField, DataField
from aicsimageprocessing import thumbnailGenerator
from aicsimageprocessing import textureAtlas

# import copy
import argparse
import collections
from copy import deepcopy
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


log = logging.getLogger()


###############################################################################


def check_num_planes(omepixels):
    if len(omepixels.planes) != omepixels.size_c * omepixels.size_t * omepixels.size_z:
        raise ValueError(
            f"number of planes {len(omepixels.planes)} not consistent with sizeC*sizeZ*sizeT {omepixels.size_c}*{omepixels.size_t}*{omepixels.size_z}"
        )


def _clean_ome_xml_for_known_issues(xml: str) -> str:
    # This is a known issue that could have been caused by prior versions of aicsimageio
    # due to our old OMEXML.py file.
    #
    # You can see the PR that updated this exact line here:
    # https://github.com/AllenCellModeling/aicsimageio/pull/116/commits/e3f9cde7f680edeef3ef3586a67fd8106e746167#diff-46a483e94af833f7eaa1106921191fed5e7c77f33a5c0c47a8f5a2d35ad3ba96L47
    #
    # Notably why this is invalid is that the 2012-03 schema _doesn't exist_
    #
    # Don't know how this wasn't ever caught before that PR but to ensure that we don't
    # error in reading the OME in aicsimageio>=4.0.0, we manually find and replace this
    # line in OME xml prior to creating the OME object.
    KNOWN_INVALID_OME_XSD_REFERENCES = [
        "www.openmicroscopy.org/Schemas/ome/2013-06",
        "www.openmicroscopy.org/Schemas/OME/2012-03",
    ]
    REPLACEMENT_OME_XSD_REFERENCE = "www.openmicroscopy.org/Schemas/OME/2016-06"
    # Store list of changes to print out with warning
    metadata_changes = []

    # Fix xsd reference
    # This is from OMEXML object just having invalid reference
    for known_invalid_ref in KNOWN_INVALID_OME_XSD_REFERENCES:
        if known_invalid_ref in xml:
            xml = xml.replace(known_invalid_ref, REPLACEMENT_OME_XSD_REFERENCE,)
            metadata_changes.append(
                f"Replaced '{known_invalid_ref}' with "
                f"'{REPLACEMENT_OME_XSD_REFERENCE}'."
            )

    # Read in XML
    root = ET.fromstring(xml)

    # Get the namespace
    # In XML etree this looks like
    # "{http://www.openmicroscopy.org/Schemas/OME/2016-06}"
    # and must prepend any etree finds
    namespace_matches = re.match(r"\{.*\}", root.tag)
    if namespace_matches is not None:
        namespace = namespace_matches.group(0)
    else:
        raise ValueError("XML does not contain a namespace")

    # Find all Image elements and fix IDs
    # This is for certain for test files of ours and ACTK files
    for image_index, image in enumerate(root.findall(f"{namespace}Image")):
        image_id = image.get("ID")
        if not image_id.startswith("Image"):
            image.set("ID", f"Image:{image_id}")
            metadata_changes.append(
                f"Updated attribute 'ID' from '{image_id}' to 'Image:{image_id}' "
                f"on Image element at position {image_index}."
            )

        # Find all Pixels elements and fix IDs
        for pixels_index, pixels in enumerate(image.findall(f"{namespace}Pixels")):
            pixels_id = pixels.get("ID")
            if not pixels_id.startswith("Pixels"):
                pixels.set("ID", f"Pixels:{pixels_id}")
                metadata_changes.append(
                    f"Updated attribute 'ID' from '{pixels_id}' to "
                    f"Pixels:{pixels_id}' on Pixels element at "
                    f"position {pixels_index}."
                )

            # Determine if there is an out-of-order channel / plane elem
            # This is due to OMEXML "add channel" function
            # That added Channels and appropriate Planes to the XML
            # But, placed them in:
            # Channel
            # Plane
            # Plane
            # ...
            # Channel
            # Plane
            # Plane
            #
            # Instead of grouped together:
            # Channel
            # Channel
            # ...
            # Plane
            # Plane
            # ...
            #
            # This effects all CFE files (new and old) but for different reasons
            pixels_children_out_of_order = False
            encountered_something_besides_channel = False
            for child in pixels:
                if child.tag != f"{namespace}Channel":
                    encountered_something_besides_channel = True
                if (
                    encountered_something_besides_channel
                    and child.tag == f"{namespace}Channel"
                ):
                    pixels_children_out_of_order = True
                    break

            # Ensure order of:
            # channels -> bindata | tiffdata | metadataonly -> planes
            # setting this to true means just ALWAYS do this.
            pixels_children_out_of_order = True
            if pixels_children_out_of_order:
                # Get all relevant elems
                channels = [deepcopy(c) for c in pixels.findall(f"{namespace}Channel")]
                bin_data = [deepcopy(b) for b in pixels.findall(f"{namespace}BinData")]
                tiff_data = [
                    deepcopy(t) for t in pixels.findall(f"{namespace}TiffData")
                ]
                # There should only be one metadata only element but to standardize
                # list comprehensions later we findall
                metadata_only = [
                    deepcopy(m) for m in pixels.findall(f"{namespace}MetadataOnly")
                ]
                planes = [deepcopy(p) for p in pixels.findall(f"{namespace}Plane")]

                # Old (2018 ish) cell feature explorer files sometimes contain both
                # an empty metadata only element and filled tiffdata elements
                # Since the metadata only elements are empty we can check this and
                # choose the tiff data elements instead
                #
                # First check if there are any metadata only elements
                if len(metadata_only) == 1:
                    # Now check if _one of_ of the other two choices are filled
                    # ^ in Python is XOR
                    if (len(bin_data) > 0) ^ (len(tiff_data) > 0):
                        metadata_children = list(metadata_only[0])
                        # Now check if the metadata only elem has no children
                        if len(metadata_children) == 0:
                            # If so, just "purge" by creating empty list
                            metadata_only = []

                        # If there are children elements
                        # Return XML and let XMLSchema Validation show error
                        else:
                            return xml

                # After cleaning metadata only, validate the normal behaviors of
                # OME schema
                #
                # Validate that there is only one of bindata, tiffdata, or metadata
                if len(bin_data) > 0:
                    if len(tiff_data) == 0 and len(metadata_only) == 0:
                        selected_choice = bin_data
                    else:
                        # Return XML and let XMLSchema Validation show error
                        return xml
                elif len(tiff_data) > 0:
                    if len(bin_data) == 0 and len(metadata_only) == 0:
                        selected_choice = tiff_data
                    else:
                        # Return XML and let XMLSchema Validation show error
                        return xml
                elif len(metadata_only) == 1:
                    if len(bin_data) == 0 and len(tiff_data) == 0:
                        selected_choice = metadata_only
                    else:
                        # Return XML and let XMLSchema Validation show error
                        return xml
                else:
                    # Return XML and let XMLSchema Validation show error
                    return xml

                # Remove all children from element to be replaced
                # with ordered elements
                for elem in list(pixels):
                    pixels.remove(elem)

                # Re-attach elements
                for channel in channels:
                    pixels.append(channel)
                for elem in selected_choice:
                    pixels.append(elem)
                for plane in planes:
                    pixels.append(plane)

                metadata_changes.append(
                    f"Reordered children of Pixels element at "
                    f"position {pixels_index}."
                )

    # This is a result of dumping basically all experiement metadata
    # into "StructuredAnnotation" blocks
    #
    # This affects new (2020) Cell Feature Explorer files
    #
    # Because these are structured annotations we don't want to mess with anyones
    # besides the AICS generated bad structured annotations
    aics_anno_removed_count = 0
    sa = root.find(f"{namespace}StructuredAnnotations")
    if sa is not None:
        for xml_anno in sa.findall(f"{namespace}XMLAnnotation"):
            # At least these are namespaced
            if xml_anno.get("Namespace") == "alleninstitute.org/CZIMetadata":
                # Get ID because some elements have annotation refs
                # in both the base Image element and all plane elements
                aics_anno_id = xml_anno.get("ID")
                for image in root.findall(f"{namespace}Image"):
                    for anno_ref in image.findall(f"{namespace}AnnotationRef"):
                        if anno_ref.get("ID") == aics_anno_id:
                            image.remove(anno_ref)

                    # Clean planes
                    pixels = image.find(f"{namespace}Pixels")
                    for plane in pixels.findall(f"{namespace}Plane"):
                        for anno_ref in plane.findall(f"{namespace}AnnotationRef"):
                            if anno_ref.get("ID") == aics_anno_id:
                                plane.remove(anno_ref)

                # Remove the whole etree
                sa.remove(xml_anno)
                aics_anno_removed_count += 1

    # Log changes
    if aics_anno_removed_count > 0:
        metadata_changes.append(
            f"Removed {aics_anno_removed_count} AICS generated XMLAnnotations."
        )

    # If there are no annotations in StructuredAnnotations, remove it
    if sa is not None:
        if len(list(sa)) == 0:
            root.remove(sa)

    # If any piece of metadata was changed alert and rewrite
    if len(metadata_changes) > 0:
        log.debug("OME metadata was cleaned for known AICSImageIO 3.x OMEXML errors.")
        log.debug(f"Full list of OME cleaning changes: {metadata_changes}")

        # Register namespace
        ET.register_namespace("", f"http://{REPLACEMENT_OME_XSD_REFERENCE}")

        # Write out cleaned XML to string
        xml = ET.tostring(root, encoding="unicode", method="xml",)

    return xml


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

        self.image = self.add_segs_to_img()

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

    def build_file_list(self):
        # TODO:
        # make all this data driven.
        # either we just take the ALL channels from the images IN ORDER
        # or we provide some kind of listing and renaming:
        #     source file
        #     channel index
        #     new name
        #     color (maybe not even needed)
        # this is enough data to combine together many channels from many files

        # start with 4 channels for membrane, structure, nucleus and transmitted light
        self.channel_names = ["Membrane", "Labeled structure", "DNA", "Bright field"]
        self.channel_colors = [
            _rgba255(128, 0, 128, 255),
            _rgba255(128, 128, 0, 255),
            _rgba255(0, 128, 128, 255),
            _rgba255(128, 128, 128, 255),
        ]
        self.channel_indices = [
            int(self.row[DataField.ChannelNumber638]),
            int(self.row[DataField.ChannelNumberStruct]),
            int(self.row[DataField.ChannelNumber405]),
            int(self.row[DataField.ChannelNumberBrightfield]),
        ]

        # special case replace OBS_STRUCT with OBS_Alpha-actinin-2
        # if CellIndex starts with C and FOVId starts with F then we are in a special data set.
        # gene-editing FISH chaos 2019
        # TODO FIX ME
        if self.row[DataField.CellIndex].startswith("C") and self.row[
            DataField.FOVId
        ].startswith("F"):
            self.channel_names[1] = "Alpha-actinin-2"

        # special case of channel combo when "gene-pair" is present
        # TODO FIX ME
        genepair = self.row.get(AugmentedDataField.GenePair)
        if genepair is not None:
            # genepair is a hyphenated split of 561 and 638 channel names
            pair = genepair.split("-")
            if len(pair) != 2:
                raise ValueError(
                    "Expected gene-pair to have two values joined by hyphen"
                )
            self.channel_names = [
                "Alpha-actinin-2",
                "DNA",
                "Bright field",
                f"{pair[0]}",
                f"{pair[1]}",
            ]
            self.channel_colors = [
                _rgba255(128, 0, 128, 255),
                _rgba255(128, 128, 0, 255),
                _rgba255(0, 128, 128, 255),
                _rgba255(128, 128, 128, 255),
                _rgba255(255, 0, 0, 255),
            ]
            self.channel_indices = [
                int(self.row[DataField.ChannelNumberStruct]),
                int(self.row[DataField.ChannelNumber405]),
                int(self.row[DataField.ChannelNumberBrightfield]),
                int(self.row[AugmentedDataField.ChannelNumber561]),
                int(self.row[DataField.ChannelNumber638]),
            ]

        self.channels_to_mask = []

        log.info("loading segmentations for " + self.file_name + "...")
        # print(seg_path)
        # list of tuple(readpath, channelindex, outputChannelName)
        file_list = []

        # structure segmentation
        readpath = self.row[DataField.StructureSegmentationReadPath]
        if readpath != "" and readpath is not None:
            struct_seg_file = utils.normalize_path(readpath)
            # print(struct_seg_file)
            file_list.append((struct_seg_file, 0, "SEG_STRUCT"))
            self.channel_names.append("SEG_STRUCT")
            self.channel_colors.append(_rgba255(255, 0, 0, 255))

        # cell segmentation
        readpath = self.row[DataField.MembraneSegmentationReadPath]
        if readpath != "" and readpath is not None:
            cell_seg_file = utils.normalize_path(readpath)
            # print(cell_seg_file)
            file_list.append(
                (
                    cell_seg_file,
                    self.row[DataField.MembraneSegmentationChannelIndex],
                    "SEG_Memb",
                )
            )
            self.channel_names.append("SEG_Memb")
            self.channel_colors.append(_rgba255(0, 0, 255, 255))
            self.channels_to_mask.append(len(self.channel_names) - 1)

        # nucleus segmentation
        readpath = self.row[DataField.NucleusSegmentationReadPath]
        if readpath != "" and readpath is not None:
            nuc_seg_file = utils.normalize_path(readpath)
            # print(nuc_seg_file)
            file_list.append(
                (
                    nuc_seg_file,
                    self.row[DataField.NucleusSegmentationChannelIndex],
                    "SEG_DNA",
                )
            )
            self.channel_names.append("SEG_DNA")
            self.channel_colors.append(_rgba255(0, 255, 0, 255))
            self.channels_to_mask.append(len(self.channel_names) - 1)

        if self.row[DataField.MembraneContourReadPath] is None:
            self.row[DataField.MembraneContourReadPath] = self.row[
                DataField.MembraneSegmentationReadPath
            ]

        # cell contour segmentation (good for viz in the volume viewer)
        readpath = self.row[DataField.MembraneContourReadPath]
        if readpath != "" and readpath is not None:
            cell_con_file = utils.normalize_path(readpath)
            # print(cell_seg_file)
            file_list.append(
                (
                    cell_con_file,
                    self.row[DataField.MembraneContourChannelIndex],
                    "CON_Memb",
                )
            )
            self.channel_names.append("CON_Memb")
            self.channel_colors.append(_rgba255(255, 255, 0, 255))
            self.channels_to_mask.append(len(self.channel_names) - 1)

        if self.row[DataField.NucleusContourReadPath] is None:
            self.row[DataField.NucleusContourReadPath] = self.row[
                DataField.NucleusSegmentationReadPath
            ]

        # nucleus contour segmentation (good for viz in the volume viewer)
        readpath = self.row[DataField.NucleusContourReadPath]
        if readpath != "" and readpath is not None:
            nuc_con_file = utils.normalize_path(readpath)
            # print(nuc_seg_file)
            file_list.append(
                (
                    nuc_con_file,
                    self.row[DataField.NucleusContourChannelIndex],
                    "CON_DNA",
                )
            )
            self.channel_names.append("CON_DNA")
            self.channel_colors.append(_rgba255(0, 255, 255, 255))
            self.channels_to_mask.append(len(self.channel_names) - 1)

        return file_list

    def add_segs_to_img(self):
        # outdir = self.job.cbrImageLocation
        # make_dir(outdir)

        # thumbnaildir = self.job.cbrThumbnailLocation
        # make_dir(thumbnaildir)

        file_list = self.build_file_list()

        image_file = self.image_file
        image_file = utils.normalize_path(image_file)
        # print(image_file)

        # COPY FILE TO LOCAL TMP STORAGE BEFORE READING
        image_file = retrieve_file(image_file, self.row[DataField.SourceFilename])

        # 1. obtain OME XML metadata from original microscopy image
        cr = AICSImage(image_file)

        with TiffFile(image_file) as tiff:
            if tiff.is_ome:
                description = tiff.pages[0].description.strip()
                description = _clean_ome_xml_for_known_issues(description)
                # print(description)
                self.omexml = from_xml(description)
            else:
                # this is REALLY catastrophic. Its not expected to happen for AICS data.
                raise ValueError("Bad OME TIFF file")

        # 2. obtain relevant channels from original image file
        image = cr.get_image_data("CZYX", T=0)
        if len(image.shape) != 4:
            raise ValueError("Image did not return 4d CZYX data")

        # image.shape[0] is num of channels.
        if image.shape[0] <= max(self.channel_indices):
            raise ValueError(
                f"Image does not have enough channels - needs at least {max(self.channel_indices)} but has {image.shape[0]}"
            )

        image = np.array([image[i] for i in self.channel_indices])

        # 3. fix up XML to reorder channels
        # we want to preserve all channel and plane data for the channels we are keeping!
        # rename:
        #   channel_indices[0] to channel0
        #   channel_indices[1] to channel1
        #   channel_indices[2] to channel2
        #   channel_indices[3] to channel3

        pix = self.omexml.images[0].pixels
        chxml = [pix.channels[channel] for channel in self.channel_indices]
        pix.channels = chxml
        pix.size_c = len(chxml)

        # fixups:

        # 1. channel ids
        for (c, channel) in enumerate(pix.channels):
            channel.id = f"Channel:0:{c}"

        # 2. remove all planes whose C index is not in self.channel_indices
        new_planes = [p for p in pix.planes if p.the_c in self.channel_indices]
        pix.planes = new_planes

        # 3. then remap the C of the remaining planes, as they stll have their old channel indices
        for p in pix.planes:
            p.the_c = self.channel_indices.index(p.the_c)

        # 4. remove all tiffdata elements in favor of one single one
        pix.tiff_data_blocks = [TiffData(plane_count=len(pix.planes))]
        check_num_planes(pix)

        def add_channel(pix, name):
            channel_index = len(pix.channels)
            channel = Channel(id=f"Channel:0:{channel_index}", name=name)
            pix.channels.append(channel)
            # add Planes(?)
            for p in range(pix.size_z):
                pix.planes.append(
                    Plane(the_c=channel_index, the_z=p, the_t=pix.size_t - 1)
                )
            pix.size_c += 1

        nch = len(self.channel_indices)
        self.seg_indices = []
        i = 0
        for f in file_list:
            fpath = retrieve_file(f[0], os.path.basename(f[0]))
            # expect TifReader to handle it.
            reader = AICSImage(fpath)
            seg = reader.get_image_data("ZYX", C=f[1], T=0)
            # seg is expected to be ZYX
            # image is expected to be CZYX
            if seg.shape[0] != image.shape[1]:
                raise ValueError(
                    f"FOV {self.row[DataField.FOVId]} has shape mismatch {f[2]} {seg.shape[0]} vs FOV {image.shape[1]}"
                )
            if seg.shape[1] != image.shape[2]:
                raise ValueError(
                    f"FOV {self.row[DataField.FOVId]} has shape mismatch {f[2]} {seg.shape[1]} vs FOV {image.shape[2]}"
                )
            if seg.shape[2] != image.shape[3]:
                raise ValueError(
                    f"FOV {self.row[DataField.FOVId]} has shape mismatch {f[2]} {seg.shape[2]} vs FOV {image.shape[3]}"
                )
            # append channels containing segmentations
            add_channel(pix, self.channel_names[nch + i])

            # axis=0 is the C axis, and nucseg, cellseg, and structseg are assumed to be of shape ZYX
            image = np.append(image, [seg], axis=0)
            self.seg_indices.append(image.shape[0] - 1)
            reader.close()
            unretrieve_file(fpath)
            i += 1

        log.info("done making combined image")
        cr.close()
        unretrieve_file(image_file)
        return image

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
            # m["alignedTransform"] = {
            #     "translation": [
            #         row[AugmentedDataField.MitoticAlignedX],
            #         row[AugmentedDataField.MitoticAlignedY],
            #         0,
            #     ],
            #     "rotation": [0, 0, row[AugmentedDataField.MitoticAlignedAngle]],
            # }
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

    def generate_and_save(self, do_segmented_cells=True, save_raw=True):
        base = self.file_name

        # indices of channels in the original image
        # before this, indices have been re-organized in add_segs_to_img (in __init__)
        memb_index = 0
        nuc_index = 2
        struct_index = 1
        thumbnail_colors = [[1.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 0.0]]

        log.info(f"Generating images for FOVId {self.row[DataField.FOVId]}")

        if self.do_thumbnails:
            log.info("making thumbnail...")
            generator = thumbnailGenerator.ThumbnailGenerator(
                channel_indices=[memb_index, nuc_index, struct_index],
                size=self.job.cbrThumbnailSize,
                mask_channel_index=self.seg_indices[1],
                colors=thumbnail_colors,
                projection="slice",
            )
            ffthumb = generator.make_thumbnail(
                self.image.transpose(1, 0, 2, 3), apply_cell_mask=False
            )
            log.info("done making thumbnail")
        else:
            ffthumb = None

        im_to_save = self.image

        # do texture atlas here
        aimage = AICSImage(self.image, known_dims="CZYX")

        log.info("generating atlas ...")
        atlas = textureAtlas.generate_texture_atlas(
            aimage, name=base, max_edge=2048, pack_order=None,
        )
        log.info("done making atlas")
        p = self.omexml.images[0].pixels
        atlas.dims.pixel_size_x = p.physical_size_x
        atlas.dims.pixel_size_y = p.physical_size_y
        atlas.dims.pixel_size_z = p.physical_size_z
        atlas.dims.channel_names = [c for c in self.channel_names]
        # grab metadata for display
        static_meta = self.generate_meta(self.omexml, self.row)

        self._save_and_post(
            image=im_to_save if save_raw else None,
            thumbnail=ffthumb,
            textureatlas=atlas,
            name=base,
            omexml=self.omexml,
            other_data=static_meta,
        )

        # GET READY TO DO SEGMENTED CELL IMAGES
        if not do_segmented_cells:
            return

        # assumption: less than 256 cells segmented in the file.
        # assumption: cell segmentation is a numeric index in the pixels
        cell_segmentation_image = self.image[self.seg_indices[1], :, :, :]
        # which bins have segmented pixels?
        # note that this includes zeroes, which is to be ignored.
        h0 = np.unique(cell_segmentation_image)
        h0 = h0[h0 > 0]

        for idx, row in enumerate(self.job.cells):
            # for each cell segmented from this image:
            cell_name = utils.get_cell_name(
                row[DataField.CellId], row[DataField.FOVId], row[DataField.CellLine]
            )
            i = row[DataField.CellIndex]
            log.info(
                f"Generating images for CellId {row[DataField.CellId]}, segmented cell index {i}"
            )

            bounds = get_segmentation_bounds(cell_segmentation_image, i)
            cropped = crop_to_bounds(self.image, bounds)
            # Turn the seg channels into true masks
            # by zeroing out all elements != i.
            # Note that structure segmentation and contour does not use same masking index rules -
            # the values stored are not indexed by cell number.
            for mi in self.channels_to_mask:
                cropped[mi] = image_to_mask(cropped[mi], i, 255)

            if self.do_thumbnails:
                log.info("making thumbnail...")
                generator = thumbnailGenerator.ThumbnailGenerator(
                    channel_indices=[memb_index, nuc_index, struct_index],
                    size=self.job.cbrThumbnailSize,
                    mask_channel_index=self.seg_indices[1],
                    colors=thumbnail_colors,
                    projection="max",
                )
                thumb = generator.make_thumbnail(
                    cropped.copy().transpose(1, 0, 2, 3), apply_cell_mask=True
                )
                log.info("done making thumbnail")
            else:
                thumb = None

            cell_meta = CellMeta(
                bounds={
                    "xmin": int(bounds[0][0]),
                    "xmax": int(bounds[0][1]),
                    "ymin": int(bounds[1][0]),
                    "ymax": int(bounds[1][1]),
                    "zmin": int(bounds[2][0]),
                    "zmax": int(bounds[2][1]),
                },
                index=i,
                parent_image=base,
            )

            # for bn in cell_meta.bounds:
            #    print(bn, cell_meta.bounds[bn])
            minz = int(bounds[2][0])
            maxz = int(bounds[2][1])

            # print(f"cell Z size = {cropped.shape[1]}")
            # print(f"cell Z bounds: {minz} to {maxz}")

            # copy self.omexml for output
            copyxml = None

            log.info("making cropped image...")
            # start with original metadata and make a copy
            # copyxml = self.omexml.copy(deep=True)
            copystr = to_xml(self.omexml)
            copyxml = from_xml(copystr)

            # now fix it up
            pixels = copyxml.images[0].pixels
            pixels.size_x = cropped.shape[3]
            pixels.size_y = cropped.shape[2]
            pixels.size_z = cropped.shape[1]

            # if sizeZ changed, then we have to use bounds to fix up the plane elements:
            # 1. drop planes outside of z bounds.
            # 2. update remaining planes' the_z indices.
            pixels.planes = [
                p for p in pixels.planes if ((p.the_z < maxz) and (p.the_z >= minz))
            ]
            for p in pixels.planes:
                p.the_z -= minz

            log.info("done making cropped image")

            # do texture atlas here
            aimage_cropped = AICSImage(cropped, known_dims="CZYX")
            # aimage_cropped.metadata = copyxml
            log.info("generating cropped atlas ...")
            atlas_cropped = textureAtlas.generate_texture_atlas(
                aimage_cropped, name=cell_name, max_edge=2048, pack_order=None,
            )
            atlas_cropped.dims.pixel_size_x = pixels.physical_size_x
            atlas_cropped.dims.pixel_size_y = pixels.physical_size_y
            atlas_cropped.dims.pixel_size_z = pixels.physical_size_z
            atlas_cropped.dims.channel_names = [c for c in self.channel_names]

            static_meta_cropped = self.generate_meta(copyxml, row, cell_meta)

            im_to_save = cropped
            log.info("done making cropped atlas")

            self._save_and_post(
                image=im_to_save if save_raw else None,
                thumbnail=thumb,
                textureatlas=atlas_cropped,
                name=cell_name,
                omexml=copyxml,
                other_data=static_meta_cropped,
            )
            log.info("done with cropped image")
        log.info("done processing cells for this fov")

    def _save_and_post(
        self, image, thumbnail, textureatlas, name="", omexml=None, other_data=None,
    ):
        # physical_size = [0.065, 0.065, 0.29]
        # note these are strings here.  it's ok for xml purposes but not for any math.
        physical_size = [
            self.row[DataField.PixelScaleX],
            self.row[DataField.PixelScaleY],
            self.row[DataField.PixelScaleZ],
        ]

        png_dir = os.path.join(self.png_dir, name + ".png")
        ometif_dir = os.path.join(self.ometif_dir, name + ".ome.tif")
        # atlas_dir = os.path.join(self.atlas_dir, name + "_atlas.json")

        if thumbnail is not None:
            log.info("saving thumbnail...")
            with PngWriter(file_path=png_dir, overwrite_file=True) as writer:
                writer.save(thumbnail)
            log.info("thumbnail saved")

        if image is not None:
            transposed_image = image.transpose(1, 0, 2, 3)
            log.info("saving image...")
            # calculate the proper plane count
            omepixels = omexml.images[0].pixels
            check_num_planes(omepixels)
            omepixels.tiff_data_blocks = [
                TiffData(
                    plane_count=omepixels.size_c * omepixels.size_z * omepixels.size_t
                )
            ]

            ome_str = to_xml(omexml)
            # appease ChimeraX and possibly others who expect to see this
            ome_str = '<?xml version="1.0" encoding="UTF-8"?>' + ome_str
            with OmeTiffWriter(file_path=ometif_dir, overwrite_file=True) as writer:
                writer.save(
                    transposed_image,
                    ome_xml=ome_str,
                    # channel_names=self.channel_names, channel_colors=self.channel_colors,
                    pixels_physical_size=physical_size,
                )
            log.info("image saved")

        if textureatlas is not None:
            log.info("saving texture atlas...")
            textureatlas.save(self.atlas_dir, user_data=other_data)
            log.info("texture atlas saved")


def do_main_image_with_celljob(info):
    processor = ImageProcessor(info)
    processor.generate_and_save(do_segmented_cells=info.do_crop, save_raw=info.save_raw)


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
