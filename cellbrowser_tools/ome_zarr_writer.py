import zarr
from zarr.storage import DirectoryStore, FSStore

from typing import List, Tuple, Any
from dataclasses import dataclass, asdict

import numpy as np
import logging

import dask.array as da
import skimage.transform

from bioio import BioImage
from ngff_zarr.zarr_metadata import Metadata, Axis, Scale, Translation, Dataset

log = logging.getLogger(__name__)

def chunk_size_from_memory_target(shape: Tuple[int], dtype: str, memory_target: int) -> Tuple[int]:
    """
    Calculate chunk size from memory target.
    :param shape: Shape of the array.
    :param dtype: Data type of the array.
    :param memory_target: Memory target in bytes.
    :return: Chunk size tuple.
    """

    itemsize = np.dtype(dtype).itemsize
    chunk_size = np.array(shape)
    # let's start by just mandating that chunks have to be smaller than
    # 1 T and 1 C
    chunk_size[0] = 1
    chunk_size[1] = 1
    while chunk_size.size * chunk_size.prod() * itemsize > memory_target:
        chunk_size //= 2
    return tuple(chunk_size)

def dim_tuple_to_dict(dims:Tuple[int])->dict:
    if (len(dims) != 5):
        raise ValueError("dims must be a 5-tuple in TCZYX order")
    return {"t":dims[0], "c":dims[1], "z":dims[2], "y":dims[3], "x":dims[4]}

def resize(
    image: da.Array, output_shape: Tuple[int, ...], *args: Any, **kwargs: Any
) -> da.Array:
    r"""
    Wrapped copy of "skimage.transform.resize"
    Resize image to match a certain size.
    :type image: :class:`dask.array`
    :param image: The dask array to resize
    :type output_shape: tuple
    :param output_shape: The shape of the resize array
    :type \*args: list
    :param \*args: Arguments of skimage.transform.resize
    :type \*\*kwargs: dict
    :param \*\*kwargs: Keyword arguments of skimage.transform.resize
    :return: Resized image.
    """
    factors = np.array(output_shape) / np.array(image.shape).astype(float)
    # Rechunk the input blocks so that the factors achieve an output
    # blocks size of full numbers.
    better_chunksize = tuple(
        np.maximum(1, np.round(np.array(image.chunksize) * factors) / factors).astype(
            int
        )
    )
    image_prepared = image.rechunk(better_chunksize)

    # If E.g. we resize image from 6675 by 0.5 to 3337, factor is 0.49992509 so each
    # chunk of size e.g. 1000 will resize to 499. When assumbled into a new array, the
    # array will now be of size 3331 instead of 3337 because each of 6 chunks was
    # smaller by 1. When we compute() this, dask will read 6 chunks of 1000 and expect
    # last chunk to be 337 but instead it will only be 331.
    # So we use ceil() here (and in resize_block) to round 499.925 up to chunk of 500
    block_output_shape = tuple(
        np.ceil(np.array(better_chunksize) * factors).astype(int)
    )

    # Map overlap
    def resize_block(image_block: da.Array, block_info: dict) -> da.Array:
        # if the input block is smaller than a 'regular' chunk (e.g. edge of image)
        # we need to calculate target size for each chunk...
        chunk_output_shape = tuple(
            np.ceil(np.array(image_block.shape) * factors).astype(int)
        )
        return skimage.transform.resize(
            image_block, chunk_output_shape, *args, **kwargs
        ).astype(image_block.dtype)

    output_slices = tuple(slice(0, d) for d in output_shape)
    output = da.map_blocks(
        resize_block, image_prepared, dtype=image.dtype, chunks=block_output_shape
    )[output_slices]
    return output.rechunk(image.chunksize).astype(image.dtype)

@dataclass
class ZarrLevel:
    shape: Tuple[int]
    chunk_size: Tuple[int]
    dtype: str
    zarray: zarr.core.Array

# To do this you have to know how much you want to scale each dimension.
# This does not calculate chunk sizes for you.
def compute_level_shapes(lvl0shape:Tuple[int], scaling:Tuple[float], nlevels:int) -> List[Tuple[int]]:
    shapes = [lvl0shape]
    for i in range(nlevels-1):
        nextshape = (
            int(shapes[i][0] / scaling[0]),
            int(shapes[i][1] / scaling[1]),
            int(shapes[i][2] / scaling[2]),
            int(shapes[i][3] / scaling[3]),
            int(shapes[i][4] / scaling[4]),
        )
        shapes.append(nextshape)
    return shapes

class OmeZarrWriter:
    def __init__(self):
        self.output_path : str = ""
        self.levels : List[ZarrLevel] = []
        self.store : zarr.Store = None
        self.root : zarr.hierarchy.Group = None

    def init_store(self, output_path:str):
        self.output_path = output_path
        # assumes authentication/permission for writes
        is_remote = output_path.startswith("s3://") or output_path.startswith("gs://")
        if is_remote:
            self.store = FSStore(
                url=output_path, dimension_separator="/"
            )
        else:
            self.store = DirectoryStore(output_path, dimension_separator="/")
        # create a group with all the levels
        self.root = zarr.group(store=self.store, overwrite=True)
        # pre-create all levels here?
        self.create_levels(self.root)


    def create_levels(self, root, level_shapes, level_chunk_sizes, dtype):
        self.levels = []
        for i in range(len(level_shapes)):
            lvl = root.zeros(
                str(i), shape=level_shapes[i], chunks=level_chunk_sizes[i], dtype=dtype
            )
            level = ZarrLevel(level_shapes[i], level_chunk_sizes[i], dtype, lvl)
            self.levels.append(level)


    def downsample_and_write_batch_t(self, im: BioImage, start_t: int, end_t: int):
        dtype = im.dtype
        # assume start t and end t are in range (caller should guarantee this)
        ti = im.get_image_dask_data("TCZYX", T=slice(start_t, end_t))

        # write level 0 first
        ti = ti.persist()
        ti.compute()
        for k in range(start_t, end_t):
            self.levels[0].zarray[k] = ti[k - start_t]

        # downsample to next level then write
        for j in range(1, len(self.levels)):
            # downsample to next level
            nextshape = self.levels[j].shape
            ti = resize(ti, nextshape, order=0)
            ti = ti.astype(dtype)
            ti = ti.persist()
            ti.compute()
            # write ti to zarr
            # for some reason this is not working: not allowed to write in this way to a non-memory store
            # lvls[j][start_t:end_t] = ti[:]
            # lvls[j].set_basic_selection(slice(start_t,end_t), ti[:])
            for k in range(start_t, end_t):
                self.levels[j].zarray[k] = ti[k - start_t]
            # for some reason this is not working: not allowed to write in this way to a non-memory store
            # dask.array.to_zarr(ti, lvls[j], component=None, storage_options=None, overwrite=False, region=(slice(start_t,end_t)))

        log.info(f"Completed {start_t} to {end_t}")

    def write_t_batches(self, im: BioImage, tbatch:int=4):
        # loop over T in batches
        numT = im.dims.T
        log.info("Starting loop over T")
        for i in range(numT // tbatch):
            start_t = i * tbatch
            end_t = min((i + 1) * tbatch, numT)
            self.downsample_and_write_batch_t(im, start_t, end_t)
        log.info("Finished loop over T")

    def get_scale_ratio(self, level:int)->Tuple[float]:
        lvl_shape = self.levels[level].shape
        lvl0_shape = self.levels[0].shape
        return (lvl0_shape[0]/lvl_shape[0], 
                lvl0_shape[1]/lvl_shape[1], 
                lvl0_shape[2]/lvl_shape[2], 
                lvl0_shape[3]/lvl_shape[3], 
                lvl0_shape[4]/lvl_shape[4])

    def write_metadata(self, 
                       im: BioImage,
                       image_name:str, 
                       physical_pixel_size_factor:float=1.0, 
                       physical_pixel_size_units:str="micrometers",
                       time_interval:float=1.0,
                       time_units:str="milliseconds",
                       ):
        pixel_sizes = im.physical_pixel_sizes
        # write metadata
        physical_scale_0 = {
            "c": 1,
            "t": time_interval,
            "x": physical_pixel_size_factor * pixel_sizes.X
            if pixel_sizes.X
            else physical_pixel_size_factor,
            "y": physical_pixel_size_factor * pixel_sizes.Y
            if pixel_sizes.Y
            else physical_pixel_size_factor,
            "z": physical_pixel_size_factor * pixel_sizes.Z
            if pixel_sizes.Z
            else physical_pixel_size_factor,
        }
        dims= ("t", "c", "z", "y", "x")
        units={
            "x": physical_pixel_size_units,
            "y": physical_pixel_size_units,
            "z": physical_pixel_size_units,
            "t": time_units,
        }

        # shape = {"t":im3.dims.T, "c":im3.dims.C, "z":im3.dims.Z, "y":im3.dims.Y, "x":im3.dims.X}
        # scale = {"c":1, "t":1, "x":im3.physical_pixel_sizes.X, "y":im3.physical_pixel_sizes.Y, "z":im3.physical_pixel_sizes.Z},
        # levels = [{"x":2, "y":2, "z":1, "t":1, "c":1}, ...]
        axes = []
        for dim in dims:
            unit = None
            if units and dim in units:
                unit = units[dim]
            if dim in {"x", "y", "z"}:
                axis = Axis(name=dim, type="space", unit=unit)
            elif dim == "c":
                axis = Axis(name=dim, type="channel", unit=unit)
            elif dim == "t":
                axis = Axis(name=dim, type="time", unit=unit)
            else:
                msg = f"Dimension identifier is not valid: {dim}"
                raise KeyError(msg)
            axes.append(axis)
        
        datasets = []
        for index, level in enumerate(self.levels):
            path = f"{index}"
            scale = []
            level_scale = self.get_scale_ratio(index)
            level_scale = dim_tuple_to_dict(level_scale)
            for dim in dims:
                phys = physical_scale_0[dim] * level_scale[dim] if dim in physical_scale_0 and dim in level_scale else 1.0
                scale.append(phys)
            translation = []
            for dim in dims:
                # TODO handle optional translations e.g. xy stage position, start time etc
                translation.append(0.0)
            coordinateTransformations = [Scale(scale), Translation(translation)]
            dataset = Dataset(
                path=path, coordinateTransformations=coordinateTransformations
            )
            datasets.append(dataset)
        metadata = Metadata(
            axes=axes,
            datasets=datasets,
            name="/",
            coordinateTransformations=None,
        )
        self.root.attrs["multiscales"] = [asdict(metadata)]

        # get the total shape as dict:
        shapedict = dim_tuple_to_dict(self.levels[0].shape)

        # add the omero data
        ome_json = OmeZarrWriter.build_ome(
            shapedict["z"] if "z" in shapedict else 1,
            image_name,
            channel_names=im.channel_names,  # assumes we have written all channels!
            channel_colors=[],  # type: ignore
            # TODO: Rely on user to supply the per-channel min/max.
            channel_minmax=[(0.0, 1.0) for i in range(shapedict["c"] if "c" in shapedict else 1)],
        )
        self.root.attrs["omero"] = ome_json

        return metadata
