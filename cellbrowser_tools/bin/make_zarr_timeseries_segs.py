# allow for dask parallelism
from dask.distributed import Client, LocalCluster

import os
from typing import Tuple, Any, List, Optional
import atexit
import signal
import sys
from pathlib import Path
from dataclasses import asdict
import logging

# import pathlib
# from pathlib import Path
import s3fs
import bioio
from bioio_base.dimensions import DimensionNames
from bioio.writers import OmeZarrWriter
from bioio import BioImage
from bioio.plugins import get_plugins, dump_plugins
from bioio_czi import Reader as CziReader
from bioio_ome_tiff import Reader as TiffReader
from aicsfiles import fms, FileLevelMetadataKeys

import numpy as np
import pandas
import dask
from dask import array as da
from zarr.storage import DirectoryStore, FSStore
import zarr
import skimage

from cellbrowser_tools.dataHandoffUtils import normalize_path

from ngff_zarr import config, to_ngff_image, to_multiscales, to_ngff_zarr, Methods
from ngff_zarr.rich_dask_progress import NgffProgress
from ngff_zarr.zarr_metadata import Metadata, Axis, Scale, Translation, Dataset
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.pretty import Pretty
from rich.progress import (
    MofNCompleteColumn,
    SpinnerColumn,
    TimeElapsedColumn,
)
from rich.progress import (
    Progress as RichProgress,
)
from rich.spinner import Spinner

log = logging.getLogger(__name__)

# import nuc_morph_analysis
# from nuc_morph_analysis.preprocessing.load_data import (
#     load_all_datasets,
#     load_dataset,
#     get_length_threshold_in_tps,
#     get_dataset_pixel_size,
# )

# def dask_scale(data: dask.Array, num_levels: int, scale_factor: Tuple, method="nearest"):
#     """
#     Downsample a dask array by a given factor using a given method.
#     """
#     # get the shape of the data
#     shape = data.shape
#     # get the number of dimensions
#     ndim = len(shape)
#     # get the number of chunks in each dimension
#     chunks = data.chunks


def load_drug_dataset():
    manifest_path = (
        "C:\\Users\\danielt\\Downloads\\drug_cell_meta_pilot_Danhandoff2023.csv"
    )
    dataset = pandas.read_csv(manifest_path)
    dataset.drop_duplicates(subset=["SourceFilename"], inplace=True)
    print("Num Rows: " + str(dataset.shape[0]))
    print("Len: " + str(len(dataset)))
    return dataset


def get_recipe(dataset, i):
    print(dataset.iloc[i])
    output_filename = dataset.iloc[i]["SourceFilename"]
    filepath = dataset.iloc[i]["SourceReadPath"]
    filepath = os.path.join(filepath, output_filename)
    channelinds = [
        dataset.iloc[i]["ChannelNumber638"] - 1,
        dataset.iloc[i]["ChannelNumberStruct"] - 1,
        dataset.iloc[i]["ChannelNumber405"] - 1,
        dataset.iloc[i]["ChannelNumberBrightfield"] - 1,
    ]
    print(channelinds)
    print(filepath)
    return (output_filename, filepath, channelinds)


def write_scene(
    storeroot, scenename, sceneindex, img, channel_selection=None, levels=4, scaling=2.0
):
    # construct some per-channel lists to feed in to the writer.
    # hardcoding to 9 for now
    channel_colors = [
        0xFF0000,
        0x00FF00,
        0x0000FF,
        0xFFFF00,
        0xFF00FF,
        0x00FFFF,
        0x880000,
        0x008800,
        0x000088,
    ]

    print(scenename)
    print(storeroot)
    img.set_scene(sceneindex)
    pps = img.physical_pixel_sizes
    cn = img.channel_names

    data = img.get_image_dask_data("TCZYX")
    print(data.shape)

    if channel_selection is not None:
        cn = [cn[i] for i in channel_selection]
        data = dask.array.take(data, channel_selection, axis=1)

    writer = OmeZarrWriter(storeroot)

    writer.write_image(
        image_data=data,
        image_name=scenename,
        physical_pixel_sizes=pps,
        channel_names=cn,
        channel_colors=channel_colors,
        scale_num_levels=levels,
        scale_factor=scaling,
    )


# f = "//allen/programs/allencell/data/proj0/47f/7da/637/567/957/7a9/54b/ef9/39e/e6f/22/lineage_goldilocks_manifest_20230315_2.csv"
# p = Path(f)

# df = pandas.read_csv(f).set_index("CellId")
# df["dataset"] = dataset

# # set track id and frame numbers to integer type
# cols = ["track_id", "index_sequence", "T_index"]
# df[cols] = df[cols].astype(int)
# df = df.sort_values(by=cols)

# print(f"Dataset loaded.")
# print(f"{df.shape[0]} single-timepoint nuclei in dataset.")
# print(f"{df.track_id.nunique()} nuclear tracks in dataset.")


def convert_to_zarr(
    filepath,
    output_bucket="animatedcell-test-data",
    levels=4,
    scaling=2.0,
    channel_selection=None,
):
    output_filename = os.path.splitext(os.path.basename(filepath))[0]
    print(output_filename)

    # load our image
    chunk_dims = [
        DimensionNames.SpatialY,
        DimensionNames.SpatialX,
        DimensionNames.Samples,
    ]
    img = BioImage(filepath, chunk_dims=chunk_dims)

    # print some data about the image we loaded
    scenes = img.scenes
    print("Image Info:")
    print("  Number of Scenes: " + str(len(scenes)))
    print("  Scenes: " + str(scenes))
    print("  Channels: " + str(img.channel_names))
    print("  Physical Pixel Size (S0): " + str(img.physical_pixel_sizes))
    print("  Scene 0 Shape: " + str(img.shape))

    # methods = Scaler.methods()
    # print(list(methods))

    scene_indices = range(len(img.scenes))
    if len(scene_indices) == 1:
        write_scene(
            f"s3://{output_bucket}/{output_filename}.zarr/",
            "",
            0,
            img,
            channel_selection,
            levels,
            scaling,
        )
    else:
        # here we are splitting multi-scene images into separate zarr images based on scene name
        for i in scene_indices:
            scenename = img.scenes[i]
            scenename = scenename.replace(":", "_")
            write_scene(
                f"s3://{output_bucket}/{output_filename}/{scenename}.zarr/",
                scenename,
                i,
                img,
                channel_selection,
                levels,
                scaling,
            )


def generate_metadata(image_name:str, shape: dict, dims: Tuple[str], physicalscale: dict, units: Optional[dict], levels: List[dict], channel_names: List[str], channel_colors:List[int], group):
    # dims= ("t", "c", "z", "y", "x"),
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
    for index, scaledict in enumerate(levels):
        path = f"{index}"
        scale = []
        for dim in dims:
            phys = physicalscale[dim] if dim in physicalscale and physicalscale[dim] else 1.0
            if dim in scaledict:
                scale.append(scaledict[dim] * phys)
            else:
                scale.append(phys)
        translation = []
        for dim in dims:
            translation.append(0.0)
            # if dim in translation:
            #     translation.append(translation[dim])
            # else:
            #     translation.append(1.0)
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
    group.attrs["multiscales"] = [asdict(metadata)]

    ome_json = OmeZarrWriter.build_ome(
        shape["z"] if "z" in shape else 1,
        image_name,
        channel_names=channel_names,  # type: ignore
        channel_colors=channel_colors,  # type: ignore
        # This can be slow if computed here.
        # TODO: Rely on user to supply the per-channel min/max.
        channel_minmax=[
            (0.0, 1.0)
            for i in range(shape["c"] if "c" in shape else 1)
        ],
    )
    group.attrs["omero"] = ome_json

    return metadata


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



if __name__ == "__main__":
    ###########################
    # Infrastructure init
    ###########################
    # aws config
    os.environ["AWS_PROFILE"] = "animatedcell"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
    # initialize for writing direct to S3
    s3 = s3fs.S3FileSystem(anon=False, config_kwargs={"connect_timeout": 60})

    # configure for parallelism
    n_workers = 4
    worker_memory_target = config.memory_target // n_workers
    try:
        import psutil
        n_workers = psutil.cpu_count(False) // 2
        worker_memory_target = config.memory_target // n_workers
    except ImportError:
        pass

    cluster = LocalCluster(
        n_workers=n_workers,
        memory_limit=worker_memory_target,
        processes=True,
        threads_per_worker=2,
    )
    client = Client(cluster)

    def shutdown_client(sig_id, frame):  # noqa: ARG001
        client.shutdown()

    atexit.register(shutdown_client, None, None)
    signal.signal(signal.SIGTERM, shutdown_client)
    signal.signal(signal.SIGINT, shutdown_client)

    print(client)
    print(client.dashboard_link)

    # dataset = load_drug_dataset()
    # (output_filename, filepath, channelinds) = get_recipe(dataset, 0)

    channelinds = None

    filepath = "\\\\allen\\aics\\assay-dev\\MicroscopyData\\Leveille\\2023\\20230425\\20230425_L02-01_processed.czi"
    filepath = normalize_path(filepath)
    info = {"fmsid": "c394ea65357e4c0384a9df2e74ae48de"}
    # filepath = "\\\\allen\\aics\\assay-dev\\MicroscopyData\\Leveille\\2023\\20230425\\20230425-L03-01_processed.czi"
    # info = {"fmsid": "6bff9d48c00844d786f3a530438417b6"}

    # we need to get list of segmentations
    datadir = None

    annotations = {FileLevelMetadataKeys.FILE_ID.value: info["fmsid"]}
    fms_file = list(
        fms.find(
            annotations=annotations,
            limit=1,
        )
    )[0]
    path = fms_file.path
    path = normalize_path(path)
    # path = path.replace("/", "\\")
    # path = "\\" + path
    df = pandas.read_csv(path, nrows=None).set_index("CellId")
    df = df[["index_sequence", "seg_full_zstack_path", "raw_full_zstack_path"]]
    df = df.sort_values(by=["index_sequence"])
    seg_paths = df.seg_full_zstack_path.unique()
    raw_paths = df.raw_full_zstack_path.unique()

    im = BioImage(filepath, reader=CziReader)
    print(str(im.dims.T) + " original timepoints found")
    print(str(len(seg_paths)) + " segmentation timepoints found")
    print("Image Info: ")
    print(str(im.dims.X))
    print(str(im.dims.Y))
    print(str(im.dims.Z))
    im2 = BioImage(seg_paths[0], reader=TiffReader)
    print("Segmentation Info: ")
    print(str(im2.dims.X))
    print(str(im2.dims.Y))
    print(str(im2.dims.Z))
    im3 = BioImage(raw_paths[0], reader=TiffReader)
    print("Raw cropped Info: ")
    print(str(im3.dims.X))
    print(str(im3.dims.Y))
    print(str(im3.dims.Z))

    numT = len(seg_paths)  # min(len(seg_paths), 2)

    # make dask chunks large.
    # dask best practices say to use at least 100MB per chunk.
    # but also want to keep chunk size a multiple of our
    # output chunk size.  We know for zarr, output chunks will
    # be much smaller.
    bioio_chunk_dims = [
        DimensionNames.SpatialZ,
        DimensionNames.SpatialY,
        DimensionNames.SpatialX,
        DimensionNames.Samples,
    ]
    zarr_chunk_dims = []
    zarr_chunk_dims_lists = []
    # TODO - allow different zarr chunk dims for each downsample
    # TODO - bioio input chunk dims are too coarse-grained

    # TODO determine nlevels to go down far enough for effective visualization
    nlevels = 5
    
    inv_scaling = {"t":1.0, "c":1.0, "z":1.0, "y":2.0, "x":2.0}
    scaling = {d:1.0/inv_scaling[d] for d in inv_scaling}
    for i in range(nlevels):
        zarr_chunk_dims.append(
            {
                "t":1,
                "c":1,
                "z":(int(inv_scaling["y"] * inv_scaling["x"]) ** i),
                "y":int(im3.dims.Y * (scaling["y"]**i)),
                "x":int(im3.dims.X * (scaling["x"]**i)),
            }
        )
        zarr_chunk_dims_lists.append(
            [1,1,(int(inv_scaling["y"] * inv_scaling["x"]) ** i),int(im3.dims.Y * (scaling["y"]**i)),int(im3.dims.X * (scaling["x"]**i))]
        )

    # load all data into a nice big delayed array
    data = []
    for i in range(numT):
        im = BioImage(raw_paths[i], reader=TiffReader, chunk_dims=bioio_chunk_dims)
        data_raw = im.get_image_dask_data("CZYX")
        data.append(data_raw)
        # # attach segmentations as channel
        # im2 = BioImage(seg_paths[i], reader=TiffReader, chunk_dims=bioio_chunk_dims)
        # data_seg = im2.get_image_dask_data("CZYX")
        # all = dask.array.concatenate((data_raw, data_seg), axis=0)
        # data.append(all)
    # now the outer list data is dimension T and the inner items are all CZYX
    data = dask.array.stack(data)
    print("Dask array has been built")
    print(data.shape)

    output_bucket = "animatedcell-test-data"
    output_filename = os.path.splitext(os.path.basename(filepath))[0]

    ####################
    ## USE NGFF_ZARR
    ####################
    # console = Console()
    # progress = RichProgress(
    #     SpinnerColumn(),
    #     MofNCompleteColumn(),
    #     TimeElapsedColumn(),
    #     *RichProgress.get_default_columns(),
    #     transient=False,
    #     console=console,
    # )
    # rich_dask_progress = NgffProgress(progress)

    # ngff_image = to_ngff_image(data,
    #                            dims= ("t", "c", "z", "y", "x"),
    #                            scale = {"c":1, "t":1, "x":im3.physical_pixel_sizes.X, "y":im3.physical_pixel_sizes.Y, "z":im3.physical_pixel_sizes.Z},
    #                            translation = None,
    #                            name = "image",
    #                            axes_units = None
    # )
    # multiscales = to_multiscales(
    #     ngff_image,
    #     method=Methods.DASK_IMAGE_NEAREST,
    #     progress=rich_dask_progress,
    #     chunks=zarr_chunk_dims[0],
    #     cache= (ngff_image.data.nbytes > config.memory_target),
    #     scale_factors=[{"x":2, "y":2, "z":1, "t":1, "c":1}]
    # )
    # output_store = DirectoryStore(f"s3://{output_bucket}/{output_filename}_with_seg.zarr", dimension_separator="/")
    # to_ngff_zarr(
    #     output_store,
    #     multiscales,
    #     progress=rich_dask_progress
    # )
    ####################
    ## END USE NGFF_ZARR
    ####################

    # construct some per-channel lists to feed in to the writer.
    # hardcoding to 2 for now
    channel_colors = [
        0xFF0000,
        # 0x00FF00
    ]
    channel_names = ["raw"]  # , "seg"]

    ####################
    ## USE BIOIO
    ####################
    # writer = OmeZarrWriter(f"s3://{output_bucket}/{output_filename}_with_seg.zarr/")
    # writer.write_image(
    #     image_data=data,
    #     image_name="",
    #     physical_pixel_sizes=im3.physical_pixel_sizes,
    #     channel_names=channel_names,
    #     channel_colors=channel_colors,
    #     scale_num_levels=nlevels,
    #     scale_factor=scale_factor,
    #     chunk_dims=zarr_chunk_dims_lists,
    # )
    ####################
    ## END USE BIOIO
    ####################

    # init zarr store as a group.
    # each multiresolution level will be a zarr array
    # but we will write into each group by breadth
    # loop over T
    # for each t in T
    #   load level 0 as a dask array
    #   write level 0 to zarr group 0 at level=0 / T=t
    #   downsample and write other levels

    output_store = FSStore(url=f"s3://{output_bucket}/{output_filename}_TEST.zarr", dimension_separator="/")
    # output_store = DirectoryStore(f"c:/{output_bucket}/{output_filename}_TEST.zarr", dimension_separator="/")
    # create a group with all the levels
    root = zarr.group(store=output_store, overwrite=True)

    # set up levels
    lvl_shape = data.shape
    lvls = []
    for i in range(nlevels):
        lvl = root.zeros(str(i), shape=lvl_shape, chunks=zarr_chunk_dims_lists[i])
        lvls.append(lvl)
        lvl_shape = (lvl_shape[0]*scaling["t"], lvl_shape[1]*scaling["c"], lvl_shape[2]*scaling["z"], lvl_shape[3]*scaling["y"], lvl_shape[4]*scaling["x"])
        lvl_shape = (int(lvl_shape[0]), int(lvl_shape[1]), int(lvl_shape[2]), int(lvl_shape[3]), int(lvl_shape[4]))

    # loop over T in batches
    log.debug("Starting loop over T")
    tbatch = 4
    for i in range(numT//tbatch):
        start_t = i*tbatch
        end_t = min((i+1)*tbatch, numT)
        ti = data[start_t:end_t]
        # ti is level0's TCZYX data. 
        # we can write it right now and then downsample
        for j in range(nlevels):
            ti = ti.persist()
            ti.compute()
            # write ti to zarr
            # for some reason this is not working: not allowed to write in this way to a non-memory store
            # lvls[j][start_t:end_t] = ti[:]
            # lvls[j].set_basic_selection(slice(start_t,end_t), ti[:])
            for k in range(start_t, end_t):
                lvls[j][k] = ti[k-start_t]
            # for some reason this is not working: not allowed to write in this way to a non-memory store
            # dask.array.to_zarr(ti, lvls[j], component=None, storage_options=None, overwrite=False, region=(slice(start_t,end_t)))
            # downsample to next level
            nextshape = (int(ti.shape[0]/inv_scaling["t"]),
                         int(ti.shape[1]/inv_scaling["c"]),
                         int(ti.shape[2]/inv_scaling["z"]),
                         int(ti.shape[3]/inv_scaling["y"]),
                         int(ti.shape[4]/inv_scaling["x"]))
            ti = resize(ti, nextshape, order=0)
            ti = ti.astype("uint16")
        log.debug(f"Completed {start_t} to {end_t}")
        # ti = data[i]
        # # ti is level0's CZYX data. 
        # # we can write it right now and then downsample
        # for j in range(nlevels):
        #     ti = ti.persist()
        #     ti.compute()
        #     lvls[j][i] = ti
        #     # downsample to next level
        #     nextshape = (int(ti.shape[0]/inv_scaling["c"]),
        #                  int(ti.shape[1]/inv_scaling["z"]),
        #                  int(ti.shape[2]/inv_scaling["y"]),
        #                  int(ti.shape[3]/inv_scaling["x"]))
        #     ti = resize(ti, nextshape, order=0)
        #     ti = ti.astype("uint16")
    log.debug("Finished loop over T")

    # write metadata
    physical_scale = {"c":1,
                      "t":1,
                      "x":im3.physical_pixel_sizes.X if im3.physical_pixel_sizes.X else 1.0,
                      "y":im3.physical_pixel_sizes.Y if im3.physical_pixel_sizes.Y else 1.0,
                      "z":im3.physical_pixel_sizes.Z if im3.physical_pixel_sizes.Z else 1.0}
    generate_metadata(image_name=output_filename,
                      shape={"t":numT, "c":1, "z":im3.dims.Z, "y":im3.dims.Y, "x":im3.dims.X},
                      # shape={"t":data.shape[0], "c":data.shape[1], "z":data.shape[2], "y":data.shape[3], "x":data.shape[4]},
                      dims=("t", "c", "z", "y", "x"),
                      physicalscale = physical_scale,
                      units={"x":"micrometer", "y":"micrometer", "z":"micrometer", "t":"millisecond"},
                      levels= [{"x":inv_scaling["x"]**i, "y":inv_scaling["y"]**i, "z":1, "t":1, "c":1} for i in range(nlevels)],
                      channel_names=channel_names, channel_colors=channel_colors, group=root)
    # dims= ("t", "c", "z", "y", "x"),
    # shape = {"t":im3.dims.T, "c":im3.dims.C, "z":im3.dims.Z, "y":im3.dims.Y, "x":im3.dims.X}
    # scale = {"c":1, "t":1, "x":im3.physical_pixel_sizes.X, "y":im3.physical_pixel_sizes.Y, "z":im3.physical_pixel_sizes.Z},
    # levels = [{"x":2, "y":2, "z":1, "t":1, "c":1}, ...]

