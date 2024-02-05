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
from bioio_nd2 import Reader as ND2Reader
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
from cellbrowser_tools.fms_util import fms_id_to_path
from cellbrowser_tools.ome_zarr_writer import OmeZarrWriter

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

if __name__ == "__main__":
    # aws config
    os.environ["AWS_PROFILE"] = "animatedcell"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
    # initialize for writing direct to S3
    s3 = s3fs.S3FileSystem(anon=False, config_kwargs={"connect_timeout": 60})

    ###########################
    # INFRASTRUCTURE INIT
    ###########################

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
    ############################
    ## END INFRASTRUCTURE INIT
    ############################

    channelinds = None

    filepath = "\\\\allen\\aics\\assay-dev\\MicroscopyData\\Leveille\\2023\\20230425\\20230425_L02-01_processed.czi"
    filepath = normalize_path(filepath)
    # info = {"fmsid": "c394ea65357e4c0384a9df2e74ae48de"}  # lattice3
    # filepath = "\\\\allen\\aics\\assay-dev\\MicroscopyData\\Leveille\\2023\\20230425\\20230425-L03-01_processed.czi"

    # info = {
    #     "fmsid": "0709695427454d788852ca50b838cf5b",
    #     "name": "baby_bear",
    #     "pixel_size": 0.108,
    #     "original_fmsid": "7191a69c6d8f4f37b7a43cc962c72935",
    #     "scene": 8,
    # }
    # info = {
    #     "name": "goldilocks",
    #     "fmsid": "22e6f39eef954b7a99575676377da47f",
    #     "pixel_size": 0.108,
    #     "original_fmsid": "7191a69c6d8f4f37b7a43cc962c72935",
    #     "scene": 5,
    #     "time_interval": 5,  # min
    #     "length_threshold": 10.0,  # hours,
    #     "experiment": "ZSD-control",
    #     "overview": None,
    # }
    info = {
        "name": "mama_bear",
        "fmsid": "9dbaf24f86124b96bd5f5b10ce9f892f",
        "pixel_size": 0.108,
        "original_fmsid": "7191a69c6d8f4f37b7a43cc962c72935",
        "scene": 4,
        "time_interval": 5,#min
        "length_threshold": 10.0,#hours,
        "experiment": "ZSD-control",
        "overview": None
    }

    # path = fms_id_to_path(fms_id=info["fmsid"])
    # df = pandas.read_csv(path, nrows=None).set_index("CellId")
    # df = df[["index_sequence", "seg_full_zstack_path", "raw_full_zstack_path"]]
    # df = df.sort_values(by=["index_sequence"])
    # seg_paths = df.seg_full_zstack_path.unique()
    # raw_paths = df.raw_full_zstack_path.unique()

    # original_path = fms_id_to_path(fms_id=info["original_fmsid"])
    # im = BioImage(original_path, reader=CziReader)

    original_path = (
        "/allen/aics/microscopy/Antoine/Nikon test/3500006064_20X_water002.nd2"
    )
    im = BioImage(original_path, reader=ND2Reader)
    im.set_scene(info["scene"])
    original_dims = im.dims
    numT = im.dims.T
    # print(str(im.dims.T) + " original timepoints found")
    # print(str(len(seg_paths)) + " segmentation timepoints found")
    # print(str(len(raw_paths)) + " raw timepoints found")
    # numT = min(len(seg_paths), len(raw_paths), im.dims.T)

    output_filename = info["name"]  # os.path.splitext(os.path.basename(filepath))[0]
    output_filename = (
        os.path.splitext(os.path.basename(original_path))[0] + "_" + info["name"]
    )

    pixel_size = 1.0  # info["pixel_size"]

    print("Image Info: ")
    print(str(im.dims.X))
    print(str(im.dims.Y))
    print(str(im.dims.Z))
    # im2 = BioImage(seg_paths[0])
    # print("Segmentation Info: ")
    # print(str(im2.dims.X))
    # print(str(im2.dims.Y))
    # print(str(im2.dims.Z))
    # im3 = BioImage(raw_paths[0])
    # print("Raw cropped Info: ")
    # print(str(im3.dims.X))
    # print(str(im3.dims.Y))
    # print(str(im3.dims.Z))

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

    # TODO determine how many levels
    nlevels = 5
    # TODO determine customized scaling
    inv_scaling = {"t": 1.0, "c": 1.0, "z": 1.0, "y": 2.0, "x": 2.0}
    scaling = {d: 1.0 / inv_scaling[d] for d in inv_scaling}

    # compute chunk sizes
    zarr_chunk_dims_tuples = []
    for i in range(nlevels):
        zarr_chunk_dims_tuples.append(
            (
                1,
                1,
                (int(inv_scaling["y"] * inv_scaling["x"]) ** i),
                int(im.dims.Y * (scaling["y"] ** i)),
                int(im.dims.X * (scaling["x"] ** i)),
            )
        )

    # compute level shapes
    lvl_shape = (im.dims.T, im.dims.C, im.dims.Z, im.dims.Y, im.dims.X)
    lvl_shapes = [lvl_shape]
    for i in range(1, nlevels):
        lvl_shape = (
            int(lvl_shape[0] * scaling["t"]),
            int(lvl_shape[1] * scaling["c"]),
            int(lvl_shape[2] * scaling["z"]),
            int(lvl_shape[3] * scaling["y"]),
            int(lvl_shape[4] * scaling["x"]),
        )
        lvl_shapes.append(lvl_shape)


    # construct some per-channel lists to feed in to the writer.
    # hardcoding to 2 for now
    channel_colors = [
        0xFFFFFF
        for i in range(im.dims.C)
    ]
    channel_names = im.channel_names  # ["raw"]  # , "seg"]


    writer = OmeZarrWriter()
    
    output_bucket = "animatedcell-test-data"
    writer.init_store(output_path=f"s3://{output_bucket}/{output_filename}.zarr", 
                      shapes=lvl_shapes, 
                      chunk_sizes=zarr_chunk_dims_tuples, 
                      dtype=im.dtype
    )

    writer.write_t_batches(im, 4)

    physical_scale = {
        "c": 1,
        "t": info.time_interval,
        "x": pixel_size * im.physical_pixel_sizes.X
        if im.physical_pixel_sizes.X
        else pixel_size,
        "y": pixel_size * im.physical_pixel_sizes.Y
        if im.physical_pixel_sizes.Y
        else pixel_size,
        "z": pixel_size * im.physical_pixel_sizes.Z
        if im.physical_pixel_sizes.Z
        else pixel_size,
    }
    physical_units = {
        "x": "micrometer",
        "y": "micrometer",
        "z": "micrometer",
        "t": "minute",
    }
    writer.write_metadata(image_name=output_filename,
                          channel_names=channel_names,
                          physical_dims=physical_scale,
                          physical_units=physical_units,
                          )



