# allow for dask parallelism
from distributed import LocalCluster, Client

import os

# import pathlib
# from pathlib import Path
import s3fs
import bioio
from bioio.writers import OmeZarrWriter
from bioio import BioImage
from bioio.plugins import get_plugins, dump_plugins
from bioio_czi import Reader as CziReader
from bioio_ome_tiff import Reader as TiffReader
from aicsfiles import fms, FileLevelMetadataKeys
# import numpy
import pandas
import dask

# from ome_zarr.scale import Scaler

# import nuc_morph_analysis
# from nuc_morph_analysis.preprocessing.load_data import (
#     load_all_datasets,
#     load_dataset,
#     get_length_threshold_in_tps,
#     get_dataset_pixel_size,
# )


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
    cluster = LocalCluster(n_workers=4, processes=True, threads_per_worker=1)
    client = Client(cluster)
    print(client)
    print(client.dashboard_link)

    # dataset = load_drug_dataset()
    # (output_filename, filepath, channelinds) = get_recipe(dataset, 0)

    channelinds = None

    filepath = "\\\\allen\\aics\\assay-dev\\MicroscopyData\\Leveille\\2023\\20230425\\20230425_L02-01_processed.czi"
    info = {"fmsid": "c394ea65357e4c0384a9df2e74ae48de"}
    # filepath = "\\\\allen\\aics\\assay-dev\\MicroscopyData\\Leveille\\2023\\20230425\\20230425-L03-01_processed.czi"
    # info = {"fmsid": "6bff9d48c00844d786f3a530438417b6"}

    # we need to get list of segmentations
    datadir = None

    annotations = {
       FileLevelMetadataKeys.FILE_ID.value: info["fmsid"]
    }
    fms_file = list(fms.find(
        annotations=annotations,
        limit=1,
    ))[0]

    # Copy the file to a local directory
    #path_to_downloaded_file = fms_file.download(output_directory='/tmp')

    # Optionally symlink the file rather than copying it
    #symlinked_file_path = fms_file.download(output_directory='/tmp', as_sym_link=True)

    # hack init bioio
    get_plugins()
    dump_plugins()



    #if datadir is not None:
    #    record = fms.retrieve_file(info["fmsid"], datadir)[1]
    #else:
    #    record = fms.get_file_by_id(info["fmsid"])
    path = fms_file.path
    path = path.replace("/", "\\")
    path = "\\" + path
    df = pandas.read_csv(path, nrows=None).set_index("CellId")
    df = df[["index_sequence", "seg_full_zstack_path"]]
    df = df.sort_values(by=["index_sequence"])
    seg_paths = df.seg_full_zstack_path.unique()

    im = BioImage(filepath, reader=CziReader)
    print(str(im.dims.T) + " timepoints found")
    print(str(len(seg_paths)) + " segmentations found")
    im2 = BioImage(seg_paths[0], reader=TiffReader)
    print("Segmentation Info: ")
    print(str(im2.dims.X))
    print(str(im2.dims.Y))
    print(str(im2.dims.Z))
    print("Image Info: ")
    print(str(im.dims.X))
    print(str(im.dims.Y))
    print(str(im.dims.Z))

    #output_bucket = "animatedcell-test-data"
    # convert_to_zarr(filepath, output_bucket, channel_selection=channelinds)
