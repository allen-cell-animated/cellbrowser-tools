#!/usr/bin/env python

from cellbrowser_tools import cellJob, zarr_fov_processing

import argparse
from distributed import LocalCluster, Client
import json
import sys
import traceback


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
    return zarr_fov_processing.do_main_image_with_celljob(info)


def main():
    parser = argparse.ArgumentParser(
        description="Process data set defined in csv files, and prepare for ingest into bisque db."
        "Example: python processImageWithSegmentation.py /path/to/csv --outpath /path/to/destination/dir"
    )
    parser.add_argument("input", help="input json file")
    args = parser.parse_args()

    # Set up for dask processing as a global thing

    # # cluster = LocalCluster(processes=True)
    # cluster = LocalCluster(n_workers=4, processes=True, threads_per_worker=1)
    # # cluster = LocalCluster(memory_limit="7GB")  # threaded instead of multiprocess
    # # cluster = LocalCluster(n_workers=4, processes=True, threads_per_worker=1, memory_limit="12GB")
    # client = Client(cluster)
    # # client

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
