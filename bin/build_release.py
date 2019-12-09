#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import os
from pathlib import Path

from cellbrowser_tools import dataHandoffUtils
from cellbrowser_tools.createJobsFromCSV import process_images
from cellbrowser_tools.validateProcessedImages import validate_processed_images
from cellbrowser_tools.generateCellLineDef import generate_cellline_def

###############################################################################

log = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)

###############################################################################


def parse_args():
    parser = argparse.ArgumentParser(description='Process data set using options described in a json file.'
                                                 'Example: python build_release.py -c')

    # to generate images on cluster:
    # python createJobsFromCSV.py -c
    # to generate images serially:
    # python createJobsFromCSV.py -r

    # python createJobsFromCSV.py -c myprefs.json

    parser.add_argument(
        'prefs',
        nargs='?',
        default='prefs.json',
        help='prefs file'
    )

    parser.add_argument(
        '--first',
        type=int,
        help='how many FOVs to process',
        default=-1
    )

    runner = parser.add_mutually_exclusive_group()
    runner.add_argument(
        '--run',
        '-r',
        help='run the jobs locally',
        action='store_true',
        default=False
    )
    runner.add_argument(
        '--cluster',
        '-c',
        help='run jobs using the cluster',
        action='store_true',
        default=False
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    prefs = dataHandoffUtils.setup_prefs(args.prefs)

    process_images(args, prefs)
    validate_processed_images(args, prefs)


if __name__ == "__main__":
    main()
