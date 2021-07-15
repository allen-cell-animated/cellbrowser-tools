import argparse
import csv
import logging
import os
import sys
import traceback

from datetime import datetime
from logging import FileHandler, StreamHandler, Formatter
from cellbrowser_tools.dataHandoffUtils import (
    QueryOptions,
    ActionOptions,
    OutputPaths,
    get_data_groups2,
)
from cellbrowser_tools.dataset_constants import DataField, FILE_LIST_FILENAME


class Args(argparse.Namespace):
    def __init__(self):
        super().__init__()
        self.output_dir = "./output/make_images"
        self.input_manifest = ""
        self.env = "stg"
        self.debug = False
        self.cell_lines = None
        self.plates = None
        self.fovids = None
        self.start_date = None
        self.end_date = None
        self.do_crop = False
        #
        self.__parse()

    def __parse(self):
        p = argparse.ArgumentParser(
            prog="Make downloader manifest",
            description="Generates csv file with input data for downloader dataset",
        )
        p.add_argument(
            "--input_manifest",
            type=str,
            help="csv file containing dataset source data",
            required=True,
        )
        p.add_argument(
            "--output_dir",
            type=str,
            help="directory where outputs should be saved (can be isilon)",
            default="./output/make_downloader_manifest",
            required=False,
        )
        p.add_argument(
            "--env",
            choices=["dev", "stg", "prod"],
            help="AICS Labkey / Data platform environment to use (default is 'stg')",
            default="stg",
            required=False,
        )
        p.add_argument(
            "--debug",
            help="Enable debug mode",
            default=False,
            required=False,
            action="store_true",
        )
        actions_group = p.add_argument_group(
            "Actions",
            "Combine any of the following flags to determine the image outputs",
        )
        actions_group.add_argument(
            "--do_crop",
            help="Generate cropped child images",
            default=False,
            required=False,
            action="store_true",
        )
        filter_group = p.add_argument_group(
            "Filter options",
            "Combine any of the following options to filter FOVs that will be processed.",
        )
        filter_group.add_argument(
            "--cell_lines",
            nargs="+",
            help="Array of Cell-lines to run. E.g. --cell_lines 'AICS-11' 'AICS-7' ",
            default=None,
            required=False,
        )
        filter_group.add_argument(
            "--plates",
            nargs="+",
            help="Array of plates to run. E.g. --plates '3500003813' '3500003642' ",
            default=None,
            required=False,
        )
        filter_group.add_argument(
            "--fovids",
            nargs="+",
            help="Array of fovids to run. E.g. --fovs '123' '6' ",
            default=None,
            required=False,
        )
        filter_group.add_argument(
            "--start_date",
            type=str,
            help="Filter on FOVs created on or after a specific date (inclusive). Date format YYYY-MM-DD. Ex: '2020-12-01' ",
            default=None,
            required=False,
        )
        filter_group.add_argument(
            "--end_date",
            type=str,
            help="Filter on FOVs created on or before a specific date (inclusive). Date format YYYY-MM-DD. Ex: '2020-12-01' ",
            default=None,
            required=False,
        )
        p.parse_args(namespace=self)


###############################################################################


def configure_logging(debug: bool):
    f = Formatter(fmt="[%(asctime)s][%(levelname)s] %(message)s")
    streamHandler = StreamHandler()
    streamHandler.setFormatter(f)
    fileHandler = FileHandler(
        filename=f"make_images_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log",
        mode="w",
    )
    fileHandler.setFormatter(f)
    log = logging.getLogger()  # root logger
    log.handlers = [streamHandler, fileHandler]  # overwrite handlers
    log.setLevel(logging.DEBUG if debug else logging.INFO)


def main():
    args = Args()
    debug = args.debug
    configure_logging(debug)
    log = logging.getLogger(__name__)

    try:
        log.info("Start make_images")
        log.info(f"Environment: {args.env}")
        log.info(args)

        query_options = QueryOptions(
            args.fovids, args.plates, args.cell_lines, args.start_date, args.end_date,
        )
        action_options = ActionOptions(
            args.do_thumbnails, args.do_atlases, args.do_crop
        )

        # setup directories
        output_paths = OutputPaths(args.output_dir)

        # gather data set
        groups = get_data_groups2(args.input_manifest, query_options, args.output_dir)

        # TODO log the command line args
        # statusdir = output_paths.status_dir
        # prefspath = Path(f"{statusdir}/prefs.json").expanduser()
        # shutil.copyfile(p.prefs, prefspath)

        outrows = []
        for index, rows in enumerate(groups):
            # use row 0 as the "full field" row
            fovrow = rows[0]
            outrows.append(
                {
                    "file_id": str(fovrow[DataField.FOVId]),
                    "file_name": fovrow[DataField.SourceFilename],
                    "read_path": fovrow[DataField.SourceReadPath],
                    "file_size": os.path.getsize(fovrow[DataField.SourceReadPath]),
                    # "other_keys": "other_values"
                }
            )
        keys = outrows[0].keys()
        with open(
            os.path.join(output_paths.out_dir, FILE_LIST_FILENAME), "w", newline="",
        ) as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(outrows)

        log.info("All done!")

    except Exception as e:
        log.error("=============================================")
        log.error("\n\n" + traceback.format_exc())
        log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")
        sys.exit(1)


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()
