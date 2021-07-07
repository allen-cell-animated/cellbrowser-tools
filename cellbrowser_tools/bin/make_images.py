import argparse
import logging
import sys
import traceback

from datetime import datetime
from logging import FileHandler, StreamHandler, Formatter
from cellbrowser_tools.dataHandoffUtils import QueryOptions
import cellbrowser_tools.build_images


class Args(argparse.Namespace):
    def __init__(self):
        super().__init__()
        self.output_dir = "./output/make_images"
        self.input_manifest = ""
        self.env = "stg"
        self.distributed = False
        self.debug = False
        self.cell_lines = None
        self.plates = None
        self.fovids = None
        self.start_date = None
        self.end_date = None
        #
        self.__parse()

    def __parse(self):
        p = argparse.ArgumentParser(
            prog="Make images",
            description="Generates volume-viewer files for a series of images with or without segmentations.",
        )
        p.add_argument(
            "--output_dir",
            type=str,
            help="directory where outputs should be saved (can be isilon)",
            default="./output/make_images",
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
        filter_group = p.add_argument_group(
            "Filter options",
            "Combine any of the following options to filter FOVs that will be processed.",
        )
        filter_group.add_argument(
            "--cell_lines",
            nargs="+",
            help="Array of Cell-lines to run segmentations on. E.g. --cell_lines 'AICS-11' 'AICS-7' ",
            default=None,
            required=False,
        )
        filter_group.add_argument(
            "--plates",
            nargs="+",
            help="Array of plates to run segmentations on. E.g. --plates '3500003813' '3500003642' ",
            default=None,
            required=False,
        )
        filter_group.add_argument(
            "--fovids",
            nargs="+",
            help="Array of fovids to run segmentations on. E.g. --fovs '123' '6' ",
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
        distributed = p.add_argument_group("distributed", "Distributed run options")
        distributed.add_argument(
            "--distributed",
            help="Run in distributed mode (default is False)",
            default=False,
            required=False,
            action="store_true",
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
        build_images.build_images(
            args.input_manifest, args.output_dir, args.distributed, query_options
        )

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
