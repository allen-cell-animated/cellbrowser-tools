import argparse
import logging
import sys
import traceback

from datetime import datetime
from logging import FileHandler, StreamHandler, Formatter
from cellbrowser_tools.dataHandoffUtils import (
    OutputPaths,
    get_data_groups2,
    normalize_path,
)
from cellbrowser_tools.validateProcessedImages import (
    create_variance_dataset_from_features,
)


class Args(argparse.Namespace):
    def __init__(self):
        super().__init__()
        self.output_dir = "./output/make_dataset_from_csv"
        self.input_manifest = ""
        self.env = "stg"
        self.debug = False
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
        p.parse_args(namespace=self)


###############################################################################


def configure_logging(debug: bool):
    f = Formatter(fmt="[%(asctime)s][%(levelname)s] %(message)s")
    streamHandler = StreamHandler()
    streamHandler.setFormatter(f)
    fileHandler = FileHandler(
        filename=f"make_dataset_from_csv_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log",
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
        log.info("Start make_dataset_from_csv")
        log.info(f"Environment: {args.env}")
        log.info(args)

        # setup directories
        # output_paths = OutputPaths(args.output_dir)

        # gather data set
        create_variance_dataset_from_features(args.input_manifest, args.output_dir)

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
