import argparse
import logging
import sys
import traceback

from datetime import datetime
from logging import FileHandler, StreamHandler, Formatter
from cellbrowser_tools.fms_util import fms_id_to_path


class Args(argparse.Namespace):
    def __init__(self):
        super().__init__()
        self.fms_id = ""
        #
        self.__parse()

    def __parse(self):
        p = argparse.ArgumentParser(
            prog="Get FMS path",
            description="Display the read path to a FMS object, given its FMS File ID",
        )
        p.add_argument(
            "--id",
            type=str,
            help="file id of FMS object",
            default="",
            required=True,
        )
        p.parse_args(namespace=self)


###############################################################################


def configure_logging(debug: bool):
    f = Formatter(fmt="[%(asctime)s][%(levelname)s] %(message)s")
    streamHandler = StreamHandler()
    streamHandler.setFormatter(f)
    log = logging.getLogger()  # root logger
    log.handlers = [streamHandler]  # overwrite handlers
    log.setLevel(logging.DEBUG if debug else logging.INFO)


def main():
    args = Args()
    debug = args.debug
    configure_logging(debug)
    log = logging.getLogger(__name__)

    try:
        print(fms_id_to_path(args.id))

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
