from bioio import BioImage
import argparse
import logging
import sys
import traceback

from datetime import datetime
from logging import FileHandler, StreamHandler, Formatter


class Args(argparse.Namespace):
    def __init__(self):
        super().__init__()
        self.fms_id = ""
        #
        self.__parse()

    def __parse(self):
        p = argparse.ArgumentParser(
            prog="show_info",
            description="Display the dimensions and channel names for all scenes in a file",
        )
        p.add_argument(
            "imagepath",
            type=str,
            default="",
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
    configure_logging(False)
    log = logging.getLogger(__name__)

    try:
        imagepath = args.imagepath
        # Load the image
        image = BioImage(imagepath)
        s = image.scenes
        print(imagepath)
        for s in image.scenes:
            image.set_scene(s)
            print(f"Scene {s}")
            print(f"  Shape: {image.shape}, dtype: {image.dtype}")
            print(f"  Channel names: {image.channel_names}")

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
