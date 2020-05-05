#!/usr/bin/env python

from . import dataset_constants
from . import dataHandoffUtils
import json
import sys
import logging
import os
import argparse
import traceback
import pprint

# LabKey API
from lkaccess import LabKey
import lkaccess.contexts

###############################################################################
# Global Objects

log = logging.getLogger()
logging.basicConfig(
    level=logging.DEBUG, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)

# Set the default log level for other modules used by this script
logging.getLogger("labkey").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

PP = pprint.PrettyPrinter(indent=2, width=120)


###############################################################################


class Args(object):
    """
    Use this to define command line arguments and use them later.

    For each argument do the following
    1. Create a member in __init__ before the self.__parse call.
    2. Provide a default value here.
    3. Then in p.add_argument, set the dest parameter to that variable name.

    See the debug parameter as an example.
    """

    def __init__(self, log_cmdline=True):
        self.debug = False
        self.cell_line = None
        self.prefs = ""
        #
        self.__parse()
        #
        if self.debug:
            log.setLevel(logging.DEBUG)
            log.debug("-" * 80)
            self.show_info()
            log.debug("-" * 80)
        else:
            if log_cmdline:
                log.debug("-" * 80)
                log.info(" ".join(sys.argv))
                log.debug("-" * 80)

    def __parse(self):
        p = argparse.ArgumentParser()
        # Add arguments
        p.add_argument("prefs", nargs="?", default="prefs.json", help="prefs file")
        p.add_argument("-c", "--cell_line", action="store", dest="cell_line")
        p.add_argument(
            "-d",
            "--debug",
            action="store_true",
            dest="debug",
            help="If set debug log output is enabled",
        )
        #
        p.parse_args(namespace=self)

    def show_info(self):
        log.debug("Working Dir:")
        log.debug("\t{}".format(os.getcwd()))
        log.debug("Command Line:")
        log.debug("\t{}".format(" ".join(sys.argv)))
        log.debug("Args:")
        for (k, v) in self.__dict__.items():
            log.debug("\t{}: {}".format(k, v))


###############################################################################


def generate_cellline_def(prefs):
    print("init labkey connection")
    lk = LabKey(server_context=lkaccess.contexts.PROD)
    print("issue celllines query")
    results = lk.select_rows_as_list(
        schema_name="celllines",
        query_name="CellLineDefinition",
        columns="CellLineId/Name, ProteinId/Name, StructureId/Name, ProteinId/DisplayName",
        filter_array=[
            lk.QueryFilter(
                "CellLineId/Name", "AICS-", lk.QueryFilter.Types.STARTS_WITH
            ),
            lk.QueryFilter("ProteinId/Name", [""], lk.QueryFilter.Types.NOT_IN),
        ],
    )
    rows = results["rows"]
    log.debug(PP.pformat(rows))
    log.debug("Row Count {}: ".format(len(rows)))

    print("saving cellline results")
    with open(
        os.path.join(prefs.get("out_dir"), dataset_constants.CELL_LINE_DATA_FILENAME),
        "w",
    ) as outfile:
        json.dump(rows, outfile, indent=4)


###############################################################################

if __name__ == "__main__":
    dbg = False
    try:
        args = Args()
        dbg = args.debug
        prefs = dataHandoffUtils.setup_prefs(args.prefs)
        generate_cellline_def(prefs)

    except Exception as e:
        log.error("=============================================")
        if dbg:
            log.error("\n\n" + traceback.format_exc())
            log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")
        sys.exit(1)
