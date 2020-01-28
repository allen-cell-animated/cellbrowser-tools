#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging

from prefect import task, Flow, unmapped

# from fov_processing_pipeline import wrappers, utils
from cellbrowser_tools import createJobsFromCSV
from cellbrowser_tools import dataHandoffUtils
from cellbrowser_tools import generateCellLineDef
from cellbrowser_tools import validateProcessedImages

###############################################################################

log = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)

###############################################################################


@task
def setup_prefs(p):
    prefs = dataHandoffUtils.setup_prefs(p)
    return prefs


@task
def get_data_groups(prefs):
    data = dataHandoffUtils.collect_data_rows(fovids=prefs.get("fovs"))
    log.info("Number of total cell rows: " + str(len(data)))
    # group by fov id
    data_grouped = data.groupby("FOVId")
    total_jobs = len(data_grouped)
    log.info("Number of total FOVs: " + str(total_jobs))
    # log.info('ABOUT TO CREATE ' + str(total_jobs) + ' JOBS')
    count = 10
    groups = []
    for index, (fovid, group) in enumerate(data_grouped):
        groups.append(group)
        count = count - 1
        if count <= 0:
            break
    return groups


@task
def process_fov_row(group, args, prefs):
    rows = group.to_dict(orient="records")
    createJobsFromCSV.do_image(args, prefs, rows)


@task
def validate_fov_rows(groups, args, prefs):
    validateProcessedImages.validate_rows(groups, args, prefs)
    return True


@task
def build_feature_data(prefs):
    validateProcessedImages.build_feature_data(prefs)
    return True


@task
def generate_cellline_def(prefs):
    generateCellLineDef.generate_cellline_def(prefs)
    return True


def str2bool(v):
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def main():
    """
    Dask/Prefect distributed command for running pipeline
    """

    p = argparse.ArgumentParser(prog="process", description="Process the FOV pipeline")

    p.add_argument("prefs", nargs="?", default="prefs.json", help="prefs file")

    p.add_argument(
        "-s",
        "--save_dir",
        action="store",
        default="./results/",
        help="Save directory for results",
    )

    p.add_argument(
        "--n_fovs", type=int, default=100, help="Number of fov's per cell line to use."
    )
    p.add_argument(
        "--debug",
        type=str2bool,
        default=False,
        help="Do debugging things (currently applies only to distributed)",
    )
    # distributed stuff
    p.add_argument(
        "--distributed",
        type=str2bool,
        default=False,
        help="Use Prefect/Dask to do distributed compute.",
    )
    p.add_argument(
        "--port",
        type=int,
        default=99999,
        help="Port over which to communicate with the Dask scheduler.",
    )

    p = p.parse_args()
    # see createJobsFromCSV.do_image implementation:
    p.run = True
    p.cluster = False

    # save_dir = str(Path(p.save_dir).resolve())
    # overwrite = p.overwrite
    # use_current_results = p.use_current_results

    # log.info("Saving in {}".format(save_dir))

    # if not os.path.exists(p.save_dir):
    #     os.makedirs(p.save_dir)

    # https://github.com/AllenCellModeling/scheduler_tools/blob/master/remote_job_scheduling.md

    if p.distributed:
        from prefect.engine.executors import DaskExecutor

        executor = DaskExecutor(
            address="tcp://localhost:{PORT}".format(**{"PORT": p.port})
        )
    else:
        from prefect.engine.executors import LocalExecutor

        executor = LocalExecutor()

    # This is the main function
    with Flow("FOV_processing_pipeline") as flow:

        prefs = setup_prefs(p.prefs)

        groups = get_data_groups(prefs)

        process_fov_row_map = process_fov_row.map(
            group=groups, args=unmapped(p), prefs=unmapped(prefs)
        )

        validate_result = validate_fov_rows(
            groups, p, prefs, upstream_tasks=[process_fov_row_map]
        )

        my_return_value = build_feature_data(prefs, upstream_tasks=[validate_result])

        generate_cellline_def(prefs, upstream_tasks=[my_return_value])

    # flow.run can return a state object to be used to get results
    flow.run(executor=executor)

    # pull some result data (return values) back into this host's process
    # df_stats = state.result[flow.get_tasks(name="load_stats")[0]].result

    log.info("Done!")

    return


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)


if __name__ == "__main__":
    main()
