#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from datetime import datetime
import logging
from pathlib import Path
import traceback

from dask_jobqueue import SLURMCluster
from distributed import LocalCluster
from prefect import task, Flow, unmapped
from prefect.engine.executors import DaskExecutor, LocalExecutor

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
    groups = []
    for index, (fovid, group) in enumerate(data_grouped):
        groups.append(group)
    return groups


@task
def process_fov_row(group, args, prefs):
    rows = group.to_dict(orient="records")
    try:
        createJobsFromCSV.do_image(args, prefs, rows)
    except Exception as e:
        log.error("=============================================")
        if args.debug:
            log.error("\n\n" + traceback.format_exc())
            log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")
        raise


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


def select_dask_executor(p, prefs):
    if p.debug:
        executor = LocalExecutor()
        log.info(f"Debug flagged. Will use threads instead of Dask.")
        return executor
    else:
        if p.distributed:
            # Create or get log dir
            # Do not include ms
            log_dir_name = datetime.now().isoformat().split(".")[0]
            statusdir = prefs["out_status"]
            log_dir = Path(f"{statusdir}/{log_dir_name}").expanduser()
            # Log dir settings
            log_dir.mkdir(parents=True, exist_ok=True)

            # Configure dask config
            dask.config.set(
                {
                    "scheduler.work-stealing": False,
                    "logging.distributed.worker": "info",
                }
            )

            # Create cluster
            log.info("Creating SLURMCluster")
            cluster = SLURMCluster(
                cores=4,
                memory="20GB",
                queue="aics_cpu_general",
                walltime="10:00:00",
                local_directory=str(log_dir),
                log_directory=str(log_dir),
            )
            log.info("Created SLURMCluster")

            # Set worker scaling settings
            cluster.scale(60)

            # Use the port from the created connector to set executor address
            distributed_executor_address = cluster.scheduler_address

            # Log dashboard URI
            log.info(f"Dask dashboard available at: {cluster.dashboard_link}")
        else:
            # Create local cluster
            log.info("Creating LocalCluster")
            cluster = LocalCluster()
            log.info("Created LocalCluster")

            # Set distributed_executor_address
            distributed_executor_address = cluster.scheduler_address

            # Log dashboard URI
            log.info(f"Dask dashboard available at: {cluster.dashboard_link}")

        # Use dask cluster
        executor = DaskExecutor(distributed_executor_address)
        return executor


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

    p = p.parse_args()
    # see createJobsFromCSV.do_image implementation:
    p.run = True
    p.cluster = False

    # read prefs
    prefs = setup_prefs(p.prefs)

    # set up execution environment
    executor = select_dask_executor(p, prefs)

    # This is the main function
    with Flow("CFE_dataset_pipeline") as flow:

        groups = get_data_groups(prefs)

        # process_fov_row_map = process_fov_row.map(
        #     group=groups, args=unmapped(p), prefs=unmapped(prefs)
        # )
        batch_size = 20
        process_fov_row_map = []
        for i in range(0, len(groups), batch_size):
            batch = groups[i : i + batch_size]
            futures = process_fov_row.map(group=batch, args=unmapped(p), prefs=unmapped(prefs)
            process_fov_row_map += futures

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
