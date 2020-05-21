#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from enum import Enum
import json
import logging
import traceback
from datetime import datetime
import os
from pathlib import Path
import smtplib

import dask
from aics_dask_utils import DistributedHandler
from distributed import LocalCluster

# from fov_processing_pipeline import wrappers, utils
from cellbrowser_tools import (
    createJobsFromCSV,
    dataHandoffUtils,
    dataset_constants,
    generateCellLineDef,
    jobScheduler,
    validateProcessedImages,
)
from dask_jobqueue import SLURMCluster
from prefect import Flow, task

###############################################################################

log = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)

###############################################################################


class BuildStep(Enum):
    VALIDATE = "validate"
    FEATUREDATA = "featuredata"
    CELLLINES = "celllines"
    DONE = "done"

    def __str__(self):
        return self.value


def setup_prefs(p):
    prefs = dataHandoffUtils.setup_prefs(p)
    return prefs


def cache_dataset(prefs, groups):
    with open(
        os.path.join(prefs["out_dir"], dataset_constants.DATASET_JSON_FILENAME), "w"
    ) as savefile:
        json.dump(groups, savefile)
    log.info("Saved dataset to json")


def uncache_dataset(prefs):
    groups = []
    with open(
        os.path.join(prefs["out_dir"], dataset_constants.DATASET_JSON_FILENAME), "r"
    ) as savefile:
        groups = json.load(savefile)
    return groups


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
        groups.append(group.to_dict(orient="records"))
    log.info("Converted groups to lists of dicts")

    # for debugging/testing, uncomment this to run on a limited set of groups
    groups = groups[0:10]

    # make dataset available as a file for later runs
    cache_dataset(prefs, groups)

    return groups


def process_fov_row(group, args, prefs):
    rows = group  # .to_dict(orient="records")
    log.info("STARTING FOV")
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
    log.info("COMPLETED FOV")


@task
def process_fov_rows(groups, args, prefs, distributed_executor_address):
    # Batch process the FOVs
    with DistributedHandler(distributed_executor_address) as handler:
        handler.batched_map(
            process_fov_row,
            [g for g in groups],
            [args for g in groups],
            [prefs for g in groups],
        )
    return "Done"


def submit_fov_rows(args, prefs, groups):
    # gather cluster commands and submit in batch
    jobdata_list = []
    for index, rows in enumerate(groups):
        # print(
        #     "(" + str(index) + "/" + str(total_jobs) + ") : Processing " + " : " + fovid
        # )
        jobdata = createJobsFromCSV.do_image(args, prefs, rows)
        jobdata_list.append(jobdata)

    print("SUBMITTING " + str(len(groups)) + " JOBS")
    job_ids = jobScheduler.slurp_commands(jobdata_list, prefs, name="fovs")
    return job_ids


def validate_fov_rows(groups, args, prefs):
    validateProcessedImages.validate_rows(groups, args, prefs)
    return True


def submit_validate_rows(prefs, job_ids):
    command = "build_release --step validate"
    deps = job_ids
    new_job_ids = jobScheduler.slurp_commands(
        [command], prefs, name="validate", deps=deps
    )
    return new_job_ids


def submit_build_feature_data(prefs, job_ids):
    command = "build_release --step featuredata"
    deps = job_ids
    new_job_ids = jobScheduler.slurp_commands(
        [command], prefs, name="featuredata", deps=deps
    )
    return new_job_ids


def submit_generate_celline_defs(prefs, job_ids):
    command = "build_release --step celllines"
    deps = job_ids
    new_job_ids = jobScheduler.slurp_commands(
        [command], prefs, name="celllines", deps=deps
    )
    return new_job_ids


def submit_done(prefs, job_ids):
    command = "build_release --step done"
    deps = job_ids
    new_job_ids = jobScheduler.slurp_commands([command], prefs, name="done", deps=deps)
    return new_job_ids


def build_feature_data(prefs, groups):
    validateProcessedImages.build_feature_data(prefs, groups)
    return True


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


def send_done_email():
    # send a notification that the data set is complete
    message = """
Subject: Dataset build complete

dataset build complete
"""
    with smtplib.SMTP("aicas-1.corp.alleninstitute.org") as s:
        s.sendmail(
            "cellbrowsertools@alleninstitute.org",
            "danielt@alleninstitute.org",
            message,
        )


# return address and cluster
def select_dask_executor(p, prefs):
    if p.debug:
        log.info(f"Debug flagged. Will use threads instead of Dask.")
        return None, None
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
                cores=2,
                memory="20GB",
                queue="aics_cpu_general",
                walltime="10:00:00",
                local_directory=str(log_dir),
                log_directory=str(log_dir),
            )
            log.info("Created SLURMCluster")

            # Set worker scaling settings
            cluster.scale(100)

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
        return distributed_executor_address, cluster


def build_release_sync(p, prefs):
    # gather data set
    groups = get_data_groups(prefs)

    # set up execution environment
    distributed_executor_address, cluster = select_dask_executor(p, prefs)

    # This is the main function
    with Flow("CFE_dataset_pipeline") as flow:

        #####################################
        # in a perfect world, I just do this:
        #####################################
        # process_fov_row_map = process_fov_row.map(
        #     group=groups, args=unmapped(p), prefs=unmapped(prefs)
        # )
        #####################################
        # but the world is not perfect:
        #####################################
        process_fov_rows(groups, p, prefs, distributed_executor_address)

    print("************************************************")
    print("***Submission complete.  Beginning execution.***")
    print("************************************************")
    # flow.run can return a state object to be used to get results
    flow.run()

    print("************************************************")
    print("***Flow execution complete.                  ***")
    print("************************************************")
    if cluster is not None:
        cluster.close()

    validate_fov_rows(groups, p, prefs)
    print("validate_fov_rows done")
    build_feature_data(prefs, groups)
    print("build_feature_data done")
    generate_cellline_def(prefs)
    print("generate_cellline_def done")

    send_done_email()

    log.info("Done!")


def build_release_async(p, prefs):
    # gather data set
    groups = get_data_groups(prefs)

    # use SLURM sbatch submission to schedule all the steps
    # each step will run build_release.py with a step id
    job_ids = submit_fov_rows(p, prefs, groups)
    job_ids = submit_validate_rows(prefs, job_ids)
    job_ids = submit_build_feature_data(prefs, job_ids)
    job_ids = submit_generate_celline_defs(prefs, job_ids)
    job_ids = submit_done(prefs, job_ids)
    log.info("All Jobs Submitted!")


def parse_args():
    p = argparse.ArgumentParser(prog="process", description="Process the FOV pipeline")

    p.add_argument("prefs", nargs="?", default="prefs.json", help="prefs file")

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

    # internal use
    p.add_argument("step", type=BuildStep, choices=list(BuildStep))

    p = p.parse_args()
    # see createJobsFromCSV.do_image implementation:
    p.run = True
    p.cluster = False
    return p


def main():
    """
    Dask/Prefect distributed command for running pipeline
    """

    p = parse_args()

    # read prefs
    prefs = setup_prefs(p.prefs)

    if p.distributed:
        # use Dask/Prefect distributed build
        build_release_sync(p, prefs)
    else:
        # if a step was passed in, then we need to run that step!
        if p.step == BuildStep.VALIDATE:
            groups = uncache_dataset(prefs)
            validate_fov_rows(groups, p, prefs)
            print("validate_fov_rows done")
        elif p.step == BuildStep.FEATUREDATA:
            groups = uncache_dataset(prefs)
            build_feature_data(prefs, groups)
            print("build_feature_data done")
        elif p.step == BuildStep.CELLLINES:
            generate_cellline_def(prefs)
            print("generate_cellline_def done")
        elif p.step == BuildStep.DONE:
            send_done_email()
            log.info("Done!")
        else:
            # use SBATCH submission
            build_release_async(p, prefs)

    return


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)


if __name__ == "__main__":
    main()
