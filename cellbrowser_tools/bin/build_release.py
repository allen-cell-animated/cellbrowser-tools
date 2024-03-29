#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from enum import Enum
import logging
import traceback
from datetime import datetime
import os
from pathlib import Path
import shutil
import smtplib

import dask
from aics_dask_utils import DistributedHandler
from distributed import LocalCluster

# from fov_processing_pipeline import wrappers, utils
from cellbrowser_tools import (
    createJobsFromCSV,
    dataHandoffUtils,
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
    NONE = "None"
    IMAGES = "images"
    VALIDATE = "validate"
    FEATUREDATA = "featuredata"
    CELLLINES = "celllines"
    DONE = "done"

    def __str__(self):
        return self.value


def setup_prefs(p):
    prefs = dataHandoffUtils.setup_prefs(p)
    return prefs


def process_fov_row(group, args, prefs):
    rows = group  # .to_dict(orient="records")
    name = dataHandoffUtils.get_fov_name_from_row(rows[0])
    log.info(f"STARTING FOV {name}")
    try:
        createJobsFromCSV.do_image(args.cluster, args.run, prefs, rows)
    except Exception as e:
        log.error("=============================================")
        if args.debug:
            log.error("\n\n" + traceback.format_exc())
            log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")
        # write traceback to a file
        with open(
            os.path.join(prefs["sbatch_error"], f"ERROR_{name}.txt"), "w"
        ) as myfile:
            myfile.write(str(e))
            myfile.write("\n\n")
            myfile.write(traceback.format_exc())
            myfile.write("\n\n")
        raise
    log.info("COMPLETED FOV")


@task
def process_fov_rows(groups, args, prefs, distributed_executor_address):
    # Batch process the FOVs
    batch_size = 100 if args.distributed else 4
    with DistributedHandler(distributed_executor_address) as handler:
        handler.batched_map(
            process_fov_row,
            [g for g in groups],
            [args for g in groups],
            [prefs for g in groups],
            batch_size=batch_size,
        )
    return "Done"


def submit_fov_rows(args, prefs, groups):
    # gather cluster commands and submit in batch
    jobdata_list = []
    log.info("PREPARING " + str(len(groups)) + " JOBS")
    for index, rows in enumerate(groups):
        jobdata = createJobsFromCSV.do_image(args.cluster, args.run, prefs, rows)
        jobdata_list.append(jobdata)

    log.info("SUBMITTING " + str(len(groups)) + " JOBS")
    job_ids = jobScheduler.submit_batch(jobdata_list, prefs, name="fovs")
    return job_ids


def validate_fov_rows(groups, args, prefs):
    validateProcessedImages.validate_rows(groups, args, prefs)
    return True


def submit_validate_rows(prefs, prefspath, job_ids):
    command = f"build_release {prefspath} --step validate"
    deps = job_ids
    new_job_ids = jobScheduler.submit_one(command, prefs, name="validate", deps=deps)
    return new_job_ids


def submit_build_feature_data(prefs, prefspath, job_ids):
    command = f"build_release {prefspath} --step featuredata"
    deps = job_ids
    new_job_ids = jobScheduler.submit_one(
        command, prefs, name="featuredata", deps=deps, mem="32G"
    )
    return new_job_ids


def submit_generate_celline_defs(prefs, prefspath, job_ids):
    command = f"build_release {prefspath} --step celllines"
    deps = job_ids
    new_job_ids = jobScheduler.submit_one(command, prefs, name="celllines", deps=deps)
    return new_job_ids


def submit_done(prefs, prefspath, job_ids):
    command = f"build_release {prefspath} --step done"
    deps = job_ids
    new_job_ids = jobScheduler.submit_one(command, prefs, name="done", deps=deps)
    return new_job_ids


def build_feature_data(prefs):
    validateProcessedImages.build_cfe_dataset_2020(prefs)
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
    groups = dataHandoffUtils.get_data_groups(prefs, p.n)

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

    log.info("************************************************")
    log.info("***Submission complete.  Beginning execution.***")
    log.info("************************************************")
    # flow.run can return a state object to be used to get results
    flow.run()

    log.info("************************************************")
    log.info("***Flow execution complete.                  ***")
    log.info("************************************************")
    if cluster is not None:
        cluster.close()

    validate_fov_rows(groups, p, prefs)
    log.info("validate_fov_rows done")
    build_feature_data(prefs, groups)
    log.info("build_feature_data done")
    generate_cellline_def(prefs)
    log.info("generate_cellline_def done")

    send_done_email()

    log.info("Done!")


def build_release_async(p, prefs):
    # gather data set
    groups = dataHandoffUtils.get_data_groups(prefs, p.n)

    # copy the prefs file to a location where it can be found for all steps.
    statusdir = prefs["out_status"]
    prefspath = Path(f"{statusdir}/prefs.json").expanduser()
    shutil.copyfile(p.prefs, prefspath)

    # use SLURM sbatch submission to schedule all the steps
    # each step will run build_release.py with a step id
    job_ids = submit_fov_rows(p, prefs, groups)
    job_ids = submit_validate_rows(prefs, prefspath, job_ids)
    job_ids = submit_build_feature_data(prefs, prefspath, job_ids)
    job_ids = submit_generate_celline_defs(prefs, prefspath, job_ids)
    job_ids = submit_done(prefs, prefspath, job_ids)
    log.info("All Jobs Submitted!")


def build_images_async(p, prefs):
    # gather data set
    groups = dataHandoffUtils.get_data_groups(prefs, p.n)

    # copy the prefs file to a location where it can be found for all steps.
    statusdir = prefs["out_status"]
    prefspath = Path(f"{statusdir}/prefs.json").expanduser()
    shutil.copyfile(p.prefs, prefspath)

    # use SLURM sbatch submission to schedule all the steps
    # each step will run build_release.py with a step id
    job_ids = submit_fov_rows(p, prefs, groups)
    job_ids = submit_done(prefs, prefspath, job_ids)
    log.info("All Jobs Submitted!")


def parse_args():
    p = argparse.ArgumentParser(prog="process", description="Process the FOV pipeline")

    p.add_argument("prefs", nargs="?", default="prefs.json", help="prefs file")

    p.add_argument("--n", type=int, default=0, help="Number of fov's to process.")
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
        "--sbatch",
        type=str2bool,
        default=False,
        help="Use SBATCH to submit computation graph.",
    )

    # internal use
    p.add_argument("--step", type=BuildStep, choices=list(BuildStep), default="None")

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
    elif p.sbatch:
        p.run = False
        p.cluster = True
        # use SBATCH submission
        build_release_async(p, prefs)
    else:
        # if a step was passed in, then we need to run that step!
        if p.step == BuildStep.IMAGES:
            p.run = False
            p.cluster = True
            build_images_async(p, prefs)
        elif p.step == BuildStep.VALIDATE:
            groups = dataHandoffUtils.uncache_dataset(prefs.get("out_dir"))
            validate_fov_rows(groups, p, prefs)
            log.info("validate_fov_rows done")
        elif p.step == BuildStep.FEATUREDATA:
            build_feature_data(prefs)
            log.info("build_feature_data done")
        elif p.step == BuildStep.CELLLINES:
            generate_cellline_def(prefs)
            log.info("generate_cellline_def done")
        elif p.step == BuildStep.DONE:
            send_done_email()
            log.info("Done!")
        else:
            # no cmd line args at all - just run using threads and not localcluster
            p.debug = True
            # p.n = 8  # set number of fovs to run
            # prefs["fovs"] = [135934]  # set individual fovs to run
            build_release_sync(p, prefs)

    return


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)


if __name__ == "__main__":
    main()
