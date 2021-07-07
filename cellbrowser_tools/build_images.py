import logging
import os
from pathlib import Path
import shutil

import createJobsFromCSV
import dataHandoffUtils
import jobScheduler


log = logging.getLogger(__name__)


def submit_done(prefs, prefspath, job_ids):
    command = f"build_release {prefspath} --step done"
    deps = job_ids
    new_job_ids = jobScheduler.submit_one(command, prefs, name="done", deps=deps)
    return new_job_ids


def submit_fov_rows(args, prefs, groups):
    # gather cluster commands and submit in batch
    jobdata_list = []
    log.info("PREPARING " + str(len(groups)) + " JOBS")
    for index, rows in enumerate(groups):
        jobdata = createJobsFromCSV.do_image(args, prefs, rows)
        jobdata_list.append(jobdata)

    log.info("SUBMITTING " + str(len(groups)) + " JOBS")
    job_ids = jobScheduler.submit_batch(jobdata_list, prefs, name="fovs")
    return job_ids


def get_data_groups(
    input_manifest: os.PathLike,
    query_options: dataHandoffUtils.QueryOptions,
    prefs,
    n=0,
):
    data = dataHandoffUtils.collect_csv_data_rows(
        input_manifest, fovids=query_options.fovids, cell_lines=query_options.cell_lines
    )
    log.info("Number of total cell rows: " + str(len(data)))
    # group by fov id
    data_grouped = data.groupby("FOVId")
    total_jobs = len(data_grouped)
    log.info("Number of total FOVs: " + str(total_jobs))
    # log.info('ABOUT TO CREATE ' + str(total_jobs) + ' JOBS')
    groups = []
    for index, (fovid, group) in enumerate(data_grouped):
        groups.append(group.to_dict(orient="records"))
        # only the first n FOVs (one group per FOV)
        if n > 0 and index >= n - 1:
            break

    log.info("Converted groups to lists of dicts")

    # make dataset available as a file for later runs
    dataHandoffUtils.cache_dataset(prefs, groups)

    return groups


def build_images(
    input_manifest: os.PathLike,
    output_dir: os.PathLike,
    distributed: bool,
    query_options: dataHandoffUtils.QueryOptions,
):
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
