from cellbrowser_tools.dataHandoffUtils import ActionOptions, OutputPaths
import logging
import os

from . import createJobsFromCSV
from . import dataHandoffUtils
from . import jobScheduler


log = logging.getLogger(__name__)


def submit_done(prefs, job_ids):
    command = f"build_release --step done"
    deps = job_ids
    new_job_ids = jobScheduler.submit_one(command, prefs, name="done", deps=deps)
    return new_job_ids


def submit_fov_rows(distributed: bool, prefs, groups, action_options: ActionOptions):
    # gather cluster commands and submit in batch
    jobdata_list = []
    log.info("PREPARING " + str(len(groups)) + " JOBS")
    for index, rows in enumerate(groups):
        jobdata = createJobsFromCSV.do_image(
            distributed,
            not distributed,
            prefs,
            rows,
            do_thumbnails=action_options.do_thumbnails,
            do_crop=action_options.do_crop,
            save_raw=False,
        )
        jobdata_list.append(jobdata)

    log.info("SUBMITTING " + str(len(groups)) + " JOBS")
    job_ids = jobScheduler.submit_batch(jobdata_list, prefs, name="fovs")
    return job_ids


def build_images(
    input_manifest: os.PathLike,
    output_dir: os.PathLike,
    distributed: bool,
    query_options: dataHandoffUtils.QueryOptions,
    action_options: dataHandoffUtils.ActionOptions,
):
    # setup directories
    output_paths = OutputPaths(output_dir)

    # gather data set
    groups = dataHandoffUtils.get_data_groups2(
        input_manifest, query_options, output_dir
    )

    # TODO log the command line args
    # statusdir = output_paths.status_dir
    # prefspath = Path(f"{statusdir}/prefs.json").expanduser()
    # shutil.copyfile(p.prefs, prefspath)

    # use SLURM sbatch submission to schedule all the steps
    # each step will run build_release.py with a step id
    job_ids = submit_fov_rows(
        distributed, output_paths.__dict__, groups, action_options,
    )
    job_ids = submit_done(output_paths.__dict__, job_ids)
    log.info("All Jobs Submitted!")
