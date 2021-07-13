from cellbrowser_tools.dataHandoffUtils import OutputPaths
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


def submit_fov_rows(args, prefs, groups):
    # gather cluster commands and submit in batch
    jobdata_list = []
    log.info("PREPARING " + str(len(groups)) + " JOBS")
    for index, rows in enumerate(groups):
        jobdata = createJobsFromCSV.do_image(
            args["cluster"],
            args["run"],
            prefs,
            rows,
            do_thumbnails=False,  # TODO get this from args
            do_crop=False,  # TODO get this from args
            save_raw=False,
        )
        jobdata_list.append(jobdata)

    log.info("SUBMITTING " + str(len(groups)) + " JOBS")
    job_ids = jobScheduler.submit_batch(jobdata_list, prefs, name="fovs")
    return job_ids


def get_data_groups(
    input_manifest: os.PathLike,
    query_options: dataHandoffUtils.QueryOptions,
    out_dir: os.PathLike,
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
        if query_options.first_n > 0 and index >= query_options.first_n - 1:
            break

    log.info("Converted groups to lists of dicts")

    # make dataset available as a file for later runs
    dataHandoffUtils.cache_dataset(out_dir, groups)

    return groups


def build_images(
    input_manifest: os.PathLike,
    output_dir: os.PathLike,
    distributed: bool,
    query_options: dataHandoffUtils.QueryOptions,
):
    # setup directories
    output_paths = OutputPaths(output_dir)

    # gather data set
    groups = get_data_groups(input_manifest, query_options, output_dir)

    # TODO log the command line args
    # statusdir = output_paths.status_dir
    # prefspath = Path(f"{statusdir}/prefs.json").expanduser()
    # shutil.copyfile(p.prefs, prefspath)

    # use SLURM sbatch submission to schedule all the steps
    # each step will run build_release.py with a step id
    job_ids = submit_fov_rows(
        {"cluster": distributed, "run": not distributed},
        output_paths.__dict__,
        groups,
    )
    job_ids = submit_done(output_paths.__dict__, job_ids)
    log.info("All Jobs Submitted!")
