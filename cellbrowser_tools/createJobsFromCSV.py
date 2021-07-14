import argparse
import json
import os
import sys

from . import cellJob
from . import dataHandoffUtils as lkutils
from . import jobScheduler
from .dataset_constants import DataField, SLURM_SCRIPTS_DIR
from .fov_processing import do_main_image_with_celljob

# cbrImageLocation path to cellbrowser images
# cbrThumbnailLocation path to cellbrowser thumbnails
# cbrThumbnailSize size of thumbnail image in pixels (max side of edge)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process data set defined in csv files, "
        "and set up a job script for each row."
        "Example: python createJobsFromCSV.py -c -n"
    )

    # to generate images on cluster:
    # python createJobsFromCSV.py -c
    # to generate images serially:
    # python createJobsFromCSV.py -r

    # python createJobsFromCSV.py -c myprefs.json

    parser.add_argument("prefs", nargs="?", default="prefs.json", help="prefs file")

    parser.add_argument("--first", type=int, help="how many to process", default=-1)

    runner = parser.add_mutually_exclusive_group()
    runner.add_argument(
        "--run", "-r", help="run the jobs locally", action="store_true", default=False
    )
    runner.add_argument(
        "--cluster",
        "-c",
        help="run jobs using the cluster",
        action="store_true",
        default=False,
    )

    args = parser.parse_args()

    return args


def make_json(jobname, info, prefs):
    cell_job_postfix = jobname
    cellline = info.cells[0]["CellLine"]
    current_dir = os.path.join(
        prefs["status_dir"], SLURM_SCRIPTS_DIR
    )  # os.path.join(os.getcwd(), outdir)
    dest_dir = os.path.join(current_dir, cellline)

    os.makedirs(dest_dir, exist_ok=True)

    jsonname = os.path.join(dest_dir, f"FOV_{cell_job_postfix}.json")
    with open(jsonname, "w") as fp:
        json.dump(info.__dict__, fp)

    return f"processImageWithSegmentation {jsonname}"


def do_image(
    make_job: bool,
    run_now: bool,
    prefs,
    rows,
    do_thumbnails=True,
    do_crop=True,
    save_raw=True,
):
    # use row 0 as the "full field" row
    row = rows[0]

    jobname = lkutils.get_fov_name_from_row(row)

    aicscelllineid = str(row[DataField.CellLine])
    celllinename = aicscelllineid
    subdir = celllinename

    info = cellJob.CellJob(rows)

    info.do_thumbnails = do_thumbnails
    info.do_crop = do_crop
    info.save_raw = save_raw

    # drop images here
    info.cbrDataRoot = prefs["images_dir"]
    # drop thumbnails here
    info.cbrThumbnailRoot = prefs["thumbs_dir"]
    # drop texture atlases here
    info.cbrTextureAtlasRoot = prefs["atlas_dir"]

    info.cbrImageRelPath = subdir
    info.cbrImageLocation = os.path.join(info.cbrDataRoot, info.cbrImageRelPath)
    info.cbrThumbnailLocation = os.path.join(
        info.cbrThumbnailRoot, info.cbrImageRelPath
    )
    info.cbrTextureAtlasLocation = os.path.join(
        info.cbrTextureAtlasRoot, info.cbrImageRelPath
    )

    info.cbrThumbnailSize = 128

    os.makedirs(info.cbrImageLocation, exist_ok=True)
    os.makedirs(info.cbrThumbnailLocation, exist_ok=True)

    if run_now:
        do_main_image_with_celljob(info)
    elif make_job:
        # TODO: set arg to copy each indiv file to another output
        return make_json(jobname, info, prefs)


def process_images(args, prefs):

    # Read every cell image to be processed
    data = lkutils.collect_data_rows(fovids=prefs.get("fovs"))

    print("Number of total cell rows: " + str(len(data)))
    # group by fov id
    data_grouped = data.groupby("FOVId")
    total_jobs = len(data_grouped)
    print("Number of total FOVs: " + str(total_jobs))
    print("ABOUT TO CREATE " + str(total_jobs) + " JOBS")

    #
    # arrange into list of lists of dicts?

    # one_of_each = data_grouped.first().reset_index()
    # data = data.to_dict(orient='records')

    # process each file
    if args.cluster:
        # gather cluster commands and submit in batch
        jobdata_list = []
        for index, (fovid, group) in enumerate(data_grouped):
            rows = group.to_dict(orient="records")
            print(
                "("
                + str(index)
                + "/"
                + str(total_jobs)
                + ") : Processing "
                + " : "
                + fovid
            )

            jobdata = do_image(args.cluster, args.run, prefs, rows)
            jobdata_list.append(jobdata)

        print("SUBMITTING " + str(total_jobs) + " JOBS")
        jobScheduler.submit_batch(jobdata_list, prefs, name="fovs")
    else:
        # run serially
        for index, (fovid, group) in enumerate(data_grouped):
            rows = group.to_dict(orient="records")
            print(
                "("
                + str(index)
                + "/"
                + str(total_jobs)
                + ") : Processing "
                + " : "
                + fovid
            )
            do_image(args.cluster, args.run, prefs, rows)


def is_process_images_done(args):
    if args.cluster:
        return jobScheduler.any_jobs_queued()
    else:
        return True


def main():
    args = parse_args()

    prefs = lkutils.setup_prefs(args.prefs)

    process_images(args, prefs)


if __name__ == "__main__":
    print(sys.argv)
    main()
    sys.exit(0)
