import subprocess
import os
import random
import time
from pathlib import Path
import jinja2


def submit_job(files, json_obj, tmp_file_name="tmp_script.sh", files_deps=[]):

    if type(files) is not list:
        files = list([files])

    str_list = list()

    for i, file in enumerate(files):
        queuename = " -q " + json_obj["queue_name"]
        walltime = " -l walltime=" + json_obj["walltime"]
        memory = " -l mem=" + json_obj["memory"]

        if len(files_deps) >= len(files):
            # one dep in json_obj['deps'] per file in files list.
            deps = " -W depend=afterany:" + str(files_deps[i])
        elif "deps" in json_obj:
            # one dep in json_obj['deps'] per file in files list.
            deps = " -W depend=afterany" + str(json_obj["deps"][i])
        #             deps = ' -W depend=afterany' + ''.join([':' + str(dep_job) for dep_job in json_obj['deps']])
        else:
            deps = ""

        if json_obj.get("debug") is not None and json_obj["debug"].lower() == "true":
            debug = " -k eo"
        else:
            debug = ""

        if json_obj.get("ppn") is None:
            json_obj["ppn"] = "1"

        if json_obj.get("node") is None:
            json_obj["node"] = "1"

        resources = " -l nodes=" + json_obj["node"] + ":ppn=" + json_obj["ppn"]

        out_file = file + ".out"
        out_file_str = " -o " + out_file

        err_file = file + ".err"
        err_file_str = " -e " + err_file

        sub_str = (
            "qsub"
            + deps
            + " -V "
            + file
            + out_file_str
            + err_file_str
            + queuename
            + walltime
            + memory
            + resources
            + debug
            + "\n"
        )
        str_list.append(sub_str)
        # big_str += sub_str

    big_str = "".join(str_list)
    # big_str = 'export MALLOC_CHECK_=0\n' + ''.join(str_list)

    tmp_file = json_obj["script_dir"] + os.sep + tmp_file_name
    target = open(tmp_file, "w")
    target.write(big_str)
    target.close()

    proc = subprocess.Popen("bash " + tmp_file, stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()

    job_ids = [
        int(token.split(".")[0])
        for token in out.decode("utf8").split("\n")
        if token != ""
    ]

    return job_ids


def touch(fname):
    if os.path.exists(fname):
        os.utime(fname, None)
    else:
        open(fname, "w").close()


def submit_job_deps(files, json_obj, tmp_file_name="tmp_script.sh"):
    # this is a workaround function to get over the fact that we can have only a limited number of dependencies for a
    # job so we submit all jobs, and have jobs that wait for a handfull of those jobs to finish, and so on and so forth

    donothing_file = json_obj["script_dir"] + os.sep + "donothing.sh"
    touch(donothing_file)

    job_ids = submit_job(files, json_obj, tmp_file_name)

    c = 0
    max_deps = 40
    while len(job_ids) > max_deps:
        tmplist = list()
        while len(tmplist) < max_deps:
            tmplist.append(job_ids.pop())

        json_obj["deps"] = tmplist
        dep_id = submit_job(
            donothing_file, json_obj, tmp_file_name + "_dep_" + str(c) + ".sh"
        )
        job_ids += dep_id

        c += 1
        time.sleep(0.01)

    return job_ids


def batch(iterable, n=1):
    count = len(iterable)

    for ndx in range(0, count, n):
        last = min(ndx + n, count)
        yield iterable[ndx:last]


def submit_jobs_batches(files, json_obj, batch_size=128, tmp_file_name="tmp_script"):
    i = 0
    last_deps = []
    for x in batch(files, batch_size):
        last_deps = submit_job(
            x, json_obj, tmp_file_name + "_" + str(i) + ".sh", last_deps
        )
        i = i + 1


# put entire command lines into a text file and run them from
# srun $(head -n $SLURM_ARRAY_TASK_ID cmds.txt | tail -n 1)
# This can also be done with sed like:
# srun $(sed -n ${SLURM_ARRAY_TASK_ID}p cmds.txt)
# or:
# srun bash -c "$(head -n $SLURM_ARRAY_TASK_ID cmds.txt | tail -n 1)"
def slurp_commands(commandlist, prefs, name="", do_run=True):
    # adding this unique id lets me submit over and over and know that i'm not overwriting a key data file
    unique_id = "%08x" % random.randrange(16 ** 8)

    # Chunk up the list of commands.
    # This is to guarantee that we don't submit sbatch arrays greater than our slurm cluster's
    # limit (currently 10k a the time of writing this comment).
    n = 4096
    # TODO: consider using json_lists = more_itertools.chunked(json_list, n)
    command_lists = [commandlist[i : i + n] for i in range(0, len(commandlist), n)]
    scripts = []
    for i, commands in enumerate(command_lists):

        job_prefs = prefs["job_prefs"].copy()
        max_simultaneous_jobs = job_prefs.pop("max_simultaneous_jobs")

        slurm_args = []
        for keyword, value in job_prefs.items():
            slurm_args.append(f"--{keyword} {value}")

        batchrunnerscriptname = f"BatchRunner{i}{name}_{unique_id}.sh"
        batchdatafilename = f"BatchData{i}{name}_{unique_id}.txt"
        script = Path(prefs["out_status"]) / batchrunnerscriptname
        batchfile = Path(prefs["out_status"]) / batchdatafilename

        config = {
            "mybatchdatafilename": batchfile,
            "mybatchsize": len(commands),
            "directives": slurm_args,
            "max_simultaneous_jobs": max_simultaneous_jobs,
            "cwd": os.getcwd(),
        }

        template_path = str(Path(__file__).parent)
        j2env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path))

        with open(script, "w") as f:
            script_text = j2env.get_template("batch_job.j2").render(config)
            f.write(script_text)
        scripts.append(script)

        with open(batchfile, "w") as bf:
            for cmd in commands:
                bf.write(cmd + "\n")

    if do_run or len(scripts) == 1:
        for script in scripts:
            proc = subprocess.Popen(
                ["sbatch", script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            print(f"{script.name} output:")
            output = []
            for line in iter(proc.stdout.readline, b""):
                line = line.decode("utf-8").rstrip()
                output.append(line)
                print(line)
            proc.wait()
            code = proc.returncode
            if code != 0:
                print(f"Error occurred in {script.name} processing")
                raise subprocess.CalledProcessError(code, script.name)
