import subprocess
import os
import random
import re
from pathlib import Path
import jinja2


# put entire command lines into a text file and run them from
# srun $(head -n $SLURM_ARRAY_TASK_ID cmds.txt | tail -n 1)
# This can also be done with sed like:
# srun $(sed -n ${SLURM_ARRAY_TASK_ID}p cmds.txt)
# or:
# srun bash -c "$(head -n $SLURM_ARRAY_TASK_ID cmds.txt | tail -n 1)"
def slurp_commands(commandlist, prefs, name="", do_run=True, deps=[]):
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
        job_prefs["output"] = os.path.join(
            prefs["out_status"], prefs["script_dir"], job_prefs["output"]
        )
        job_prefs["error"] = os.path.join(
            prefs["out_status"], prefs["script_dir"], job_prefs["error"]
        )
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

    jobids = []
    if do_run or len(scripts) == 1:
        depstring = ""
        if len(deps) > 0:
            depliststring = ":".join([str(x) for x in deps])
            depstring = f"-d=afterany:{depliststring}"
        for script in scripts:
            result = subprocess.run(
                ["sbatch", depstring, script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            output = result.stdout.decode("utf-8")
            code = result.returncode
            print(f"{script.name} output:")
            print(output)
            if code != 0:
                print(f"Error occurred in {script.name} processing")
                raise subprocess.CalledProcessError(code, script.name)
            else:
                if len(output) > 0:
                    for line in output.splitlines():
                        if line.startswith(
                            "Submitted batch job"
                        ):  # The output should have a line with the SLURM job ID
                            match = re.match(".*?([0-9]+)$", line)
                            if match:
                                job_id = match.group(1)
                    if job_id:
                        jobids.append(job_id)
                    else:
                        raise RuntimeError(
                            "Error submitting job - no SLURM job ID was found in the output"
                        )
                else:
                    raise RuntimeError("Failed to get a result from sbatch submission.")
    return jobids
