#!/usr/bin/env python

# author: Greg Johnson, gregj@alleninstitute.org
# Ported to python and based jobScheduler.m 1.1 by gregjohnso@gmail.com
# 
# Aug 5, 2016

# this should really be turned into an object, to avoid all the mess of variables being passed around

# status: UNTESTED

import os
import sys
import json
import glob
import time
import logging
import shutil
import subprocess

import pdb

def main(json_obj):
    startup('version 2.0')

    pausetime = 5

    work_dir = json_obj["work_dir"]

    work_dir, done_dir, err_dir, exe_dir, log_dir = check_dirs(json_obj)

    logger = get_logger(log_dir)

    # while FOREVER
    c = 1
    while c > 0:
        c = c+1

        json_obj = json.load(open(json_obj_path, 'r'))
        json_obj["work_dir"] = work_dir

        work_dir, done_dir, err_dir, exe_dir, log_dir = check_dirs(json_obj)

        did_submit = submit_jobs(json_obj, logger)
        did_cleanup = cleanup(work_dir, done_dir, err_dir, exe_dir, logger)

        if did_submit or did_cleanup:
            logger.info('Waiting for next task')

        time.sleep(pausetime)
                
def get_logger(log_dir):
    if not os.path.exists(log_dir): os.makedirs(log_dir)

    logger = logging.getLogger('jobScheduler')
    logger.setLevel(logging.DEBUG)
    # makes sure that logger drops other file handlers and doesn't print out repeated messages
    for handle in logger.handlers:
        handle.close()
    logger.handlers = []

    fh = logging.FileHandler(log_dir + os.sep + time.strftime("%Y_%m_%d-%H_%M_%S") + '.log')
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

def check_dirs(json_obj):
    work_dir = json_obj["work_dir"]

    done_dir = work_dir + os.sep + "done" + os.sep
    err_dir = work_dir + os.sep + "error" + os.sep
    exe_dir = work_dir + os.sep + "executing" + os.sep
    log_dir = work_dir + os.sep + "log" + os.sep

    if not os.path.isdir(work_dir):
        os.mkdir(work_dir)

    if not os.path.isdir(done_dir):
        os.mkdir(done_dir)

    if not os.path.isdir(err_dir):
        os.mkdir(err_dir)

    if not os.path.isdir(exe_dir):
        os.mkdir(exe_dir)

    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)

    return work_dir, done_dir, err_dir, exe_dir, log_dir

def get_jobs_running(json_obj):
    exe_str = 'qstat -u $(whoami) -q ' + json_obj["queue_name"] + ' | grep $(whoami) -c'

    proc = subprocess.Popen(exe_str, stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    n_jobs_running = int(out)

    return n_jobs_running

def get_jobs_to_submit(json_obj):
    files = glob.glob(json_obj["work_dir"] + os.sep + '*.sh')

    return files

def submit_jobs(json_obj, logger):
    did_submit = False

    n_jobs_running = get_jobs_running(json_obj)
    n_jobs_to_submit = int(json_obj["njobs"]) - n_jobs_running

    files = get_jobs_to_submit(json_obj)

    if len(files) < n_jobs_to_submit:
        # Set the number of jobs to be submitted to be the same as the
        # number of jobs that exist left to submit
        n_jobs_to_submit = len(files)

    
    if n_jobs_to_submit > 0:
        logger.info('Submitting ' + str(n_jobs_to_submit) + ' jobs')

        for i in range(0, n_jobs_to_submit):
            file = files[i]
            logger.info('Submitting job ' + file)

            submit_job(file, json_obj, logger)

        did_submit = True

    return did_submit

def submit_job(file, json_obj, logger):
    exe_dir = json_obj['script_dir'] + os.sep + 'exe'
    if not os.path.isdir(exe_dir): os.mkdir(exe_dir)

    queuename = " -q " + json_obj["queue_name"]
    walltime = " -l walltime=" + json_obj["walltime"]
    memory = " -l mem=" + json_obj["memory"]

    if json_obj.get('debug') is not None and json_obj['debug'].lower() == 'true':
        debug = ' -k eo'
    else:
        debug = ''

    if json_obj.get('ppn') is None:
        json_obj['ppn'] = '1'

    if json_obj.get('node') is None:
        json_obj['node'] = '1'

    resources = " -l nodes=" + json_obj['node'] + ":ppn=" + json_obj['ppn']

    out_file = file + '.out'
    out_file_str = ' -o ' + out_file

    err_file = file + '.err'
    err_file_str = ' -e ' + err_file

    # sub_str = 'ssh ' + json_obj["host name"] + ' \'qsub -V -l nodes=1:ppn=1 ' + file + out_file_str + err_file_str + queuename + walltime + memory + '\''
    sub_str = 'qsub -V ' + file + out_file_str + err_file_str + queuename + walltime + memory + resources + debug
    logger.info('Submitting with string:' + sub_str)

    proc = subprocess.Popen(sub_str, stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    logger.info(out)

    job_id = out[0:-1]

    time.sleep(0.25)

    logger.info('Moving ' + file + ' to ' + exe_dir)
    try:
        shutil.move(file, exe_dir)
    except:
        logger.error('Failed to move ' + file)

    return out_file, err_file, job_id

def cleanup(work_dir, done_dir, err_dir, exe_dir, logger):

    spent_scripts = glob.glob(work_dir + os.sep + '*.sh.e*')

    did_cleanup = False
    for spent_script in spent_scripts:
        e_file = spent_script
        ind = e_file.find('.sh.err')

        o_file = e_file.replace('.sh.err', '.sh.out')

        s_file = exe_dir + os.path.basename(e_file[0:-4]);

        if os.path.getsize(e_file) > 0:
            # move the file to the error directory
            target_dir = err_dir
        else:
            # move the file to the output dir
            target_dir = done_dir

        logger.info('Moving ' + e_file + ' to ' + target_dir)
        shutil.move(e_file, target_dir)
        logger.info('Moving ' + o_file + ' to ' + target_dir)
        shutil.move(o_file, target_dir)
        logger.info('Moving ' + s_file + ' to ' + target_dir)
        shutil.move(s_file, target_dir)

        did_cleanup = True

    return did_cleanup

def startup(version):
    lol = (' _____       _           _  _         _____       _____  _____  _____  _____ \n' 
           '|   __| _ _ | |_  _____ |_|| |_  ___ |  _  | ___ |_   _|| __  ||     ||   | |\n'
           '|__   || | || . ||     || ||  _||___||     ||___|  | |  |    -||  |  || | | |\n'
           '|_____||___||___||_|_|_||_||_|       |__|__|       |_|  |__|__||_____||_|___|\n'
        )

    print(lol);
    print(version)
    print('Ok lets go!')

if __name__ == "__main__":
    if len(sys.argv) < 2:
        work_dir = '.'
    else:
        work_dir = sys.argv[1]
    if len(sys.argv) < 3:
        pref_file = "preferences.json"
    else:
        pref_file = sys.argv[2]

    json_obj = json.load(open(pref_file, 'r'))
    json_obj["work_dir"] = work_dir

    main(work_dir, json_obj)

