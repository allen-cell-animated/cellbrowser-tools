import subprocess
import os
import time

def submit_job(files, json_obj, tmp_file_name='tmp_script.sh'):

    if type(files) is not list:
        files = list([files])

    str_list = list()

    for file in files:
        queuename = " -q " + json_obj["queue_name"]
        walltime = " -l walltime=" + json_obj["walltime"]
        memory = " -l mem=" + json_obj["memory"]

        if 'deps' in json_obj:
            deps = ' -W depend=afterany' + ''.join([':' + str(dep_job) for dep_job in json_obj['deps']])
        else:
            deps = ''

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

        sub_str = 'qsub' + deps + ' -V ' + file + out_file_str + err_file_str + queuename + walltime + memory + resources + debug + '\n'
        str_list.append(sub_str)
        # big_str += sub_str

    big_str = ''.join(str_list)
    # big_str = 'export MALLOC_CHECK_=0\n' + ''.join(str_list)


    tmp_file = json_obj['script_dir'] + os.sep + tmp_file_name
    target = open(tmp_file, 'w')
    target.write(big_str)
    target.close()

    proc = subprocess.Popen('bash ' + tmp_file, stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()

    job_ids = [int(token.split('.')[0]) for token in out.decode('utf8').split('\n') if token != '']

    return job_ids

def touch(fname):
    if os.path.exists(fname):
        os.utime(fname, None)
    else:
        open(fname, 'w').close()

def submit_job_deps(files, json_obj, tmp_file_name='tmp_script.sh'):
    #this is a workaround function to get over the fact that we can have only a limited number of dependencies for a job
    #so we submit all jobs, and have jobs that wait for a handfull of those jobs to finish, and so on and so forth

    donothing_file = json_obj['script_dir'] + os.sep + 'donothing.sh'
    touch(donothing_file)

    job_ids = submit_job(files, json_obj, tmp_file_name)

    c = 0
    max_deps = 40
    while len(job_ids) > max_deps:
        tmplist = list()
        while len(tmplist) < max_deps:
            tmplist.append(job_ids.pop())

        json_obj['deps'] = tmplist
        dep_id = submit_job(donothing_file, json_obj, tmp_file_name + '_dep_' + str(c) + '.sh')
        job_ids += dep_id

        c += 1
        time.sleep(0.01)

    return job_ids

