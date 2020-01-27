#!/usr/bin/python

# example:
# python dsplit.py -s 20000 \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_1.1.0\Cell-Viewer_Data test2

import itertools
import os
import re
import sys
from sys import stdout, argv
from os import path
from getopt import getopt, GetoptError

# constants
BD = "\033[36m" 	# Cyan
UL = "\033[32m" 	# Green
IT = "\033[33m" 	# Yellow
ER = "\033[31m" 	# Red
RE = "\033[0m"  	# Normal

verbose = False
veryverbose = False

MB = 1024*1024.0
maxSize = 4400 * MB
cdsize = 680*MB
dvdsize = 4400*MB
dvd2size = 8080*MB
bdrsize = 23800*MB


def die(msg):
    print(ER + msg + RE)
    sys.exit(1)


def usage():
    print("""\
dsplit [options] src dst
Splits src into CD/DVD sized sub-directories. Output is in vol-%2d subdirs of
dst, and by default the files are hard-linked to the source (so they consume no
space).
OPTIONS
    -s size, --volume-size=size
    Size of each volume in MB (default 4400MB)
    --for-dvd	Set volume size for DVD (4400MB)
    --for-dvd2	Set volume size for double layer DVD (8080MB)
    --for-cd	Set volume size for CD (680MB)
    --for-bdr   Set volume size for BDR (23800MB)
   
    -v		Verbose (print all subdirectories included in volumes)
    -V		Very verbose (print all files included too)""")
    sys.exit(0)


def newarchive(outputdirtarget, inputdirtarget, prefix, index, archivesize, thisarchivefiles):
    if index != 0:
        stdout.write((BD+"Total: %.2f MB (%d files)\n"+RE) %
                     (archivesize / MB, thisarchivefiles))

    if index >= 0:
        index += 1
        archivesize = 0

        stdout.write((BD + "Archive %02d:" + RE + "%s") %
                     (index, verbose and "\n" or ""))

        armetafilename = path.join(outputdirtarget, "meta-%s-part%02d.txt" % (prefix, index))
        # armetafileobj = open(armetafilename, "w", newline='\n')
        mypath = os.path.dirname(os.path.realpath(__file__))
        # armetafileobj.write('-C%s\n' % mypath)
        # armetafileobj.write('archive_readme.txt\n')
        # armetafileobj.write('-C%s\n' % outputdirtarget)
        # armetafileobj.write('%s-part%02d.txt\n' % (prefix, index))
        # armetafileobj.write('-C%s\n' % inputdirtarget)
        # armetafileobj.close()

        arfilename = path.join(outputdirtarget, "%s-part%02d.txt" % (prefix, index))
        arfileobj = open(arfilename, "w", newline='\n')
        return arfileobj, index, archivesize


def addfile(targetarchivefile, fvalue):
    if veryverbose:
        stdout.write((IT + "%s " + RE) % fvalue)
    fl = fvalue.replace(inputdir, '')
    fl = fl.replace('\\\\', '/')
    fl = fl.replace('\\', '/')
    fl = fl.replace(inputdir, '')
    if fl.startswith('/'):
        fl = fl[1:]
    dirandfile = fl.rsplit('/', 1)
    targetarchivefile.write('-C%s\n' % dirandfile[0])
    targetarchivefile.write(dirandfile[1] + '\n')


def sorter(txt):
    r = re.search('AICS-[0-9]{1,3}_([0-9]{1,4})\.ome\.tif', txt)
    return int(r.group(1)) if r else 9999999


def gather_aics_images(inputdir):
    # search input dir and group all files by cell line from the filename
    filegroups = {}
    for root, dirs, files in os.walk(inputdir, True):
        if verbose:
            stdout.write("  %s: " % root)

        if root.find(inputdir) != 0:
            die("%s isn't a subdir of %s" % (root, inputdir))

        for f in files:
            match = re.match('(AICS-[0-9]{1,3})_[0-9]{1,4}\.ome\.tif', f)
            if match is not None and match.group(1) is not None:
                cellline = match.group(1)
                if cellline in filegroups:
                    filegroups[cellline].append(path.join(root, f))
                else:
                    filegroups[cellline] = [path.join(root, f)]
    return filegroups


# def gather_drug_images(inputcsv):
#     filegroups = {}
#     rows = dataHandoffSpreadsheetUtils.get_rows(inputcsv)
#     for key, group in itertools.groupby(rows, lambda item: item["Drug_name_short"].lower()+'_'+item["Well Type"].lower()):
#         if key in filegroups:
#             filegroups[key].extend([item["Link to input data"] for item in group])
#         else:
#             filegroups[key] = [item["Link to input data"] for item in group]
#     return filegroups


def run(filegroups, outputdir, sorter=None):
    if not os.path.exists(outputdir):
        os.mkdir(outputdir)

    # convert this to generic groups rather than cell lines and cell names
    # filegroup in filegroups
    for filegroup in filegroups:
        files = filegroups[filegroup]
        if sorter is not None:
            files.sort(key=lambda l: sorter(l))
        else:
            files.sort()

        listnameroot = filegroup
        stdout.write(("%s\n") % (filegroup))

        (arfile, idx, volsize) = newarchive(outputdir, inputdir, listnameroot, 0, 0, 0)
        thisvolfiles = 0

        for f in files:
            # File size without following symlinks.
            s = os.lstat(f).st_size

            # file alone is bigger than allowed archive size: report it and skip.
            if s > maxSize:
                stdout.write((ER + "%s (%.2f MB) " + RE) % (f, s / MB))
                continue

            elif float(s + volsize) > maxSize:
                (arfile, idx, volsize) = \
                    newarchive(outputdir, inputdir, listnameroot, idx, volsize, thisvolfiles)
                thisvolfiles = 0

                # if verbose:
                #     stdout.write("  %s: " % root)

            addfile(arfile, f)
            thisvolfiles += 1
            volsize += s
        # end for f
        newarchive(outputdir, inputdir, listnameroot, -1, volsize, thisvolfiles)

# Code starts here

popts = []
try:
    (popts, argv) = getopt(argv[1:], 's:vVh',
                           ['volume-size=', 'for-cd', 'for-dvd', 'for-dvd2'])
except GetoptError as opt:
    die(str(opt))

for (opt, value) in popts:
    if opt == '-s' or opt == '--volume-size':
        maxSize = float(value)*MB
    elif opt == '--for-cd':
        maxSize = cdsize
    elif opt == '--for-dvd':
        maxSize = dvdsize
    elif opt == '--for-dvd2':
        maxSize = dvd2size
    elif opt == '--for-bdr':
        maxSize = bdrsize
    elif opt == '-v':
        verbose = True
    elif opt == '-V':
        verbose = True
        veryverbose = True
    elif opt == '-h':
        usage()
# end for (opt,value)

inputdir = '.'
outputdir = '.'
if len(argv) < 2:
    die("USAGE: dsplit [options] src dst. (Use -h for help)")
else:
    inputdir = argv[0]
    outputdir = argv[1]


# search input dir and group all files by cell line from the filename
filegroups = gather_aics_images(inputdir)
run(filegroups, outputdir, sorter)

# filegroups = gather_drug_images(inputdir)
# run(filegroups, outputdir)
