#!/usr/bin/python

# example:
# python dsplit.py -s 20000 \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_1.1.0\Cell-Viewer_Data test2

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


# Functions {{{1
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
        arfilename = path.join(outputdirtarget, "%s-part%02d.txt" % (prefix, index))
        arfileobj = open(arfilename, "w", newline='\n')
        arfileobj.write('-C%s\n' % outputdirtarget)
        arfileobj.write('%s-part%02d.txt\n' % (prefix, index))
        arfileobj.write('-C%s\n' % inputdirtarget)
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
    targetarchivefile.write(fl + '\n')


def sorter(txt):
    r = re.search('AICS-[0-9]{1,3}_([0-9]{1,4})\.ome\.tif', txt)
    return int(r.group(1)) if r else 9999999

# }}}

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
cellnamedict = {}
for root, dirs, files in os.walk(inputdir, True):
    if verbose:
        stdout.write("  %s: " % root)

    thisdirfiles = 0
    thisdirsize = 0

    if root.find(inputdir) != 0:
        die("%s isn't a subdir of %s" % (root, inputdir))

    for f in files:

        match = re.match('(AICS-[0-9]{1,3})_[0-9]{1,4}\.ome\.tif', f)
        if match is not None and match.group(1) is not None:
            cellline = match.group(1)
            if cellline in cellnamedict:
                cellnamedict[cellline].append(path.join(root, f))
            else:
                cellnamedict[cellline] = [path.join(root, f)]


if not os.path.exists(outputdir):
    os.mkdir(outputdir)

for cellline in cellnamedict:
    files = cellnamedict[cellline]
    files.sort(key=lambda l: sorter(l))

    listnameroot = cellline
    stdout.write(("%s\n") % (cellline))

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
            if verbose:
                stdout.write(("%d files ("+UL+"%.2f MB"+RE+").\n") %
                             (thisdirfiles, thisdirsize/MB))

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
    if verbose:
        stdout.write(("%d files ("+UL+"%.2f MB"+RE+").\n") %
                     (thisdirfiles, thisdirsize/MB))

# end for (...) in os.walk
