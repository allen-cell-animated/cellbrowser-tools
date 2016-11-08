#!/usr/bin/env bash

# assumes a naming convention for the tif files


UPLOADER_PATH=~/src/aicsviztools/bisque/uploader/
# AICSIMGSRCPATH=/Volumes/aics/software_it/danielt/images/AICS/
# AICSIMGDESTPATH=/Volumes/aics/software_it/danielt/images/AICS/bisque/
# AICSTHUMBNAILPATH=/Volumes/aics/software_it/danielt/demos/bisque/thumbnails/

URLIMGPATH=file:///data/aics/software_it/danielt/images/AICS/bisque/
URLTHUMBNAILPATH=http://stg-aics/danielt_demos/bisque/thumbnails/

#unused with file list
TAGVALUE=dummy

# filelist.csv created by splitAndCrop.py
for i in alphactinin mito tub lmnb; do
    python $UPLOADER_PATH/oneUpFileList.py ${URLIMGPATH}${i}/ ${URLTHUMBNAILPATH}${i}/ $TAGVALUE --list ./images/$i/filelist.csv
    mv out.txt outAlphaActinin.txt
done
