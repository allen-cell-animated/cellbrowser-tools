#!/usr/bin/env bash

# assumes a naming convention for the tif files


UPLOADER_PATH=~/src/aicsviztools/bisque/uploader/
# AICSIMGSRCPATH=/Volumes/aics/software_it/danielt/images/AICS/
# AICSIMGDESTPATH=/Volumes/aics/software_it/danielt/images/AICS/bisque/
# AICSTHUMBNAILPATH=/Volumes/aics/software_it/danielt/demos/bisque/thumbnails/

IMGPATH=/data/aics/software_it/danielt/images/AICS/bisque/
URLIMGPATH=file:///data/aics/software_it/danielt/images/AICS/bisque/
URLTHUMBNAILPATH=http://stg-aics/danielt_demos/bisque/thumbnails/

#unused with file list
TAGVALUE=dummy

# filelist.csv created by splitAndCrop.py
for i in 20160705_I01 20160705_S03 20160708_C01 20160708_I01 20160711_C01 20160929_I01 20160930_S01; do
    python $UPLOADER_PATH/oneUpFileList.py ${URLIMGPATH}${i}/ ${URLTHUMBNAILPATH}${i}/ $TAGVALUE --list ${IMGPATH}${i}/filelist.csv
    mv out.txt outAlphaActinin.txt
done
