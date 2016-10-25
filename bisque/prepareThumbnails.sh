#!/usr/bin/env bash

# virtualenv venv
# source venv/bin/activate
# pip install --user dist/aicsimagetools-0.0.1-py2.py3-none-any.whl

# prepare thumbnails

export AICSIMGPATH=./images/
export AICSTHUMBNAILPATH=./images/thumbnails/

for i in alphactinin mito tub lmnb; do
    ls ${AICSIMGPATH}${i}/*.ome.tif > files.txt
    export IEX=${i}
    xargs -n 1 sh -c './thumbnail2.py --path "$0" --outpath "${AICSTHUMBNAILPATH}${IEX}/" --dna 0 --str 1 --mem 2 --size 128 || exit 255' < files.txt
    rm files.txt
done
