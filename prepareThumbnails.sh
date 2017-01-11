#!/usr/bin/env bash

# virtualenv venv
# source venv/bin/activate
# pip install --user dist/aicsimagetools-0.0.1-py2.py3-none-any.whl

# prepare thumbnails

export AICSIMGPATH=/data/aics/software_it/danielt/images/AICS/bisque/
export AICSTHUMBNAILPATH=/data/aics/software_it/danielt/demos/bisque/thumbnails/

for i in 20160705_I01 20160705_S03 20160708_C01 20160708_I01 20160711_C01 20160929_I01 20160930_S01; do
    ls ${AICSIMGPATH}${i}/*.ome.tif > files.txt
    export IEX=${i}
    xargs -n 1 sh -c './thumbnail2.py --path "$0" --outpath "${AICSTHUMBNAILPATH}${IEX}/" --dna 0 --str 1 --mem 2 --seg 5 --size 128 || exit 255' < files.txt
    rm files.txt
done
