#!/usr/bin/env bash

# assumes a naming convention for the tif files

# virtualenv venv
# source venv/bin/activate
# pip install bisque-api
# pip install --user dist/aicsimagetools-0.0.1-py2.py3-none-any.whl

UPLOADER_PATH=~/src/aicsviztools/bisque/uploader/

DIR=tub
NAME=img38_3
TAGVALUE=microtubules


export AICSIMGSRCPATH=/Volumes/aics/software_it/danielt/images/AICS/$DIR/
AICSIMGDESTPATH=/Volumes/aics/software_it/danielt/images/AICS/bisque/$DIR/
URLIMGPATH=file:///data/aics/software_it/danielt/images/AICS/bisque/$DIR/
export AICSTHUMBNAILPATH=/Volumes/aics/software_it/danielt/demos/bisque/thumbnails/$DIR/
URLTHUMBNAILPATH=http://stg-aics/danielt_demos/bisque/thumbnails/$DIR/

interleave.py --path "$AICSIMGSRCPATH" --outpath "$AICSIMGDESTPATH" --prefix $NAME || exit 255
thumbnail.py --path "$AICSIMGSRCPATH" --outpath "$AICSTHUMBNAILPATH" --prefix $NAME --size 128 || exit 255
python $UPLOADER_PATH/oneUpFileList.py $URLIMGPATH $URLTHUMBNAILPATH $TAGVALUE --name $NAME
