#!/usr/bin/env bash

# assumes a naming convention for the tif files

# virtualenv venv
# source venv/bin/activate
# pip install bisque-api
# pip install --user dist/aicsimagetools-0.0.1-py2.py3-none-any.whl

# alphactinin, Mito, tub

UPLOADER_PATH=~/src/aicsviztools/bisque/uploader/
# export so that xargs sh can see it.
# export AICSIMGPATH=/Volumes/aics/software_it/danielt/images/test/
# URLIMGPATH=file:///data/aics/software_it/danielt/images/test/
# export AICSTHUMBNAILPATH=/Volumes/aics/software_it/danielt/demos/bisque/thumbnails/alphactinin/
# URLTHUMBNAILPATH=http://stg-aics/danielt_demos/bisque/thumbnails/alphactinin/

export AICSIMGPATH=/Volumes/aics/software_it/danielt/images/AICS/alphactinin/
URLIMGPATH=file:///data/aics/software_it/danielt/images/AICS/bisque/alphactinin/
export AICSTHUMBNAILPATH=/Volumes/aics/software_it/danielt/demos/bisque/thumbnails/alphactinin/
URLTHUMBNAILPATH=http://stg-aics/danielt_demos/bisque/thumbnails/alphactinin/

ls $AICSIMGPATH | grep -o 'img[0-9]\{1,3\}_[0-9]' | uniq > files.txt
xargs -n 1 sh -c 'interleave.py --path "$AICSIMGPATH" --outpath "$AICSIMGPATH" --prefix $0 || exit 255' < files.txt
xargs -n 1 sh -c 'thumbnail.py --path "$AICSIMGPATH" --outpath "$AICSTHUMBNAILPATH" --prefix $0 --size 128 || exit 255' < files.txt
python $UPLOADER_PATH/oneUpFileList.py ./files.txt $URLIMGPATH $URLTHUMBNAILPATH alpha_actinin
rm files.txt
mv out.txt outAlphaActinin.txt
