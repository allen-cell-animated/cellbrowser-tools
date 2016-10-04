#!/usr/bin/env bash

# assumes a naming convention for the tif files

# virtualenv venv
# source venv/bin/activate
# pip install bisque-api
# pip install --user dist/aicsimagetools-0.0.1-py2.py3-none-any.whl

# alphactinin, Mito, tub

UPLOADER_PATH=~/src/aicsviztools/bisque/uploader/

# 1. alphactinin
export AICSIMGSRCPATH=/Volumes/aics/software_it/danielt/images/AICS/alphactinin/
AICSIMGDESTPATH=/Volumes/aics/software_it/danielt/images/AICS/bisque/alphactinin/
URLIMGPATH=file:///data/aics/software_it/danielt/images/AICS/bisque/alphactinin/
export AICSTHUMBNAILPATH=/Volumes/aics/software_it/danielt/demos/bisque/thumbnails/alphactinin/
URLTHUMBNAILPATH=http://stg-aics/danielt_demos/bisque/thumbnails/alphactinin/
TAGVALUE=alpha_actinin

for i in $(ls $AICSIMGSRCPATH | grep -o 'img[0-9]\{1,3\}_[0-9]' | uniq);do
  interleave.py --path "$AICSIMGSRCPATH" --outpath "$AICSIMGDESTPATH" --prefix $i || exit 255
  thumbnail.py --path "$AICSIMGSRCPATH" --outpath "$AICSTHUMBNAILPATH" --prefix $i --size 128 || exit 255
  python $UPLOADER_PATH/oneUpFileList.py $URLIMGPATH $URLTHUMBNAILPATH $TAGVALUE --name $i
done
mv out.txt outAlphaActinin.txt

# 2. Mito
export AICSIMGSRCPATH=/Volumes/aics/software_it/danielt/images/AICS/Mito/
AICSIMGDESTPATH=/Volumes/aics/software_it/danielt/images/AICS/bisque/Mito/
URLIMGPATH=file:///data/aics/software_it/danielt/images/AICS/bisque/Mito/
export AICSTHUMBNAILPATH=/Volumes/aics/software_it/danielt/demos/bisque/thumbnails/Mito/
URLTHUMBNAILPATH=http://stg-aics/danielt_demos/bisque/thumbnails/Mito/
TAGVALUE=mitochondria

for i in $(ls $AICSIMGSRCPATH | grep -o 'img[0-9]\{1,3\}_[0-9]' | uniq);do
  interleave.py --path "$AICSIMGSRCPATH" --outpath "$AICSIMGDESTPATH" --prefix $i || exit 255
  thumbnail.py --path "$AICSIMGSRCPATH" --outpath "$AICSTHUMBNAILPATH" --prefix $i --size 128 || exit 255
  python $UPLOADER_PATH/oneUpFileList.py $URLIMGPATH $URLTHUMBNAILPATH $TAGVALUE --name $i
done
mv out.txt outMitochondria.txt

# 3. tub
export AICSIMGSRCPATH=/Volumes/aics/software_it/danielt/images/AICS/tub/
AICSIMGDESTPATH=/Volumes/aics/software_it/danielt/images/AICS/bisque/tub/
URLIMGPATH=file:///data/aics/software_it/danielt/images/AICS/bisque/tub/
export AICSTHUMBNAILPATH=/Volumes/aics/software_it/danielt/demos/bisque/thumbnails/tub/
URLTHUMBNAILPATH=http://stg-aics/danielt_demos/bisque/thumbnails/tub/
TAGVALUE=microtubules

for i in $(ls $AICSIMGSRCPATH | grep -o 'img[0-9]\{1,3\}_[0-9]' | uniq);do
  interleave.py --path "$AICSIMGSRCPATH" --outpath "$AICSIMGDESTPATH" --prefix $i || exit 255
  thumbnail.py --path "$AICSIMGSRCPATH" --outpath "$AICSTHUMBNAILPATH" --prefix $i --size 128 || exit 255
  python $UPLOADER_PATH/oneUpFileList.py $URLIMGPATH $URLTHUMBNAILPATH $TAGVALUE --name $i
done
mv out.txt outMicrotubules.txt
