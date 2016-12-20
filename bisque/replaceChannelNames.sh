#!/bin/bash

echo $1
# uses bioformats command line tool tiffcomment
./tiffcomment $1 | sed -e 's/MEMB:0/MEMB/g;s/STRUCT:1/STRUCT/g;s/DNA:2/DNA/g;s/TRANS:3/TRANS/g;s/SEG_NUC:4/SEG_DNA/g;s/SEG_CELL:5/SEG_MEMB/g;s/SEG
_STRUCT:6/SEG_STRUCT/g' > tmp.xml
./tiffcomment -set tmp.xml $1
