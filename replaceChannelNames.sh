#!/bin/bash

echo $1
# uses bioformats command line tool tiffcomment
# ./tiffcomment $1 | sed -e 's/MEMB:0/MEMB/g;s/STRUCT:1/STRUCT/g;s/DNA:2/DNA/g;s/TRANS:3/TRANS/g;s/SEG_NUC:4/SEG_DNA/g;s/SEG_CELL:5/SEG_MEMB/g;s/SEG_STRUCT:6/SEG_STRUCT/g' > tmp.xml
# replace SEG_STRUCT with CON_Memb, and THEN replace SEG_DNA with SEG_STRUCT
./bftools/tiffcomment $1 | sed -e 's/SEG_STRUCT/CON_Memb/g;s/SEG_DNA/SEG_STRUCT/g' > tmp.xml
./bftools/tiffcomment -set tmp.xml $1
