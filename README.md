Workflow:

0. On data handoff, verify spreadsheet(s):  
    python validateDataHandoff.py --sheets /path/to/dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreadsheets_contourXY

1. After validation, prepare cell names database with this:  
    python assignCellNames.py --sheets /path/to/dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreadsheets_contourXY    
This will update the file data/cellnames.csv.  
Push this update file back into git.

2. On cluster,  
create a virtualenv and pip install aicsimage into it.  
run:  
    python createJobsFromCSV.py --sheets /path/to/dataset_cellnuc_seg_curated/2017_05_15_tubulin/spreadsheets_contourXY --dataset 2017_05_15_tubulin_TEST -c -n  
verify images by eye (?!?!)  (use aics internal imageviewer to inspect)  
rename directory to remove _TEST once confidence is high
copy ome.tif image files to \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_1.1.0\Cell-Viewer_Data
copy png thumbnails to \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_1.1.0\Cell-Viewer_Thumbnails

3. add images to bisque db  
    python createJobsFromCSV.py --sheets /path/to/dataset_cellnuc_seg_curated/2017_05_15_tubulin/spreadsheets_contourXY --dataset 2017_05_15_tubulin -c -p

4. QA / validate final data  
    python validateBisqueDb.py --sheets /path/to/dataset_cellnuc_seg_curated\2017_05_15_tubulin/spreadsheets_contourXY

5. prepare zip archives.  This is a global operation over the entire data set.  
    python dsplit.py -s 20000 \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_1.1.0\Cell-Viewer_Data \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_1.1.0\archive\manifest\  
    for i in $(ls *.txt); do tar -c -T ${i} | gzip -1 > ${i%.txt}.tar.gz; done

6. Upon deployment, can run  
    python uploader/stress.py  
to precache the 3D data.  Verify that the correct db url is provided to stress.py!!
