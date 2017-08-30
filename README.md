Workflow:

0. On data handoff, verify spreadsheet(s):  
python validateDataHandoff.py --sheets /path/to/dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY

1. After validation, prepare cell names database with this:  
python assignCellNames.py --sheets /path/to/dataset_cellnuc_seg_curated\2017_05_15_tubulin\spreasheets_contourXY  
This will update the file data/cellnames.csv.  
Push this update file back into git.

2. On cluster,  
create a virtualenv and pip install aicsimage into it.  
run:  
python createJobsFromCSV.py --sheets /path/to/dataset_cellnuc_seg_curated/2017_05_15_tubulin/spreasheets_contourXY --dataset 2017_05_15_tubulin_TEST -c -n  
verify images by eye (?!?!)  (use aics internal imageviewer to inspect)  
rename directory to remove _TEST once confidence is high

3. add images to bisque db  
python createJobsFromCSV.py --sheets /path/to/dataset_cellnuc_seg_curated/2017_05_15_tubulin/spreasheets_contourXY --dataset 2017_05_15_tubulin -c -p

4. prepare zip archives.  This is a global operation over the entire data set.  
python dsplit.py -s 20000 \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_1.1.0\Cell-Viewer_Data \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_1.1.0\archive\manifest\

5. Upon deployment, can run  
python uploader/stress.py  
to precache the 3D data.  Verify that the correct db url is provided to stress.py!!
