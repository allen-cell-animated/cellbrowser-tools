Workflow:

LET VERSION = 1.2.0

0. On data handoff, verify spreadsheet(s):  
Modify a copy of prefs.json to have the proper parameters inside.
    python validateDataHandoff.py myprefs.json

1. On cluster,  
create a virtualenv and pip install aicsimage into it.  
run:  
    python createJobsFromCSV.py myprefs.json -c -n  
When cluster jobs are all done:
    python validateProcessedImages.py myprefs.json

If not already, copy or move ome.tif image files to final destination: \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_VERSION\Cell-Viewer_Data
If not already, copy or move png thumbnails to final destination: \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_VERSION\Cell-Viewer_Thumbnails

2. add images to bisque db  
    python createJobsFromCSV.py myprefs.json -c -p

3. QA / validate final data  
    python validateBisqueDb.py myprefs.json

4. Count files, count db entries, and do final qa. 

5. prepare zip archives.  This is a global operation over the entire data set.  
    python dsplit.py -s 20000 \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_VERSION\Cell-Viewer_Data \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_VERSION\archive\  

    cd \\allen\aics\animated-cell\Allen-Cell-Explorer\Allen-Cell-Explorer_VERSION\Cell-Viewer_Data
    for i in ../archive/*.txt; do tar -c -T "${i}" | gzip -1 > "${i%.txt}.tar.gz"; done

6. Upon deployment, can run  
    python uploader/stress.py  
to precache the 3D data.  Verify that the correct db url is provided to stress.py!!

