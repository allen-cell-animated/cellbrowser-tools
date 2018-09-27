Workflow:

LET VERSION = 1.3.0

1. On data handoff, modify a copy of prefs.json to have the proper parameters inside.

2. Generate image data: 
On cluster, create a virtualenv and pip install aicsimageio and aicsimageprocessing and labkey and pandas into it.  
run:
    ```
    python createJobsFromCSV.py myprefs.json -c -n  
    ```
When cluster jobs are all done:
    ```
    python validateProcessedImages.py myprefs.json
    ```
*This validateProcessedImages step generates an output csv file that lists all files.  This file must be handed off to Gabe for downloader use.*

3. add images to bisque db:

Password fixup for docker installs of bisque:
    `docker exec -u postgres bisque_bq_pg_1 psql bisque -c "UPDATE tg_user SET password='admin' WHERE user_name='admin'"`

 Then:
    ```
    python createJobsFromCSV.py myprefs.json -c -p
    ```

4. QA / validate final data  
    ```
    python validateBisqueDb.py myprefs.json
    ```

5. Count files, count db entries, and do final qa. 

6. prepare zip archives.  This is a global operation over the entire data set.  
    ```
    python dsplit.py -s 20000 /allen/aics/animated-cell/Allen-Cell-Explorer/Allen-Cell-Explorer_VERSION/Cell-Viewer_Data /allen/aics/animated-cell/Allen-Cell-Explorer/Allen-Cell-Explorer_VERSION/archive/  

    cd /allen/aics/animated-cell/Allen-Cell-Explorer/Allen-Cell-Explorer_VERSION/archive
    for i in AICS*.txt; do tar -c -T "meta-${i}" -T "${i}" | gzip -1 > "${i%.txt}.tar.gz"; done
    ```

7. Get modeling team to update the bisque feats_out_small.csv feature analysis spreadsheet.  
convert feats_out_small.csv to json using https://www.csvjson.com/csv2json 
Change titles (first row) to be:
structureProteinName,Cell ID,Nuclear volume (fL),Cellular volume (fL),Nuclear surface area (&micro;m&sup2;),Cellular surface area (&micro;m&sup2;),Radial proximity (unitless),Apical proximity (unitless)
Save output as cell-feature-analysis.json in bisque bqcore/bq/core/public/js/AICS/cell-feature-analysis.json
Then run updateFeatures.py in this directory to add some fields.  Output will be in cell-feature-analysis2.json for checking before replacing. 
When satisfied, replace the file.

8. run generateCellLineDef.py and deposit the output file in the appropriate place (TBD)
 

