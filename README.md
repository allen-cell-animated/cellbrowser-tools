Workflow:

LET VERSION = 1.4.0

1. On data handoff, modify a copy of prefs.json to have the proper parameters inside.  You can omit prefs.json and it will use the existing prefs.json file or modify in-place but then you have to remember if you changed anything.

2. Generate image data: 
    On slurm-master, create a virtualenv and pip install 
    aicsimageio
    aicsimageprocessing>=0.6.1
    git+https://github.com/AllenCellModeling/datasetdatabase.git
    featuredb>=0.3.0
    labkey
    lkaccess
    pandas
    jinja2

    You might have to modify fov_jov.j2 template to correct some hardcoded paths.  TODO FIXME move them into prefs.json.  
    run:
    ```
    python createJobsFromCSV.py myprefs.json -c -n  
    ```

3. When cluster jobs are all done:
    ```
    python validateProcessedImages.py myprefs.json
    ```
*This validateProcessedImages step generates a few important output files: cell-feature-analysis.json, errorFovs.txt, and cellviewer-files.csv.  These files are placed in prefs.out_status directory. The csv file must be handed off to Gabe for downloader use.*

4. Generate cell line defs file:
    ```
    python generateCellLineDef.py myprefs.json
    ```
*This generates cell-line-def.json in the prefs.out_status directory.*

5. If no errors, put the results of steps 3 and 4 in cell-feature-explorer/src/data directory.
These files should also be stored in /allen/aics/animated-cell/Allen-Cell-Explorer/Allen-Cell-Explorer_1.4.0/

6. Count files, do final qa. 

7. Upload data:  This step is tied to the implementation of Cell-Feature-Explorer.  The current strategy is to upload two large directories of files:  the OME-TIFF files, and the postprocessed json and png files.
To upload the json and png data:
    ```
    cd /allen/aics/animated-cell/Allen-Cell-Explorer/Allen-Cell-Explorer_1.4.0/
    s3promotion upload --dest-bucket bisque.allencell.org --dest-prefix v1.4.0/ Cell-Viewer_Thumbnails/
    ```
Upload of the OME-TIFF files is accomplished by handing the cellviewer-files.csv from step 3 to Gabe, currently. << fill in details here >>

8. At actual deploy time, update the files in https://github.com/AllenInstitute/cell-feature-data
cell-feature-analysis.json
cell-line-def.json


