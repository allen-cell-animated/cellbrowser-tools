Workflow:

LET VERSION = 1.4.0

1. On data handoff, modify a copy of prefs.json to have the proper parameters inside.  You can omit prefs.json and it will use the existing prefs.json file or modify in-place but then you have to remember if you changed anything.

2. Generate image data: 
On cluster, create a virtualenv and pip install 
aicsimageio
aicsimageprocessing
featurehandoff==0.1.1
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

5. If no errors, put the results of steps in cell-feature-explorer/src/data directory.

6. Count files, do final qa. 

 

