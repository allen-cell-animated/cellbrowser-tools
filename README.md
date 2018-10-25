Workflow:

LET VERSION = 1.3.0

1. On data handoff, modify a copy of prefs.json to have the proper parameters inside.  You can omit prefs.json and it will use the existing prefs.json file or modify in-place but then you have to remember if you changed anything.

2. Generate image data: 
On cluster, create a virtualenv and pip install aicsimageio and aicsimageprocessing and labkey and pandas into it.  
run:
    ```
    python createJobsFromCSV.py myprefs.json -c -n  
    ```

3. When cluster jobs are all done:
    ```
    python validateProcessedImages.py myprefs.json
    ```
*This validateProcessedImages step generates a few important output files: cell-feature-analysis.json, cellLineDef.json, and   This file must be handed off to Gabe for downloader use.*

3. Count files, count db entries, and do final qa. 

 

