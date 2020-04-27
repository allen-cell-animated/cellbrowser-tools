# cellbrowser-tools

Scripts for preparing datasets compatible with cell-feature-explorer

---

## Description

Compiles a table of FOV and segmented cell data from AICS databases.  
Copies images, prepares 3d viewer data, and produces a json database as input to cell-feature-explorer.
Results are all contained in one output directory ready to be uploaded for deployment.

## Installation

Clone the repo.  
`pip install -e .`

## Documentation

If you have more extensive technical documentation (whether generated or not), ensure they are published to the following address:
For full package documentation please visit
[allen-cell-animated.github.io/cellbrowser-tools](https://allen-cell-animated.github.io/cellbrowser-tools/index.html).

## Quick Start

Workflow:

1. Prepare environment.  
   create a virtualenv and pip install cellbrowser-tools
   or
   git clone cellbrowser-tools and pip install .

2. On data handoff, modify a copy of prefs.json to have the proper parameters inside. You can omit prefs.json and it will use the existing prefs.json file or modify in-place but then you have to remember if you changed anything.

3. Generate image data:
   run:

   ```
   python build_release.py myprefs.json
   ```

4. Upload data: This step is tied to the implementation of Cell-Feature-Explorer. The current strategy is to upload two large directories of files: the OME-TIFF files, and the postprocessed json and png files.
   To upload the json and png data:
   `set DATASET_VERSION=1.5.0`
   `cd %prefs.out_dir%`
   `s3promotion upload --dest-bucket bisque.allencell.org --dest-prefix v%DATASET_VERSION%/ %dataset_constants.THUMBNAILS_DIR%/`
   Upload of the OME-TIFF files is accomplished by handing the cellviewer-files.csv from step 3 to Gabe, currently. << fill in details here >>

5. At actual deploy time, update the files in https://github.com/AllenInstitute/cell-feature-data
   cell-feature-analysis.json
   cell-line-def.json

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the code.
