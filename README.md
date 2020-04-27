# cellbrowser tool

One line description

---

## Description

Main features in a brief descriptive text.

## Installation

Describe how to obtain the software and get it ready to run

## Documentation

If you have more extensive technical documentation (whether generated or not), ensure they are published to the following address:
For full package documentation please visit
[organization.github.io/projectname](https://organization.github.io/projectname/index.html).

## Quick Start

Workflow:

LET VERSION = 1.5.0

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
   `cd %prefs.out_dir%`
   `s3promotion upload --dest-bucket bisque.allencell.org --dest-prefix v%VERSION%/ %dataset_constants.THUMBNAILS_DIR%/`
   Upload of the OME-TIFF files is accomplished by handing the cellviewer-files.csv from step 3 to Gabe, currently. << fill in details here >>

5. At actual deploy time, update the files in https://github.com/AllenInstitute/cell-feature-data
   cell-feature-analysis.json
   cell-line-def.json

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the code.
